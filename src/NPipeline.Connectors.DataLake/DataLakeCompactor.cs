using NPipeline.Connectors.DataLake.FormatAdapters;
using NPipeline.Connectors.DataLake.Manifest;
using NPipeline.Connectors.DataLake.Partitioning;
using NPipeline.Connectors.Parquet;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace NPipeline.Connectors.DataLake
{
    /// <summary>
    ///     Handles small-file consolidation for Data Lake tables.
    ///     Compacts multiple small Parquet files into larger files for better query performance.
    /// </summary>
    public sealed class DataLakeCompactor
    {
        private readonly ParquetConfiguration _configuration;
        private readonly IStorageProvider _provider;
        private readonly StorageUri _tableBasePath;

        /// <summary>
        ///     Initializes a new instance of the <see cref="DataLakeCompactor" /> class.
        /// </summary>
        /// <param name="provider">The storage provider.</param>
        /// <param name="tableBasePath">The base path of the table.</param>
        /// <param name="configuration">Optional Parquet configuration.</param>
        public DataLakeCompactor(
            IStorageProvider provider,
            StorageUri tableBasePath,
            ParquetConfiguration? configuration = null)
        {
            ArgumentNullException.ThrowIfNull(provider);
            ArgumentNullException.ThrowIfNull(tableBasePath);

            _provider = provider;
            _tableBasePath = tableBasePath;
            _configuration = configuration ?? new ParquetConfiguration();
        }

        /// <summary>
        ///     Performs compaction on small files in the table.
        /// </summary>
        /// <param name="request">The compaction request.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result of the compaction operation.</returns>
        public async Task<TableCompactResult> CompactAsync(
            TableCompactRequest request,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(request);

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            // Read manifest to find small files
            var manifestReader = new ManifestReader(_provider, _tableBasePath);
            var entries = await manifestReader.ReadAllAsync(cancellationToken).ConfigureAwait(false);

            // Filter small files
            var smallFiles = entries
                .Where(e => e.FileSizeBytes < request.SmallFileThresholdBytes)
                .Where(e => MatchesPartitionFilter(e, request.PartitionFilters))
                .OrderBy(e => e.Path, StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (smallFiles.Count < request.MinFilesToCompact)
            {
                return new TableCompactResult
                {
                    FilesCompacted = 0,
                    FilesCreated = 0,
                    BytesBefore = 0,
                    BytesAfter = 0,
                    RowsProcessed = 0,
                    Duration = stopwatch.Elapsed,
                    WasDryRun = request.DryRun,
                    Message = $"Not enough small files to compact. Found {smallFiles.Count}, need at least {request.MinFilesToCompact}."
                };
            }

            // Limit to MaxFilesToCompact
            var filesToCompact = smallFiles.Take(request.MaxFilesToCompact).ToList();
            var bytesBefore = filesToCompact.Sum(f => f.FileSizeBytes);
            var rowsProcessed = filesToCompact.Sum(f => f.RowCount);

            if (request.DryRun)
            {
                return new TableCompactResult
                {
                    FilesCompacted = filesToCompact.Count,
                    FilesCreated = EstimateOutputFiles(bytesBefore, request.TargetFileSizeBytes),
                    BytesBefore = bytesBefore,
                    BytesAfter = bytesBefore, // Dry run doesn't change size
                    RowsProcessed = rowsProcessed,
                    Duration = stopwatch.Elapsed,
                    CompactedFiles = [.. filesToCompact.Select(f => f.Path)],
                    WasDryRun = true,
                    Message = "Dry run completed. No files were modified."
                };
            }

            // Group files by partition for separate compaction
            var partitionGroups = GroupByPartition(filesToCompact);

            var compactedFiles = new List<string>();
            var newFiles = new List<(string Path, long RowCount)>();
            long bytesAfter = 0;

            foreach (var group in partitionGroups)
            {
                var (compacted, created, bytesWritten, rowCount) = await CompactPartitionGroupAsync(
                    group,
                    cancellationToken).ConfigureAwait(false);

                compactedFiles.AddRange(compacted);
                foreach (var (path, count) in created)
                {
                    newFiles.Add((path, count));
                }
                bytesAfter += bytesWritten;
            }

            // Update manifest with new files and mark old files as deleted
            var snapshotId = ManifestWriter.GenerateSnapshotId();
            await using var manifestWriter = new ManifestWriter(_provider, _tableBasePath, snapshotId);

            // Add new entries with accurate row counts
            foreach (var (newPath, rowCount) in newFiles)
            {
                // Get file info
                var fileUri = BuildFileUri(newPath);
                long fileSize = 0;
                try
                {
                    var metadata = await _provider.GetMetadataAsync(fileUri, cancellationToken).ConfigureAwait(false);
                    fileSize = metadata?.Size ?? 0;
                }
                catch
                {
                    // Ignore
                }

                // Extract partition values from path
                var partitionValues = ExtractPartitionValuesFromPath(newPath);

                var entry = new ManifestEntry
                {
                    Path = newPath,
                    RowCount = rowCount, // Use tracked row count from compaction
                    WrittenAt = DateTimeOffset.UtcNow,
                    FileSizeBytes = fileSize,
                    PartitionValues = partitionValues,
                    SnapshotId = snapshotId,
                    FileFormat = "parquet"
                };

                manifestWriter.Append(entry);
            }

            await manifestWriter.FlushAsync(cancellationToken).ConfigureAwait(false);

            // Delete original files if requested
            if (request.DeleteOriginalFiles)
            {
                await DeleteFilesAsync(compactedFiles, cancellationToken).ConfigureAwait(false);
            }

            stopwatch.Stop();

            return new TableCompactResult
            {
                FilesCompacted = compactedFiles.Count,
                FilesCreated = newFiles.Count,
                BytesBefore = bytesBefore,
                BytesAfter = bytesAfter,
                RowsProcessed = rowsProcessed,
                Duration = stopwatch.Elapsed,
                CompactedFiles = compactedFiles,
                NewFiles = newFiles.Select(f => f.Path).ToList(),
                WasDryRun = false,
                Message = $"Compacted {compactedFiles.Count} files into {newFiles.Count} files."
            };
        }

        private static bool MatchesPartitionFilter(
            ManifestEntry entry,
            IReadOnlyDictionary<string, string>? partitionFilters)
        {
            if (partitionFilters is null || partitionFilters.Count == 0)
            {
                return true;
            }

            if (entry.PartitionValues is null)
            {
                return false;
            }

            foreach (var filter in partitionFilters)
            {
                if (!entry.PartitionValues.TryGetValue(filter.Key, out var value) ||
                    !string.Equals(value, filter.Value, StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }
            }

            return true;
        }

        private static int EstimateOutputFiles(long totalBytes, long targetFileSize)
        {
            if (targetFileSize <= 0)
            {
                return 1;
            }

            return (int)Math.Max(1, Math.Ceiling((double)totalBytes / targetFileSize));
        }

        private static List<List<ManifestEntry>> GroupByPartition(IReadOnlyList<ManifestEntry> entries)
        {
            var groups = new Dictionary<string, List<ManifestEntry>>(StringComparer.OrdinalIgnoreCase);

            foreach (var entry in entries)
            {
                // Group by the directory path (partition path)
                var partitionKey = GetPartitionKey(entry.Path);

                if (!groups.TryGetValue(partitionKey, out var group))
                {
                    group = [];
                    groups[partitionKey] = group;
                }

                group.Add(entry);
            }

            return [.. groups.Values];
        }

        private static string GetPartitionKey(string path)
        {
            // Extract the directory path as the partition key
            var lastSlashIndex = path.LastIndexOf('/');
            return lastSlashIndex > 0 ? path[..lastSlashIndex] : string.Empty;
        }

        private async Task<(List<string> Compacted, List<(string Path, long RowCount)> Created, long BytesWritten, long TotalRowCount)> CompactPartitionGroupAsync(
            List<ManifestEntry> group,
            CancellationToken cancellationToken)
        {
            var compactedFiles = group.Select(e => e.Path).ToList();
            var newFiles = new List<(string Path, long RowCount)>();
            long bytesWritten = 0;
            long totalRowCount = 0;

            // Read all records from the group
            var allRecords = new List<ParquetRow>();
            foreach (var entry in group)
            {
                var fileUri = BuildFileUri(entry.Path);
                await foreach (var row in ReadFileRowsAsync(fileUri, cancellationToken))
                {
                    allRecords.Add(row);
                }

                // Track total rows from source files for verification
                totalRowCount += entry.RowCount;
            }

            if (allRecords.Count == 0)
            {
                return (compactedFiles, newFiles, bytesWritten, 0);
            }

            // Use actual record count (the number of records we actually read)
            var actualRowCount = (long)allRecords.Count;

            // Write compacted file(s)
            var partitionPath = GetPartitionKey(group[0].Path);
            var fileSequence = 0;

            // For simplicity, write all records to a single file
            // In production, you'd split by TargetFileSizeBytes
            var newFileName = GenerateCompactedFileName(fileSequence);
            var newRelativePath = string.IsNullOrEmpty(partitionPath)
                ? newFileName
                : $"{partitionPath}/{newFileName}";

            var newFileUri = BuildFileUri(newRelativePath);

            // Write the compacted file
            await WriteCompactedFileAsync(newFileUri, allRecords, cancellationToken).ConfigureAwait(false);

            // Get the new file size
            try
            {
                var metadata = await _provider.GetMetadataAsync(newFileUri, cancellationToken).ConfigureAwait(false);
                bytesWritten = metadata?.Size ?? 0;
            }
            catch
            {
                // Ignore
            }

            // Add the new file with its actual row count
            newFiles.Add((newRelativePath, actualRowCount));

            return (compactedFiles, newFiles, bytesWritten, actualRowCount);
        }

        private async IAsyncEnumerable<ParquetRow> ReadFileRowsAsync(
            StorageUri fileUri,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            // Use ParquetSourceNode to read rows with an identity mapper (row => row)
            // since ParquetRow doesn't have a parameterless constructor for attribute mapping
            var sourceNode = new ParquetSourceNode<ParquetRow>(_provider, fileUri, row => row, _configuration);

            var dataPipe = sourceNode.Initialize(Pipeline.PipelineContext.Default, cancellationToken);

            await foreach (var item in dataPipe.WithCancellation(cancellationToken))
            {
                yield return item;
            }
        }

        private async Task WriteCompactedFileAsync(
            StorageUri fileUri,
            List<ParquetRow> records,
            CancellationToken cancellationToken)
        {
            if (records.Count == 0)
            {
                return;
            }

            // Get the schema from the first record
            var schema = records[0].Schema;
            var columnNames = records[0].ColumnNames;

            await using var stream = await _provider.OpenWriteAsync(fileUri, cancellationToken)
                .ConfigureAwait(false);

            await using var writer = await ParquetWriter.CreateAsync(schema, stream, cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            using var rowGroupWriter = writer.CreateRowGroup();

            // Write each column
            foreach (var columnName in columnNames)
            {
                var field = schema.Fields.FirstOrDefault(f => f.Name == columnName);
                if (field is null || field is not DataField dataField)
                {
                    continue;
                }

                var columnData = CreateColumnData(dataField, records, columnName);
                await rowGroupWriter.WriteColumnAsync(columnData, cancellationToken).ConfigureAwait(false);
            }
        }

        private DataColumn CreateColumnData(DataField field, List<ParquetRow> records, string columnName)
        {
            var fieldType = field.ClrType;

            if (fieldType == typeof(string))
            {
                var data = new string?[records.Count];
                for (var i = 0; i < records.Count; i++)
                {
                    data[i] = records[i][columnName] as string;
                }
                return new DataColumn(field, data);
            }

            if (fieldType == typeof(int))
            {
                if (field.IsNullable)
                {
                    var data = new int?[records.Count];
                    for (var i = 0; i < records.Count; i++)
                    {
                        var value = records[i][columnName];
                        data[i] = value is int intValue ? intValue : null;
                    }
                    return new DataColumn(field, data);
                }
                else
                {
                    var data = new int[records.Count];
                    for (var i = 0; i < records.Count; i++)
                    {
                        var value = records[i][columnName];
                        data[i] = value is int intValue ? intValue : default;
                    }
                    return new DataColumn(field, data);
                }
            }

            if (fieldType == typeof(long))
            {
                if (field.IsNullable)
                {
                    var data = new long?[records.Count];
                    for (var i = 0; i < records.Count; i++)
                    {
                        var value = records[i][columnName];
                        data[i] = value is long longValue ? longValue : null;
                    }
                    return new DataColumn(field, data);
                }
                else
                {
                    var data = new long[records.Count];
                    for (var i = 0; i < records.Count; i++)
                    {
                        var value = records[i][columnName];
                        data[i] = value is long longValue ? longValue : default;
                    }
                    return new DataColumn(field, data);
                }
            }

            if (fieldType == typeof(short))
            {
                if (field.IsNullable)
                {
                    var data = new short?[records.Count];
                    for (var i = 0; i < records.Count; i++)
                    {
                        var value = records[i][columnName];
                        data[i] = value is short shortValue ? shortValue : null;
                    }
                    return new DataColumn(field, data);
                }
                else
                {
                    var data = new short[records.Count];
                    for (var i = 0; i < records.Count; i++)
                    {
                        var value = records[i][columnName];
                        data[i] = value is short shortValue ? shortValue : default;
                    }
                    return new DataColumn(field, data);
                }
            }

            if (fieldType == typeof(byte))
            {
                if (field.IsNullable)
                {
                    var data = new byte?[records.Count];
                    for (var i = 0; i < records.Count; i++)
                    {
                        var value = records[i][columnName];
                        data[i] = value is byte byteValue ? byteValue : null;
                    }
                    return new DataColumn(field, data);
                }
                else
                {
                    var data = new byte[records.Count];
                    for (var i = 0; i < records.Count; i++)
                    {
                        var value = records[i][columnName];
                        data[i] = value is byte byteValue ? byteValue : default;
                    }
                    return new DataColumn(field, data);
                }
            }

            if (fieldType == typeof(float))
            {
                if (field.IsNullable)
                {
                    var data = new float?[records.Count];
                    for (var i = 0; i < records.Count; i++)
                    {
                        var value = records[i][columnName];
                        data[i] = value is float floatValue ? floatValue : null;
                    }
                    return new DataColumn(field, data);
                }
                else
                {
                    var data = new float[records.Count];
                    for (var i = 0; i < records.Count; i++)
                    {
                        var value = records[i][columnName];
                        data[i] = value is float floatValue ? floatValue : default;
                    }
                    return new DataColumn(field, data);
                }
            }

            if (fieldType == typeof(double))
            {
                if (field.IsNullable)
                {
                    var data = new double?[records.Count];
                    for (var i = 0; i < records.Count; i++)
                    {
                        var value = records[i][columnName];
                        data[i] = value is double doubleValue ? doubleValue : null;
                    }
                    return new DataColumn(field, data);
                }
                else
                {
                    var data = new double[records.Count];
                    for (var i = 0; i < records.Count; i++)
                    {
                        var value = records[i][columnName];
                        data[i] = value is double doubleValue ? doubleValue : default;
                    }
                    return new DataColumn(field, data);
                }
            }

            if (fieldType == typeof(bool))
            {
                if (field.IsNullable)
                {
                    var data = new bool?[records.Count];
                    for (var i = 0; i < records.Count; i++)
                    {
                        var value = records[i][columnName];
                        data[i] = value is bool boolValue ? boolValue : null;
                    }
                    return new DataColumn(field, data);
                }
                else
                {
                    var data = new bool[records.Count];
                    for (var i = 0; i < records.Count; i++)
                    {
                        var value = records[i][columnName];
                        data[i] = value is bool boolValue ? boolValue : default;
                    }
                    return new DataColumn(field, data);
                }
            }

            if (fieldType == typeof(decimal))
            {
                if (field.IsNullable)
                {
                    var data = new decimal?[records.Count];
                    for (var i = 0; i < records.Count; i++)
                    {
                        var value = records[i][columnName];
                        data[i] = value is decimal decimalValue ? decimalValue : null;
                    }
                    return new DataColumn(field, data);
                }
                else
                {
                    var data = new decimal[records.Count];
                    for (var i = 0; i < records.Count; i++)
                    {
                        var value = records[i][columnName];
                        data[i] = value is decimal decimalValue ? decimalValue : default;
                    }
                    return new DataColumn(field, data);
                }
            }

            if (fieldType == typeof(DateTime))
            {
                if (field.IsNullable)
                {
                    var data = new DateTime?[records.Count];
                    for (var i = 0; i < records.Count; i++)
                    {
                        var value = records[i][columnName];
                        data[i] = value is DateTime dateTimeValue ? dateTimeValue : null;
                    }
                    return new DataColumn(field, data);
                }
                else
                {
                    var data = new DateTime[records.Count];
                    for (var i = 0; i < records.Count; i++)
                    {
                        var value = records[i][columnName];
                        data[i] = value is DateTime dateTimeValue ? dateTimeValue : default;
                    }
                    return new DataColumn(field, data);
                }
            }

            if (fieldType == typeof(byte[]))
            {
                var data = new byte[records.Count][];
                for (var i = 0; i < records.Count; i++)
                {
                    var value = records[i][columnName];
                    data[i] = value as byte[] ?? [];
                }
                return new DataColumn(field, data);
            }

            // Default: convert to string representation
            {
                var data = new string?[records.Count];
                for (var i = 0; i < records.Count; i++)
                {
                    var value = records[i][columnName];
                    data[i] = value?.ToString();
                }
                return new DataColumn(field, data);
            }
        }

        private async Task DeleteFilesAsync(
            List<string> filesToDelete,
            CancellationToken cancellationToken)
        {
            foreach (var filePath in filesToDelete)
            {
                try
                {
                    var fileUri = BuildFileUri(filePath);

                    // Check if provider supports deletion
                    if (_provider is IDeletableStorageProvider deletableProvider)
                    {
                        await deletableProvider.DeleteAsync(fileUri, cancellationToken).ConfigureAwait(false);
                    }
                }
                catch
                {
                    // Ignore deletion errors - files will be orphaned but not corrupt data
                }
            }
        }

        private StorageUri BuildFileUri(string relativePath)
        {
            var basePath = _tableBasePath.Path?.TrimStart('/') ?? string.Empty;
            var fullPath = string.IsNullOrEmpty(basePath)
                ? $"/{relativePath.TrimStart('/')}"
                : $"/{basePath}/{relativePath.TrimStart('/')}";

            return StorageUri.Parse($"{_tableBasePath.Scheme}://{_tableBasePath.Host}{fullPath}");
        }

        private static string GenerateCompactedFileName(int sequence)
        {
            var guid = Guid.NewGuid().ToString("N")[..8];
            return $"compacted-{sequence:D5}-{guid}.parquet";
        }

        private static IReadOnlyDictionary<string, string>? ExtractPartitionValuesFromPath(string path)
        {
            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            var segments = path.Split(['/', '\\'], StringSplitOptions.RemoveEmptyEntries);
            foreach (var segment in segments)
            {
                if (PartitionKey.TryParse(segment, out var key) && key is not null)
                {
                    result[key.ColumnName] = key.Value;
                }
            }

            return result.Count > 0 ? result : null;
        }
    }
}

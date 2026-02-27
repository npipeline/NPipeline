using NPipeline.Connectors.DataLake.Manifest;
using NPipeline.Connectors.DataLake.Partitioning;
using NPipeline.Connectors.DataLake.Snapshot;
using NPipeline.Connectors.Parquet;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.DataLake
{
    /// <summary>
    ///     High-level API for writing data to a Data Lake table.
    ///     Handles partitioned writes, manifest tracking, and snapshot management.
    /// </summary>
    /// <typeparam name="T">The record type being written.</typeparam>
    public sealed class DataLakeTableWriter<T> : IAsyncDisposable
    {
        private readonly ParquetConfiguration _configuration;
        private readonly PartitionSpec<T>? _partitionSpec;
        private readonly IStorageProvider _provider;
        private readonly StorageUri _tableBasePath;
        private readonly string _snapshotId;
        private readonly FileSequenceContext _sequenceContext = new();
        private ManifestWriter? _manifestWriter;
        private bool _disposed;

        /// <summary>
        ///     Initializes a new instance of the <see cref="DataLakeTableWriter{T}" /> class.
        /// </summary>
        /// <param name="provider">The storage provider to use for writing.</param>
        /// <param name="tableBasePath">The base path of the table.</param>
        /// <param name="partitionSpec">Optional partition specification.</param>
        /// <param name="configuration">Optional Parquet configuration.</param>
        public DataLakeTableWriter(
            IStorageProvider provider,
            StorageUri tableBasePath,
            PartitionSpec<T>? partitionSpec = null,
            ParquetConfiguration? configuration = null)
        {
            ArgumentNullException.ThrowIfNull(provider);
            ArgumentNullException.ThrowIfNull(tableBasePath);

            _provider = provider;
            _tableBasePath = tableBasePath;
            _partitionSpec = partitionSpec;
            _configuration = configuration ?? new ParquetConfiguration();
            _configuration.Validate();
            _snapshotId = ManifestWriter.GenerateSnapshotId();
        }

        /// <summary>
        ///     Gets the snapshot ID for this writer session.
        /// </summary>
        public string SnapshotId => _snapshotId;

        /// <summary>
        ///     Gets the table base path.
        /// </summary>
        public StorageUri TableBasePath => _tableBasePath;

        /// <summary>
        ///     Appends data from a data pipe to the table.
        /// </summary>
        /// <param name="data">The data to append.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public async Task AppendAsync(IDataPipe<T> data, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(data);
            ObjectDisposedException.ThrowIf(_disposed, this);

            _manifestWriter ??= new ManifestWriter(_provider, _tableBasePath, _snapshotId);

            if (_partitionSpec is not null && _partitionSpec.HasPartitions)
            {
                await AppendPartitionedAsync(data, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await AppendUnpartitionedAsync(data, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        ///     Gets the current snapshot of the table.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The current table snapshot.</returns>
        public async Task<TableSnapshot> GetSnapshotAsync(CancellationToken cancellationToken = default)
        {
            var reader = new ManifestReader(_provider, _tableBasePath);
            var entries = await reader.ReadAllAsync(cancellationToken).ConfigureAwait(false);

            return new TableSnapshot(
                _snapshotId,
                entries,
                _tableBasePath);
        }

        /// <summary>
        ///     Gets a snapshot for a specific snapshot ID.
        /// </summary>
        /// <param name="snapshotId">The snapshot ID to retrieve.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The table snapshot for the specified ID.</returns>
        public async Task<TableSnapshot> GetSnapshotAsync(
            string snapshotId,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(snapshotId);

            var reader = new ManifestReader(_provider, _tableBasePath);
            var entries = await reader.ReadBySnapshotAsync(snapshotId, cancellationToken).ConfigureAwait(false);

            return new TableSnapshot(
                snapshotId,
                entries,
                _tableBasePath);
        }

        /// <summary>
        ///     Gets a snapshot as of a specific point in time (time travel).
        /// </summary>
        /// <param name="asOf">The timestamp for time travel.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The table snapshot as of the specified time.</returns>
        public async Task<TableSnapshot> GetSnapshotAsync(
            DateTimeOffset asOf,
            CancellationToken cancellationToken = default)
        {
            var reader = new ManifestReader(_provider, _tableBasePath);
            var entries = await reader.ReadAsOfAsync(asOf, cancellationToken).ConfigureAwait(false);

            // Generate a synthetic snapshot ID for the time-travel query
            var snapshotId = $"timetravel-{asOf:yyyyMMddHHmmss}";

            return new TableSnapshot(
                snapshotId,
                entries,
                _tableBasePath);
        }

        /// <summary>
        ///     Gets the list of available snapshot IDs.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A list of snapshot IDs.</returns>
        public async Task<IReadOnlyList<string>> GetSnapshotIdsAsync(CancellationToken cancellationToken = default)
        {
            var reader = new ManifestReader(_provider, _tableBasePath);
            return await reader.GetSnapshotIdsAsync(cancellationToken).ConfigureAwait(false);
        }

        private async Task AppendPartitionedAsync(IDataPipe<T> data, CancellationToken cancellationToken)
        {
            var partitionBuffers = new Dictionary<string, List<T>>(StringComparer.OrdinalIgnoreCase);
            long totalBufferedRows = 0;

            await foreach (var record in data.WithCancellation(cancellationToken))
            {
                if (record is null)
                {
                    continue;
                }

                // Determine partition path
                var partitionPath = PartitionPathBuilder.BuildPath(record, _partitionSpec!);

                // Add to appropriate buffer
                if (!partitionBuffers.TryGetValue(partitionPath, out var buffer))
                {
                    buffer = [];
                    partitionBuffers[partitionPath] = buffer;
                }

                buffer.Add(record);
                totalBufferedRows++;

                // Check if we need to flush any partition buffers
                if (buffer.Count >= _configuration.RowGroupSize)
                {
                    await FlushPartitionBufferAsync(partitionPath, buffer, cancellationToken)
                        .ConfigureAwait(false);
                    totalBufferedRows -= buffer.Count;
                    buffer.Clear();
                }

                // Backpressure guard: flush largest buffers if we exceed MaxBufferedRows
                if (totalBufferedRows > _configuration.MaxBufferedRows)
                {
                    await FlushLargestBuffersAsync(partitionBuffers, cancellationToken)
                        .ConfigureAwait(false);
                    totalBufferedRows = partitionBuffers.Values.Sum(b => b.Count);
                }
            }

            // Flush remaining buffers
            foreach (var kvp in partitionBuffers)
            {
                if (kvp.Value.Count > 0)
                {
                    await FlushPartitionBufferAsync(kvp.Key, kvp.Value, cancellationToken)
                        .ConfigureAwait(false);
                }
            }
        }

        private async Task FlushPartitionBufferAsync(
            string partitionPath,
            List<T> buffer,
            CancellationToken cancellationToken)
        {
            var fileName = GenerateFileName(_sequenceContext.GetNext());
            var relativePath = string.IsNullOrEmpty(partitionPath)
                ? fileName
                : $"{partitionPath}{fileName}";

            var fullPath = BuildFullPath(relativePath);
            var fileUri = StorageUri.Parse($"{_tableBasePath.Scheme}://{_tableBasePath.Host}{fullPath}");

            // Write Parquet file
            var sinkNode = new ParquetSinkNode<T>(_provider, fileUri, _configuration);

            // Create a data pipe from the buffer
            var dataPipe = new InMemoryDataPipe<T>(buffer);

            await sinkNode.ExecuteAsync(dataPipe, PipelineContext.Default, cancellationToken)
                .ConfigureAwait(false);

            // Get file size (best effort)
            long fileSizeBytes = 0;
            try
            {
                var metadata = await _provider.GetMetadataAsync(fileUri, cancellationToken).ConfigureAwait(false);
                fileSizeBytes = metadata?.Size ?? 0;
            }
            catch
            {
                // Ignore errors getting file size
            }

            // Extract partition values from the first record
            IReadOnlyDictionary<string, string>? partitionValues = null;
            if (_partitionSpec?.HasPartitions == true && buffer.Count > 0)
            {
                partitionValues = PartitionPathBuilder.ExtractPartitionKeys(buffer[0], _partitionSpec)
                    .ToDictionary(k => k.ColumnName, k => k.Value);
            }

            // Add manifest entry
            var entry = new ManifestEntry
            {
                Path = relativePath,
                RowCount = buffer.Count,
                WrittenAt = DateTimeOffset.UtcNow,
                FileSizeBytes = fileSizeBytes,
                PartitionValues = partitionValues,
                SnapshotId = _snapshotId,
                FileFormat = "parquet"
            };

            _manifestWriter?.Append(entry);
        }

        private async Task FlushLargestBuffersAsync(
            Dictionary<string, List<T>> partitionBuffers,
            CancellationToken cancellationToken)
        {
            // Find buffers that are at least half full
            var buffersToFlush = partitionBuffers
                .Where(kvp => kvp.Value.Count >= _configuration.RowGroupSize / 2)
                .OrderByDescending(kvp => kvp.Value.Count)
                .Take(3) // Flush up to 3 largest buffers
                .ToList();

            foreach (var kvp in buffersToFlush)
            {
                await FlushPartitionBufferAsync(kvp.Key, kvp.Value, cancellationToken)
                    .ConfigureAwait(false);
                kvp.Value.Clear();
            }
        }

        private async Task AppendUnpartitionedAsync(IDataPipe<T> data, CancellationToken cancellationToken)
        {
            var buffer = new List<T>(_configuration.RowGroupSize);

            await foreach (var record in data.WithCancellation(cancellationToken))
            {
                if (record is null)
                {
                    continue;
                }

                buffer.Add(record);

                if (buffer.Count >= _configuration.RowGroupSize)
                {
                    await FlushUnpartitionedBufferAsync(buffer, cancellationToken)
                        .ConfigureAwait(false);
                    buffer.Clear();
                }
            }

            // Flush remaining records
            if (buffer.Count > 0)
            {
                await FlushUnpartitionedBufferAsync(buffer, cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        private Task FlushUnpartitionedBufferAsync(
            List<T> buffer,
            CancellationToken cancellationToken)
        {
            return FlushPartitionBufferAsync(string.Empty, buffer, cancellationToken);
        }

        private string BuildFullPath(string relativePath)
        {
            var basePath = _tableBasePath.Path?.TrimStart('/') ?? string.Empty;
            return string.IsNullOrEmpty(basePath)
                ? $"/{relativePath.TrimStart('/')}"
                : $"/{basePath}/{relativePath.TrimStart('/')}";
        }

        private static string GenerateFileName(int sequence)
        {
            var guid = Guid.NewGuid().ToString("N")[..8];
            return $"part-{sequence:D5}-{guid}.parquet";
        }

        /// <inheritdoc />
        public async ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                return;
            }

            try
            {
                if (_manifestWriter is not null)
                {
                    await _manifestWriter.FlushAsync().ConfigureAwait(false);
                    await _manifestWriter.DisposeAsync().ConfigureAwait(false);
                }
            }
            catch
            {
                // Swallow exceptions during disposal
            }

            _disposed = true;
        }

        /// <summary>
        ///     Thread-safe context for tracking file sequence numbers.
        /// </summary>
        private sealed class FileSequenceContext
        {
            private int _current;

            public int GetNext()
            {
                return Interlocked.Increment(ref _current) - 1;
            }
        }
    }
}

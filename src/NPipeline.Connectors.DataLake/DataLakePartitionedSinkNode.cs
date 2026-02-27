using NPipeline.Connectors.DataLake.Manifest;
using NPipeline.Connectors.DataLake.Partitioning;
using NPipeline.Connectors.Parquet;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.DataLake
{
    /// <summary>
    ///     Sink node that writes partitioned data to a Data Lake table.
    ///     Routes records to partition buffers and flushes at RowGroupSize.
    /// </summary>
    /// <typeparam name="T">The record type being written.</typeparam>
    public sealed class DataLakePartitionedSinkNode<T> : SinkNode<T>
    {
        private static readonly Lazy<IStorageResolver> DefaultResolver =
            new(() => StorageProviderFactory.CreateResolver());

        private readonly ParquetConfiguration _configuration;
        private readonly PartitionSpec<T>? _partitionSpec;
        private readonly IStorageProvider? _provider;
        private readonly IStorageResolver? _resolver;
        private readonly StorageUri _tableBasePath;

        /// <summary>
        ///     Initializes a new instance of the <see cref="DataLakePartitionedSinkNode{T}" /> class with a resolver.
        /// </summary>
        /// <param name="tableBasePath">The base path of the table.</param>
        /// <param name="partitionSpec">Optional partition specification.</param>
        /// <param name="resolver">The storage resolver.</param>
        /// <param name="configuration">Optional Parquet configuration.</param>
        public DataLakePartitionedSinkNode(
            StorageUri tableBasePath,
            PartitionSpec<T>? partitionSpec = null,
            IStorageResolver? resolver = null,
            ParquetConfiguration? configuration = null)
        {
            ArgumentNullException.ThrowIfNull(tableBasePath);

            _tableBasePath = tableBasePath;
            _partitionSpec = partitionSpec;
            _resolver = resolver ?? DefaultResolver.Value;
            _configuration = configuration ?? new ParquetConfiguration();
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="DataLakePartitionedSinkNode{T}" /> class with a provider.
        /// </summary>
        /// <param name="provider">The storage provider.</param>
        /// <param name="tableBasePath">The base path of the table.</param>
        /// <param name="partitionSpec">Optional partition specification.</param>
        /// <param name="configuration">Optional Parquet configuration.</param>
        public DataLakePartitionedSinkNode(
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
        }

        /// <inheritdoc />
        public override async Task ExecuteAsync(
            IDataPipe<T> input,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            var provider = _provider ?? StorageProviderFactory.GetProviderOrThrow(
                _resolver ?? throw new InvalidOperationException("No storage resolver configured."),
                _tableBasePath);

            var snapshotId = ManifestWriter.GenerateSnapshotId();

            await using var manifestWriter = new ManifestWriter(provider, _tableBasePath, snapshotId);

            if (_partitionSpec is not null && _partitionSpec.HasPartitions)
            {
                await ExecutePartitionedAsync(provider, input, manifestWriter, snapshotId, cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                await ExecuteUnpartitionedAsync(provider, input, manifestWriter, snapshotId, cancellationToken)
                    .ConfigureAwait(false);
            }

            await manifestWriter.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        private async Task ExecutePartitionedAsync(
            IStorageProvider provider,
            IDataPipe<T> input,
            ManifestWriter manifestWriter,
            string snapshotId,
            CancellationToken cancellationToken)
        {
            var partitionBuffers = new Dictionary<string, List<T>>(StringComparer.OrdinalIgnoreCase);
            var sequenceContext = new FileSequenceContext();
            long totalBufferedRows = 0;

            await foreach (var record in input.WithCancellation(cancellationToken))
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

                // Check if we need to flush this partition buffer
                if (buffer.Count >= _configuration.RowGroupSize)
                {
                    await FlushBufferAsync(
                        provider,
                        partitionPath,
                        buffer,
                        manifestWriter,
                        snapshotId,
                        sequenceContext,
                        cancellationToken).ConfigureAwait(false);

                    totalBufferedRows -= buffer.Count;
                    buffer.Clear();
                }

                // Backpressure guard
                if (totalBufferedRows > _configuration.MaxBufferedRows)
                {
                    await FlushLargestBuffersAsync(
                        provider,
                        partitionBuffers,
                        manifestWriter,
                        snapshotId,
                        sequenceContext,
                        cancellationToken).ConfigureAwait(false);

                    totalBufferedRows = partitionBuffers.Values.Sum(b => b.Count);
                }
            }

            // Flush remaining buffers
            foreach (var kvp in partitionBuffers)
            {
                if (kvp.Value.Count > 0)
                {
                    await FlushBufferAsync(
                        provider,
                        kvp.Key,
                        kvp.Value,
                        manifestWriter,
                        snapshotId,
                        sequenceContext,
                        cancellationToken).ConfigureAwait(false);
                }
            }
        }

        private async Task ExecuteUnpartitionedAsync(
            IStorageProvider provider,
            IDataPipe<T> input,
            ManifestWriter manifestWriter,
            string snapshotId,
            CancellationToken cancellationToken)
        {
            var buffer = new List<T>(_configuration.RowGroupSize);
            var sequenceContext = new FileSequenceContext();

            await foreach (var record in input.WithCancellation(cancellationToken))
            {
                if (record is null)
                {
                    continue;
                }

                buffer.Add(record);

                if (buffer.Count >= _configuration.RowGroupSize)
                {
                    await FlushBufferAsync(
                        provider,
                        string.Empty,
                        buffer,
                        manifestWriter,
                        snapshotId,
                        sequenceContext,
                        cancellationToken).ConfigureAwait(false);

                    buffer.Clear();
                }
            }

            // Flush remaining records
            if (buffer.Count > 0)
            {
                await FlushBufferAsync(
                    provider,
                    string.Empty,
                    buffer,
                    manifestWriter,
                    snapshotId,
                    sequenceContext,
                    cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task FlushBufferAsync(
            IStorageProvider provider,
            string partitionPath,
            List<T> buffer,
            ManifestWriter manifestWriter,
            string snapshotId,
            FileSequenceContext sequenceContext,
            CancellationToken cancellationToken)
        {
            var fileName = GenerateFileName(sequenceContext.GetNext());
            var relativePath = string.IsNullOrEmpty(partitionPath)
                ? fileName
                : $"{partitionPath}{fileName}";

            var fullPath = BuildFullPath(relativePath);
            var fileUri = StorageUri.Parse($"{_tableBasePath.Scheme}://{_tableBasePath.Host}{fullPath}");

            // Write Parquet file
            var sinkNode = new ParquetSinkNode<T>(provider, fileUri, _configuration);

            // Create a data pipe from the buffer
            var dataPipe = new InMemoryDataPipe<T>(buffer);

            await sinkNode.ExecuteAsync(dataPipe, PipelineContext.Default, cancellationToken)
                .ConfigureAwait(false);

            // Get file size
            long fileSizeBytes = 0;
            try
            {
                var metadata = await provider.GetMetadataAsync(fileUri, cancellationToken).ConfigureAwait(false);
                fileSizeBytes = metadata?.Size ?? 0;
            }
            catch
            {
                // Ignore errors getting file size
            }

            // Extract partition values
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
                SnapshotId = snapshotId,
                FileFormat = "parquet"
            };

            manifestWriter.Append(entry);
        }

        private async Task FlushLargestBuffersAsync(
            IStorageProvider provider,
            Dictionary<string, List<T>> partitionBuffers,
            ManifestWriter manifestWriter,
            string snapshotId,
            FileSequenceContext sequenceContext,
            CancellationToken cancellationToken)
        {
            var buffersToFlush = partitionBuffers
                .Where(kvp => kvp.Value.Count >= _configuration.RowGroupSize / 2)
                .OrderByDescending(kvp => kvp.Value.Count)
                .Take(3)
                .ToList();

            foreach (var kvp in buffersToFlush)
            {
                await FlushBufferAsync(
                    provider,
                    kvp.Key,
                    kvp.Value,
                    manifestWriter,
                    snapshotId,
                    sequenceContext,
                    cancellationToken).ConfigureAwait(false);

                kvp.Value.Clear();
            }
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

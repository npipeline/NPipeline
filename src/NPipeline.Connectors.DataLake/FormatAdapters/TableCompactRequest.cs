using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.DataLake.FormatAdapters
{
    /// <summary>
    ///     Request model for table compaction operations.
    /// </summary>
    public sealed class TableCompactRequest
    {
        /// <summary>
        ///     Gets or sets the base path of the table.
        /// </summary>
        public required StorageUri TableBasePath { get; init; }

        /// <summary>
        ///     Gets or sets the storage provider to use.
        /// </summary>
        public required IStorageProvider Provider { get; init; }

        /// <summary>
        ///     Gets or sets the target file size for compacted files.
        ///     Files smaller than this will be candidates for compaction.
        ///     Default is 256 MB.
        /// </summary>
        public long TargetFileSizeBytes { get; init; } = 256L * 1024 * 1024;

        /// <summary>
        ///     Gets or sets the minimum number of small files required to trigger compaction.
        ///     Default is 5 files.
        /// </summary>
        public int MinFilesToCompact { get; init; } = 5;

        /// <summary>
        ///     Gets or sets the maximum number of files to compact in a single operation.
        ///     Default is 100 files.
        /// </summary>
        public int MaxFilesToCompact { get; init; } = 100;

        /// <summary>
        ///     Gets or sets the size threshold below which files are considered "small".
        ///     Files smaller than this are candidates for compaction.
        ///     Default is 32 MB.
        /// </summary>
        public long SmallFileThresholdBytes { get; init; } = 32L * 1024 * 1024;

        /// <summary>
        ///     Gets or sets optional partition filters to limit compaction scope.
        ///     If specified, only files in matching partitions will be compacted.
        /// </summary>
        public IReadOnlyDictionary<string, string>? PartitionFilters { get; init; }

        /// <summary>
        ///     Gets or sets a value indicating whether to perform a dry run without making changes.
        ///     Default is <c>false</c>.
        /// </summary>
        public bool DryRun { get; init; }

        /// <summary>
        ///     Gets or sets a value indicating whether to delete original files after compaction.
        ///     Default is <c>true</c>.
        /// </summary>
        public bool DeleteOriginalFiles { get; init; } = true;
    }

    /// <summary>
    ///     Result of a table compaction operation.
    /// </summary>
    public sealed class TableCompactResult
    {
        /// <summary>
        ///     Gets or sets the number of files that were compacted.
        /// </summary>
        public required int FilesCompacted { get; init; }

        /// <summary>
        ///     Gets or sets the number of new files created.
        /// </summary>
        public required int FilesCreated { get; init; }

        /// <summary>
        ///     Gets or sets the total bytes before compaction.
        /// </summary>
        public required long BytesBefore { get; init; }

        /// <summary>
        ///     Gets or sets the total bytes after compaction.
        /// </summary>
        public required long BytesAfter { get; init; }

        /// <summary>
        ///     Gets or sets the number of rows processed.
        /// </summary>
        public required long RowsProcessed { get; init; }

        /// <summary>
        ///     Gets or sets the duration of the compaction operation.
        /// </summary>
        public required TimeSpan Duration { get; init; }

        /// <summary>
        ///     Gets or sets the list of files that were compacted (original paths).
        /// </summary>
        public IReadOnlyList<string>? CompactedFiles { get; init; }

        /// <summary>
        ///     Gets or sets the list of new files created.
        /// </summary>
        public IReadOnlyList<string>? NewFiles { get; init; }

        /// <summary>
        ///     Gets or sets a value indicating whether this was a dry run.
        /// </summary>
        public bool WasDryRun { get; init; }

        /// <summary>
        ///     Gets or sets an optional message with additional details.
        /// </summary>
        public string? Message { get; init; }

        /// <summary>
        ///     Creates a summary string for logging or debugging.
        /// </summary>
        /// <returns>A summary string.</returns>
        public override string ToString()
        {
            var dryRunText = WasDryRun ? " (dry run)" : "";
            return $"Compacted {FilesCompacted} files into {FilesCreated} files in {Duration.TotalSeconds:N1}s{dryRunText}";
        }
    }
}

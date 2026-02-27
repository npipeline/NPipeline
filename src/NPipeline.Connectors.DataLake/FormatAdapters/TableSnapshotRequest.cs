using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.DataLake.FormatAdapters
{
    /// <summary>
    ///     Request model for reading a table snapshot.
    /// </summary>
    public sealed class TableSnapshotRequest
    {
        /// <summary>
        ///     Gets or sets the base path of the table.
        /// </summary>
        public required StorageUri TableBasePath { get; init; }

        /// <summary>
        ///     Gets or sets the storage provider to use for reading.
        /// </summary>
        public required IStorageProvider Provider { get; init; }

        /// <summary>
        ///     Gets or sets the specific snapshot ID to read.
        ///     If <c>null</c>, returns the latest snapshot.
        /// </summary>
        public string? SnapshotId { get; init; }

        /// <summary>
        ///     Gets or sets the timestamp for time-travel queries.
        ///     Returns the snapshot as of this point in time.
        ///     Mutually exclusive with <see cref="SnapshotId" />.
        /// </summary>
        public DateTimeOffset? AsOf { get; init; }

        /// <summary>
        ///     Gets or sets optional partition filters to apply.
        ///     Only entries matching these filters will be included.
        /// </summary>
        public IReadOnlyDictionary<string, string>? PartitionFilters { get; init; }

        /// <summary>
        ///     Gets or sets a value indicating whether to include file metadata in the result.
        ///     Default is <c>true</c>.
        /// </summary>
        public bool IncludeFileMetadata { get; init; } = true;
    }
}

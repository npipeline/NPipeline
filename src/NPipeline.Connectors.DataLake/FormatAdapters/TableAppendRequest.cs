using NPipeline.Connectors.DataLake.Partitioning;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.DataLake.FormatAdapters;

/// <summary>
///     Request model for table append operations.
/// </summary>
public sealed class TableAppendRequest
{
    /// <summary>
    ///     Gets or sets the base path of the table.
    /// </summary>
    public required StorageUri TableBasePath { get; init; }

    /// <summary>
    ///     Gets or sets the storage provider to use for writing.
    /// </summary>
    public required IStorageProvider Provider { get; init; }

    /// <summary>
    ///     Gets or sets the partition specification for the table.
    ///     If <c>null</c>, data is written without partitioning.
    /// </summary>
    public PartitionSpec<object>? PartitionSpec { get; init; }

    /// <summary>
    ///     Gets or sets the snapshot ID for this append operation.
    ///     If <c>null</c>, a new snapshot ID will be generated.
    /// </summary>
    public string? SnapshotId { get; init; }

    /// <summary>
    ///     Gets or sets an optional idempotency key to prevent duplicate appends after caller retries.
    /// </summary>
    public string? IdempotencyKey { get; init; }

    /// <summary>
    ///     Gets or sets the row group size for Parquet files.
    ///     Default is 50,000 rows.
    /// </summary>
    public int RowGroupSize { get; init; } = 50_000;

    /// <summary>
    ///     Gets or sets the target file size in bytes.
    ///     Writer rotates to a new file once this threshold is reached.
    ///     Default is 256 MB.
    /// </summary>
    public long TargetFileSizeBytes { get; init; } = 256L * 1024 * 1024;

    /// <summary>
    ///     Gets or sets the maximum number of buffered rows across all partition buffers.
    ///     Protects memory during high-cardinality partition fan-out.
    ///     Default is 250,000 rows.
    /// </summary>
    public int MaxBufferedRows { get; init; } = 250_000;

    /// <summary>
    ///     Gets or sets a value indicating whether to use atomic writes (write to temp, then publish).
    ///     Default is <c>true</c>.
    /// </summary>
    public bool UseAtomicWrite { get; init; } = true;

    /// <summary>
    ///     Gets or sets an optional commit message or description for this append.
    /// </summary>
    public string? CommitMessage { get; init; }

    /// <summary>
    ///     Gets or sets additional metadata to include in the manifest entry.
    /// </summary>
    public IReadOnlyDictionary<string, string>? AdditionalMetadata { get; init; }
}

/// <summary>
///     Generic version of <see cref="TableAppendRequest" /> with typed partition spec.
/// </summary>
/// <typeparam name="T">The record type.</typeparam>
public sealed class TableAppendRequest<T>
{
    /// <summary>
    ///     Gets or sets the base path of the table.
    /// </summary>
    public required StorageUri TableBasePath { get; init; }

    /// <summary>
    ///     Gets or sets the storage provider to use for writing.
    /// </summary>
    public required IStorageProvider Provider { get; init; }

    /// <summary>
    ///     Gets or sets the partition specification for the table.
    ///     If <c>null</c>, data is written without partitioning.
    /// </summary>
    public PartitionSpec<T>? PartitionSpec { get; init; }

    /// <summary>
    ///     Gets or sets the snapshot ID for this append operation.
    ///     If <c>null</c>, a new snapshot ID will be generated.
    /// </summary>
    public string? SnapshotId { get; init; }

    /// <summary>
    ///     Gets or sets an optional idempotency key to prevent duplicate appends after caller retries.
    /// </summary>
    public string? IdempotencyKey { get; init; }

    /// <summary>
    ///     Gets or sets the row group size for Parquet files.
    ///     Default is 50,000 rows.
    /// </summary>
    public int RowGroupSize { get; init; } = 50_000;

    /// <summary>
    ///     Gets or sets the target file size in bytes.
    ///     Writer rotates to a new file once this threshold is reached.
    ///     Default is 256 MB.
    /// </summary>
    public long TargetFileSizeBytes { get; init; } = 256L * 1024 * 1024;

    /// <summary>
    ///     Gets or sets the maximum number of buffered rows across all partition buffers.
    ///     Protects memory during high-cardinality partition fan-out.
    ///     Default is 250,000 rows.
    /// </summary>
    public int MaxBufferedRows { get; init; } = 250_000;

    /// <summary>
    ///     Gets or sets a value indicating whether to use atomic writes (write to temp, then publish).
    ///     Default is <c>true</c>.
    /// </summary>
    public bool UseAtomicWrite { get; init; } = true;

    /// <summary>
    ///     Gets or sets an optional commit message or description for this append.
    /// </summary>
    public string? CommitMessage { get; init; }

    /// <summary>
    ///     Gets or sets additional metadata to include in the manifest entry.
    /// </summary>
    public IReadOnlyDictionary<string, string>? AdditionalMetadata { get; init; }

    /// <summary>
    ///     Converts this typed request to an untyped request.
    /// </summary>
    /// <returns>An untyped <see cref="TableAppendRequest" />.</returns>
    public TableAppendRequest ToUntyped()
    {
        return new TableAppendRequest
        {
            TableBasePath = TableBasePath,
            Provider = Provider,
            PartitionSpec = PartitionSpec is not null
                ? ConvertPartitionSpec(PartitionSpec)
                : null,
            SnapshotId = SnapshotId,
            IdempotencyKey = IdempotencyKey,
            RowGroupSize = RowGroupSize,
            TargetFileSizeBytes = TargetFileSizeBytes,
            MaxBufferedRows = MaxBufferedRows,
            UseAtomicWrite = UseAtomicWrite,
            CommitMessage = CommitMessage,
            AdditionalMetadata = AdditionalMetadata,
        };
    }

    private static PartitionSpec<object> ConvertPartitionSpec(PartitionSpec<T> _)
    {
        // This is a simplified conversion - in practice, the spec would need to be
        // properly converted to work with object types
        return PartitionSpec<object>.None();
    }
}

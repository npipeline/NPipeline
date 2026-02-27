using NPipeline.Connectors.DataLake.Snapshot;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.DataLake.FormatAdapters;

/// <summary>
///     Defines the interface for table format adapters (e.g., Iceberg, Delta, native).
///     Adapters handle format-specific metadata and commit semantics while reusing
///     the core DataLake abstractions.
/// </summary>
public interface ITableFormatAdapter
{
    /// <summary>
    ///     Gets the name of the format adapter (e.g., "native", "iceberg", "delta").
    /// </summary>
    string Name { get; }

    /// <summary>
    ///     Appends data to the table using the format-specific commit protocol.
    /// </summary>
    /// <param name="request">The append request containing data and options.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task AppendAsync(TableAppendRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Gets a snapshot of the table at a specific point in time or version.
    /// </summary>
    /// <param name="request">The snapshot request specifying version/time filters.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The table snapshot.</returns>
    Task<TableSnapshot> GetSnapshotAsync(TableSnapshotRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Compacts small files in the table to improve query performance.
    /// </summary>
    /// <param name="request">The compaction request specifying options.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The result of the compaction operation.</returns>
    Task<TableCompactResult> CompactAsync(TableCompactRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Gets the list of available snapshots for the table.
    /// </summary>
    /// <param name="tableBasePath">The base path of the table.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A list of snapshot summaries.</returns>
    Task<IReadOnlyList<SnapshotSummary>> ListSnapshotsAsync(
        StorageUri tableBasePath,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Checks if the table exists at the specified path.
    /// </summary>
    /// <param name="tableBasePath">The base path of the table.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns><c>true</c> if the table exists; otherwise, <c>false</c>.</returns>
    Task<bool> TableExistsAsync(StorageUri tableBasePath, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Creates a new empty table at the specified path.
    /// </summary>
    /// <param name="tableBasePath">The base path of the table.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task CreateTableAsync(StorageUri tableBasePath, CancellationToken cancellationToken = default);
}

/// <summary>
///     Represents a summary of a table snapshot.
/// </summary>
public sealed class SnapshotSummary
{
    /// <summary>
    ///     Gets or sets the snapshot ID.
    /// </summary>
    public required string SnapshotId { get; init; }

    /// <summary>
    ///     Gets or sets the timestamp when the snapshot was created.
    /// </summary>
    public required DateTimeOffset CreatedAt { get; init; }

    /// <summary>
    ///     Gets or sets the number of data files in the snapshot.
    /// </summary>
    public required int FileCount { get; init; }

    /// <summary>
    ///     Gets or sets the total row count in the snapshot.
    /// </summary>
    public required long TotalRowCount { get; init; }

    /// <summary>
    ///     Gets or sets the total size in bytes.
    /// </summary>
    public required long TotalSizeBytes { get; init; }

    /// <summary>
    ///     Gets or sets an optional summary or description.
    /// </summary>
    public string? Summary { get; init; }
}

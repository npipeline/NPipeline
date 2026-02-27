using NPipeline.Connectors.DataLake.Manifest;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.DataLake.Snapshot;

/// <summary>
///     Represents a table snapshot with its manifest entries and metadata.
///     Provides access to the data files and summary statistics for a point-in-time view of the table.
/// </summary>
public sealed class TableSnapshot
{
    private readonly List<ManifestEntry> _entries;

    /// <summary>
    ///     Initializes a new instance of the <see cref="TableSnapshot" /> class.
    /// </summary>
    /// <param name="snapshotId">The unique identifier for this snapshot.</param>
    /// <param name="entries">The manifest entries belonging to this snapshot.</param>
    /// <param name="tableBasePath">The base path of the table.</param>
    public TableSnapshot(
        string snapshotId,
        IReadOnlyList<ManifestEntry> entries,
        StorageUri tableBasePath)
    {
        ArgumentNullException.ThrowIfNull(snapshotId);
        ArgumentNullException.ThrowIfNull(entries);
        ArgumentNullException.ThrowIfNull(tableBasePath);

        SnapshotId = snapshotId;
        _entries = [.. entries];
        TableBasePath = tableBasePath;

        // Compute summary statistics
        TotalRowCount = _entries.Sum(e => e.RowCount);
        TotalFileSizeBytes = _entries.Sum(e => e.FileSizeBytes);
        FileCount = _entries.Count;

        EarliestWrittenAt = _entries.Count > 0
            ? _entries.Min(e => e.WrittenAt)
            : DateTimeOffset.MinValue;

        LatestWrittenAt = _entries.Count > 0
            ? _entries.Max(e => e.WrittenAt)
            : DateTimeOffset.MinValue;
    }

    /// <summary>
    ///     Gets the unique identifier for this snapshot.
    /// </summary>
    public string SnapshotId { get; }

    /// <summary>
    ///     Gets the manifest entries belonging to this snapshot.
    /// </summary>
    public IReadOnlyList<ManifestEntry> Entries => _entries;

    /// <summary>
    ///     Gets the base path of the table.
    /// </summary>
    public StorageUri TableBasePath { get; }

    /// <summary>
    ///     Gets the total number of rows across all files in this snapshot.
    /// </summary>
    public long TotalRowCount { get; }

    /// <summary>
    ///     Gets the total size of all files in bytes.
    /// </summary>
    public long TotalFileSizeBytes { get; }

    /// <summary>
    ///     Gets the number of data files in this snapshot.
    /// </summary>
    public int FileCount { get; }

    /// <summary>
    ///     Gets the timestamp of the earliest file written in this snapshot.
    /// </summary>
    public DateTimeOffset EarliestWrittenAt { get; }

    /// <summary>
    ///     Gets the timestamp of the most recent file written in this snapshot.
    /// </summary>
    public DateTimeOffset LatestWrittenAt { get; }

    /// <summary>
    ///     Gets a value indicating whether this snapshot is empty (has no data files).
    /// </summary>
    public bool IsEmpty => _entries.Count == 0;

    /// <summary>
    ///     Gets the partition column names present in this snapshot.
    /// </summary>
    /// <returns>A set of partition column names.</returns>
    public IReadOnlySet<string> GetPartitionColumns()
    {
        var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var entry in _entries)
        {
            if (entry.PartitionValues is not null)
            {
                foreach (var key in entry.PartitionValues.Keys)
                {
                    _ = columns.Add(key);
                }
            }
        }

        return columns;
    }

    /// <summary>
    ///     Gets the unique partition values for a specific column.
    /// </summary>
    /// <param name="columnName">The partition column name.</param>
    /// <returns>A set of unique partition values for the column.</returns>
    public IReadOnlySet<string> GetPartitionValues(string columnName)
    {
        ArgumentNullException.ThrowIfNull(columnName);

        var values = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var entry in _entries)
        {
            if (entry.PartitionValues is not null &&
                entry.PartitionValues.TryGetValue(columnName, out var value))
                _ = values.Add(value);
        }

        return values;
    }

    /// <summary>
    ///     Filters entries by partition value.
    /// </summary>
    /// <param name="columnName">The partition column name.</param>
    /// <param name="value">The partition value to filter by.</param>
    /// <returns>A filtered list of manifest entries.</returns>
    public IReadOnlyList<ManifestEntry> GetEntriesByPartition(string columnName, string value)
    {
        ArgumentNullException.ThrowIfNull(columnName);
        ArgumentNullException.ThrowIfNull(value);

        return _entries
            .Where(e => e.PartitionValues is not null &&
                        e.PartitionValues.TryGetValue(columnName, out var partitionValue) &&
                        string.Equals(partitionValue, value, StringComparison.OrdinalIgnoreCase))
            .ToList();
    }

    /// <summary>
    ///     Gets all data file paths in this snapshot.
    /// </summary>
    /// <returns>A list of relative file paths.</returns>
    public IReadOnlyList<string> GetFilePaths()
    {
        return _entries.Select(e => e.Path).ToList();
    }

    /// <summary>
    ///     Gets the full URIs for all data files in this snapshot.
    /// </summary>
    /// <returns>A list of full storage URIs.</returns>
    public IReadOnlyList<StorageUri> GetFileUris()
    {
        var basePath = TableBasePath.Path?.TrimStart('/') ?? string.Empty;

        return _entries.Select(e =>
        {
            var fullPath = string.IsNullOrEmpty(basePath)
                ? $"/{e.Path.TrimStart('/')}"
                : $"/{basePath}/{e.Path.TrimStart('/')}";

            return StorageUri.Parse($"{TableBasePath.Scheme}://{TableBasePath.Host}{fullPath}");
        }).ToList();
    }

    /// <summary>
    ///     Creates a summary string for logging or debugging.
    /// </summary>
    /// <returns>A summary string.</returns>
    public override string ToString()
    {
        return $"Snapshot {SnapshotId}: {FileCount} files, {TotalRowCount:N0} rows, {FormatBytes(TotalFileSizeBytes)}";
    }

    private static string FormatBytes(long bytes)
    {
        string[] suffixes = ["B", "KB", "MB", "GB", "TB"];
        var i = 0;
        double size = bytes;

        while (size >= 1024 && i < suffixes.Length - 1)
        {
            size /= 1024;
            i++;
        }

        return $"{size:N2} {suffixes[i]}";
    }
}

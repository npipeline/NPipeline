using Parquet;
using Parquet.Schema;

namespace NPipeline.Connectors.Parquet;

/// <summary>
///     Configuration for Parquet read and write operations with sensible defaults.
/// </summary>
public class ParquetConfiguration
{
    // --- Write options ---

    /// <summary>
    ///     Gets or sets the number of rows accumulated before flushing a row group.
    ///     Larger values result in fewer row groups and faster scans; smaller values reduce memory pressure.
    ///     Default is 50,000.
    /// </summary>
    public int RowGroupSize { get; set; } = 50_000;

    /// <summary>
    ///     Gets or sets the compression codec applied to column chunks.
    ///     Default is Snappy.
    /// </summary>
    public CompressionMethod Compression { get; set; } = CompressionMethod.Snappy;

    /// <summary>
    ///     Gets or sets the target file size in bytes for partitioned/lake writes.
    ///     The writer rotates to a new file once this threshold is reached (best effort).
    ///     Default is 256 MB. Set to null to disable file rotation.
    /// </summary>
    public long? TargetFileSizeBytes { get; set; } = 256L * 1024 * 1024;

    /// <summary>
    ///     Gets or sets a value indicating whether to write to a temporary path and atomically
    ///     publish the final path on success. Prevents partial-file visibility during failed writes.
    ///     Default is true.
    /// </summary>
    public bool UseAtomicWrite { get; set; } = true;

    /// <summary>
    ///     Gets or sets the maximum buffered rows across all partition buffers before forcing flush cycles.
    ///     Protects memory during high-cardinality partition fan-out.
    ///     Default is 250,000.
    /// </summary>
    public int MaxBufferedRows { get; set; } = 250_000;

    // --- Read options ---

    /// <summary>
    ///     Gets or sets the subset of column names to materialize.
    ///     When null, all columns are read.
    ///     Pushes column projection down to the row-group reader, avoiding I/O for unused columns.
    /// </summary>
    public IReadOnlyList<string>? ProjectedColumns { get; set; }

    /// <summary>
    ///     Gets or sets an optional callback to validate the file's schema before reading begins.
    ///     Return false to throw; throw directly to propagate a custom exception.
    /// </summary>
    public Func<ParquetSchema, bool>? SchemaValidator { get; set; }

    /// <summary>
    ///     Gets or sets the compatibility behavior when the file schema and CLR model diverge.
    ///     Default is Strict.
    /// </summary>
    public SchemaCompatibilityMode SchemaCompatibility { get; set; } = SchemaCompatibilityMode.Strict;

    /// <summary>
    ///     Gets or sets a value indicating whether to scan nested directories under a prefix
    ///     when discovering Parquet files.
    ///     Default is false to preserve existing connector semantics.
    /// </summary>
    public bool RecursiveDiscovery { get; set; }

    /// <summary>
    ///     Gets or sets the number of files read in parallel (per source node).
    ///     A value of 1 results in sequential deterministic reads.
    ///     Default is 1.
    /// </summary>
    /// <remarks>
    ///     Note: This property is reserved for future implementation.
    ///     Currently, all files are read sequentially regardless of this setting.
    /// </remarks>
    [Obsolete("FileReadParallelism is not yet implemented. Files are always read sequentially.")]
    public int FileReadParallelism { get; set; } = 1;

    /// <summary>
    ///     Gets or sets a best-effort row-group predicate.
    ///     Applied using row-group metadata where available, then validated at row level for correctness.
    /// </summary>
    public Func<ParquetRow, bool>? RowFilter { get; set; }

    // --- Error handling ---

    /// <summary>
    ///     Gets or sets the handler invoked when a row mapping throws during reading.
    ///     Return true to skip the row; return false or rethrow to fail the pipeline.
    /// </summary>
    public Func<Exception, ParquetRow, bool>? RowErrorHandler { get; set; }

    /// <summary>
    ///     Gets or sets an optional observer for connector-level metrics and events.
    /// </summary>
    public IParquetConnectorObserver? Observer { get; set; }

    /// <summary>
    ///     Validates the configuration and throws if invalid.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when configuration values are invalid.</exception>
    public void Validate()
    {
        if (RowGroupSize <= 0)
        {
            throw new InvalidOperationException($"{nameof(RowGroupSize)} must be greater than 0. Current value: {RowGroupSize}");
        }

        if (MaxBufferedRows < RowGroupSize)
        {
            throw new InvalidOperationException($"{nameof(MaxBufferedRows)} ({MaxBufferedRows}) must be greater than or equal to {nameof(RowGroupSize)} ({RowGroupSize})");
        }

        if (TargetFileSizeBytes is { } targetSize && targetSize < 32L * 1024 * 1024)
        {
            throw new InvalidOperationException($"{nameof(TargetFileSizeBytes)} must be at least 32 MiB when specified. Current value: {TargetFileSizeBytes}");
        }
    }
}

using NPipeline.Connectors.Files;
using NPipeline.Connectors.Parquet;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;
using Parquet;

namespace NPipeline.Connectors.DataLake;

/// <summary>How a Data Lake table's Parquet files are written and buffered.</summary>
/// <example>
///     <code>
/// var writer = new DataLakeTableWriter&lt;Order&gt;(provider, table, spec, new DataLakeParquetOptions { Codec = CompressionMethod.Zstd });
///     </code>
/// </example>
public sealed record DataLakeParquetOptions
{
    /// <summary>The default options.</summary>
    public static DataLakeParquetOptions Default { get; } = new();

    /// <summary>
    ///     The rows buffered per partition before a data file is written, and the most rows per row group. Defaults to
    ///     50,000.
    /// </summary>
    public int RowGroupSize { get; init; } = ParquetWriteOptions.DefaultRowGroupSize;

    /// <summary>A row group is also flushed once its buffered values reach about this many bytes. Defaults to 128 MB.</summary>
    public long RowGroupBytes { get; init; } = ParquetWriteOptions.DefaultRowGroupBytes;

    /// <summary>The compression codec. Defaults to Snappy.</summary>
    public CompressionMethod Codec { get; init; } = CompressionMethod.Snappy;

    /// <summary>
    ///     The most rows buffered across all partitions; beyond it the largest buffers are written early, which bounds memory
    ///     when records fan out to many partitions. Defaults to 250,000.
    /// </summary>
    public int MaxBufferedRows { get; init; } = 250_000;

    internal DataLakeParquetOptions Validated()
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(RowGroupSize, nameof(RowGroupSize));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(RowGroupBytes, nameof(RowGroupBytes));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaxBufferedRows, nameof(MaxBufferedRows));
        return this;
    }

    /// <summary>The sink for one data file. A file becomes visible only when its write commits, and the manifest publishes it.</summary>
    internal ParquetSinkNode<T> Sink<T>(IStorageProvider provider, StorageUri file) =>
        new(new ParquetWriteOptions
        {
            Uri = file,
            Provider = provider,
            RowGroupSize = RowGroupSize,
            RowGroupBytes = RowGroupBytes,
            Codec = Codec,
        });

    /// <summary>The source for one data file. Partition values are stored in the files, so they are not read from the path.</summary>
    internal static ParquetSourceNode<T> Source<T>(IStorageProvider provider, StorageUri file) =>
        new(new ParquetReadOptions { Uri = file, Provider = provider, PartitionColumns = false });

    /// <summary>A source of every column of one data file as rows, for compaction.</summary>
    internal static ParquetSourceNode<ParquetRow> Rows(IStorageProvider provider, StorageUri file) =>
        new(new ParquetReadOptions { Uri = file, Provider = provider, PartitionColumns = false }, static row => row);
}

using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Parquet;

/// <summary>
///     Observer interface for Parquet connector metrics and events.
///     Implementations can bridge to logging frameworks, OpenTelemetry, or metrics systems.
/// </summary>
public interface IParquetConnectorObserver
{
    /// <summary>
    ///     Called when a Parquet file read operation starts.
    /// </summary>
    /// <param name="uri">The URI of the file being read.</param>
    void OnFileReadStarted(StorageUri uri);

    /// <summary>
    ///     Called when a Parquet file read operation completes successfully.
    /// </summary>
    /// <param name="uri">The URI of the file that was read.</param>
    /// <param name="rows">The number of rows read from the file.</param>
    /// <param name="bytes">The number of bytes read from the file.</param>
    /// <param name="elapsed">The time taken to read the file.</param>
    void OnFileReadCompleted(StorageUri uri, long rows, long bytes, TimeSpan elapsed);

    /// <summary>
    ///     Called when a Parquet file write operation completes successfully.
    /// </summary>
    /// <param name="uri">The URI of the file that was written.</param>
    /// <param name="rows">The number of rows written to the file.</param>
    /// <param name="bytes">The number of bytes written to the file.</param>
    /// <param name="elapsed">The time taken to write the file.</param>
    void OnFileWriteCompleted(StorageUri uri, long rows, long bytes, TimeSpan elapsed);

    /// <summary>
    ///     Called when a row mapping error occurs during reading.
    /// </summary>
    /// <param name="uri">The URI of the file being processed when the error occurred.</param>
    /// <param name="exception">The exception that was thrown during row mapping.</param>
    void OnRowMappingError(StorageUri uri, Exception exception);

    /// <summary>
    ///     Called when a row group is read from a Parquet file.
    /// </summary>
    /// <param name="uri">The URI of the file being read.</param>
    /// <param name="rowGroupIndex">The zero-based index of the row group.</param>
    /// <param name="rowCount">The number of rows in the row group.</param>
    void OnRowGroupRead(StorageUri uri, int rowGroupIndex, long rowCount);

    /// <summary>
    ///     Called when a row group is written to a Parquet file.
    /// </summary>
    /// <param name="uri">The URI of the file being written.</param>
    /// <param name="rowGroupIndex">The zero-based index of the row group.</param>
    /// <param name="rowCount">The number of rows in the row group.</param>
    void OnRowGroupWritten(StorageUri uri, int rowGroupIndex, long rowCount);
}

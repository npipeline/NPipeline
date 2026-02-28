namespace NPipeline.Connectors.DuckDB;

/// <summary>
///     Optional observer for DuckDB connector metrics and diagnostics.
///     All methods have default empty implementations so implementors only override what they need.
/// </summary>
public interface IDuckDBConnectorObserver
{
    /// <summary>Called when a row is successfully read from DuckDB.</summary>
    void OnRowRead(long rowIndex)
    {
    }

    /// <summary>Called when a row is skipped due to a mapping error.</summary>
    void OnRowSkipped(long rowIndex, Exception error)
    {
    }

    /// <summary>Called when all rows have been read.</summary>
    void OnReadCompleted(long totalRows)
    {
    }

    /// <summary>Called when a row is written to DuckDB.</summary>
    void OnRowWritten(long rowCount)
    {
    }

    /// <summary>Called when all rows have been written.</summary>
    void OnWriteCompleted(long totalRows)
    {
    }

    /// <summary>Called when a batch is flushed (SQL write strategy).</summary>
    void OnBatchFlushed(int batchSize, long totalRows)
    {
    }

    /// <summary>Called when a DuckDB extension is loaded.</summary>
    void OnExtensionLoaded(string extensionName)
    {
    }

    /// <summary>Called when a query execution starts.</summary>
    void OnQueryStarted(string query)
    {
    }

    /// <summary>Called with DuckDB progress info (if enabled).</summary>
    void OnQueryProgress(double percentage)
    {
    }
}

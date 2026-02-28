namespace NPipeline.Connectors.DuckDB.Exceptions;

/// <summary>
///     Exception thrown when row mapping fails in the DuckDB connector.
/// </summary>
public sealed class DuckDBMappingException : DuckDBConnectorException
{
    /// <summary>
    ///     Creates a new <see cref="DuckDBMappingException" />.
    /// </summary>
    public DuckDBMappingException(string message, long? rowIndex = null, Exception? innerException = null)
        : base(FormatMessage(message, rowIndex), innerException!)
    {
        RowIndex = rowIndex;
    }

    /// <summary>
    ///     The zero-based row index where the mapping error occurred, if available.
    /// </summary>
    public long? RowIndex { get; }

    private static string FormatMessage(string message, long? rowIndex)
    {
        return rowIndex.HasValue
            ? $"Row {rowIndex}: {message}"
            : message;
    }
}

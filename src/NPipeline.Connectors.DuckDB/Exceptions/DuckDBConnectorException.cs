namespace NPipeline.Connectors.DuckDB.Exceptions;

/// <summary>
///     Base exception for DuckDB connector errors.
/// </summary>
public class DuckDBConnectorException : Exception
{
    /// <summary>
    ///     Creates a new <see cref="DuckDBConnectorException" /> with the specified message.
    /// </summary>
    public DuckDBConnectorException(string message) : base(message)
    {
    }

    /// <summary>
    ///     Creates a new <see cref="DuckDBConnectorException" /> with the specified message and inner exception.
    /// </summary>
    public DuckDBConnectorException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

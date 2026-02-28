namespace NPipeline.Connectors.DuckDB.Exceptions;

/// <summary>
///     Exception thrown when a DuckDB connection cannot be established.
/// </summary>
public sealed class DuckDBConnectionException : DuckDBConnectorException
{
    /// <summary>
    ///     Creates a new <see cref="DuckDBConnectionException" />.
    /// </summary>
    public DuckDBConnectionException(string message, string? databasePath = null, Exception? innerException = null)
        : base(FormatMessage(message, databasePath), innerException!)
    {
        DatabasePath = databasePath;
    }

    /// <summary>
    ///     The database path that failed to connect, if available.
    /// </summary>
    public string? DatabasePath { get; }

    private static string FormatMessage(string message, string? databasePath)
    {
        return databasePath is not null
            ? $"{message} | Database: {databasePath}"
            : message;
    }
}

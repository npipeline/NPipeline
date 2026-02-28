namespace NPipeline.Connectors.DuckDB.Exceptions;

/// <summary>
///     Exception thrown when a SQL query fails in the DuckDB connector.
/// </summary>
public sealed class DuckDBQueryException : DuckDBConnectorException
{
    /// <summary>
    ///     Creates a new <see cref="DuckDBQueryException" />.
    /// </summary>
    public DuckDBQueryException(string message, string? query = null, Exception? innerException = null)
        : base(FormatMessage(message, query), innerException!)
    {
        Query = query;
    }

    /// <summary>
    ///     The SQL query that failed (may be truncated for security).
    /// </summary>
    public string? Query { get; }

    private static string FormatMessage(string message, string? query)
    {
        if (query is null)
            return message;

        var truncated = query.Length > 200
            ? query[..200] + "..."
            : query;

        return $"{message} | Query: {truncated}";
    }
}

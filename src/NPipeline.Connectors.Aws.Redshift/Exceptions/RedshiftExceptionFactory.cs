using Npgsql;

namespace NPipeline.Connectors.Aws.Redshift.Exceptions;

/// <summary>
///     Factory for creating appropriate Redshift exception types from Npgsql errors.
/// </summary>
public static class RedshiftExceptionFactory
{
    /// <summary>
    ///     Creates an appropriate exception from an NpgsqlException.
    /// </summary>
    /// <param name="exception">The Npgsql exception.</param>
    /// <param name="sql">The SQL that was being executed, if available.</param>
    /// <returns>An appropriate RedshiftException subtype.</returns>
    public static Exception Create(NpgsqlException exception, string? sql = null)
    {
        ArgumentNullException.ThrowIfNull(exception);

        var sqlState = exception.SqlState;

        // Connection errors (class 08)
        if (IsConnectionError(sqlState))
        {
            return new RedshiftConnectionException(
                $"Redshift connection error: {exception.Message}",
                null,
                exception);
        }

        // Default to base exception
        return new RedshiftException(
            $"Redshift error: {exception.Message}",
            sql,
            exception);
    }

    /// <summary>
    ///     Creates a RedshiftMappingException.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="mappedType">The type being mapped.</param>
    /// <param name="propertyName">The property that failed to map.</param>
    /// <param name="columnName">The column that failed to map.</param>
    /// <param name="innerException">The inner exception.</param>
    /// <returns>A new RedshiftMappingException.</returns>
    public static RedshiftMappingException CreateMappingException(
        string message,
        Type? mappedType = null,
        string? propertyName = null,
        string? columnName = null,
        Exception? innerException = null)
    {
        return new RedshiftMappingException(message, mappedType, propertyName, columnName, innerException);
    }

    /// <summary>
    ///     Creates a RedshiftConnectionException.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="connectionString">The connection string that failed.</param>
    /// <param name="innerException">The inner exception.</param>
    /// <returns>A new RedshiftConnectionException.</returns>
    public static RedshiftConnectionException CreateConnectionException(
        string message,
        string? connectionString = null,
        Exception? innerException = null)
    {
        return new RedshiftConnectionException(message, connectionString, innerException);
    }

    private static bool IsConnectionError(string? sqlState)
    {
        if (string.IsNullOrEmpty(sqlState))
            return false;

        // PostgreSQL/Redshift connection error SQLSTATE classes
        // 08xxx - Connection Exception
        // 57P01 - admin_shutdown
        // 57P02 - crash_shutdown
        // 57P03 - cannot_connect_now
        // 53300 - too_many_connections
        return sqlState.StartsWith("08", StringComparison.Ordinal) ||
               sqlState is "57P01" or "57P02" or "57P03" or "53300";
    }
}

using Microsoft.Data.SqlClient;

namespace NPipeline.Connectors.SqlServer.Exceptions;

/// <summary>
///     Factory for creating SQL Server-specific exceptions from SqlClient exceptions.
/// </summary>
public static class SqlServerExceptionFactory
{
    /// <summary>
    ///     Creates a SQL Server exception from a generic exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    /// <returns>A SQL Server-specific exception.</returns>
    public static SqlServerException Create(string message, Exception? innerException = null)
    {
        if (innerException is null)
            return new SqlServerException(message);

        return innerException switch
        {
            SqlServerException sqlServerEx => sqlServerEx,
            SqlException sqlEx => CreateFromSqlException(message, sqlEx),
            TimeoutException timeoutEx => new SqlServerException(
                $"{message} (Timeout)",
                null,
                true,
                timeoutEx),
            OperationCanceledException canceledEx => new SqlServerException(
                $"{message} (Operation canceled)",
                null,
                true,
                canceledEx),
            _ => new SqlServerException(message, innerException),
        };
    }

    /// <summary>
    ///     Creates a transient SQL Server exception from a generic exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    /// <returns>A SQL Server-specific exception marked as transient.</returns>
    public static SqlServerException CreateTransient(string message, Exception? innerException = null)
    {
        if (innerException is null)
            return new SqlServerException(message, null, true);

        return innerException switch
        {
            SqlServerException sqlServerEx => sqlServerEx,
            SqlException sqlEx => CreateFromSqlException(message, sqlEx),
            TimeoutException timeoutEx => new SqlServerException(
                $"{message} (Timeout)",
                null,
                true,
                timeoutEx),
            OperationCanceledException canceledEx => new SqlServerException(
                $"{message} (Operation canceled)",
                null,
                true,
                canceledEx),
            _ => new SqlServerException(message, null, true, innerException),
        };
    }

    /// <summary>
    ///     Creates a SQL Server connection exception from a generic exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    /// <returns>A SQL Server connection exception.</returns>
    public static SqlServerConnectionException CreateConnection(string message, Exception? innerException = null)
    {
        if (innerException is null)
            return new SqlServerConnectionException(message);

        return innerException switch
        {
            SqlServerConnectionException connectionEx => connectionEx,
            SqlException sqlEx => CreateConnectionFromSqlException(message, sqlEx),
            TimeoutException timeoutEx => new SqlServerConnectionException(
                $"{message} (Connection timeout)",
                null,
                timeoutEx),
            InvalidOperationException invalidOpEx when
                invalidOpEx.Message.Contains("connection", StringComparison.OrdinalIgnoreCase) =>
                new SqlServerConnectionException(message, null, invalidOpEx),
            _ => new SqlServerConnectionException(message, innerException),
        };
    }

    /// <summary>
    ///     Creates a SQL Server mapping exception from a generic exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    /// <returns>A SQL Server mapping exception.</returns>
    public static SqlServerMappingException CreateMapping(string message, Exception? innerException = null)
    {
        if (innerException is SqlServerMappingException mappingEx)
            return mappingEx;

        return innerException is null
            ? new SqlServerMappingException(message)
            : new SqlServerMappingException(message, innerException);
    }

    private static SqlServerException CreateFromSqlException(string message, SqlException sqlEx)
    {
        var errorCode = SqlServerTransientErrorDetector.GetErrorCode(sqlEx);
        var isTransient = SqlServerTransientErrorDetector.IsTransient(sqlEx);
        var errorCodeString = errorCode?.ToString();

        return new SqlServerException(message, errorCodeString, isTransient, sqlEx);
    }

    private static SqlServerConnectionException CreateConnectionFromSqlException(string message, SqlException sqlEx)
    {
        var errorCode = SqlServerTransientErrorDetector.GetErrorCode(sqlEx);
        var errorCodeString = errorCode?.ToString();

        return new SqlServerConnectionException(message, errorCodeString, sqlEx);
    }
}

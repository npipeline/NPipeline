using System.Data.Common;

namespace NPipeline.Connectors.Snowflake.Exceptions;

/// <summary>
///     Factory for creating Snowflake-specific exceptions from driver exceptions.
/// </summary>
public static class SnowflakeExceptionFactory
{
    /// <summary>
    ///     Creates a Snowflake exception from a generic exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    /// <returns>A Snowflake-specific exception.</returns>
    public static SnowflakeException Create(string message, Exception? innerException = null)
    {
        if (innerException is null)
            return new SnowflakeException(message);

        return innerException switch
        {
            SnowflakeException snowflakeEx => snowflakeEx,
            DbException dbEx => CreateFromDbException(message, dbEx),
            TimeoutException timeoutEx => new SnowflakeException(
                $"{message} (Timeout)",
                null,
                true,
                timeoutEx),
            OperationCanceledException canceledEx => new SnowflakeException(
                $"{message} (Operation canceled)",
                null,
                true,
                canceledEx),
            _ => new SnowflakeException(message, innerException),
        };
    }

    /// <summary>
    ///     Creates a transient Snowflake exception from a generic exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    /// <returns>A Snowflake-specific exception marked as transient.</returns>
    public static SnowflakeException CreateTransient(string message, Exception? innerException = null)
    {
        if (innerException is null)
            return new SnowflakeException(message, null, true);

        return innerException switch
        {
            SnowflakeException snowflakeEx => snowflakeEx,
            DbException dbEx => CreateFromDbException(message, dbEx),
            TimeoutException timeoutEx => new SnowflakeException(
                $"{message} (Timeout)",
                null,
                true,
                timeoutEx),
            OperationCanceledException canceledEx => new SnowflakeException(
                $"{message} (Operation canceled)",
                null,
                true,
                canceledEx),
            _ => new SnowflakeException(message, null, true, innerException),
        };
    }

    /// <summary>
    ///     Creates a Snowflake connection exception from a generic exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    /// <returns>A Snowflake connection exception.</returns>
    public static SnowflakeConnectionException CreateConnection(string message, Exception? innerException = null)
    {
        if (innerException is null)
            return new SnowflakeConnectionException(message);

        return innerException switch
        {
            SnowflakeConnectionException connectionEx => connectionEx,
            DbException dbEx => CreateConnectionFromDbException(message, dbEx),
            TimeoutException timeoutEx => new SnowflakeConnectionException(
                $"{message} (Connection timeout)",
                null,
                timeoutEx),
            InvalidOperationException invalidOpEx when
                invalidOpEx.Message.Contains("connection", StringComparison.OrdinalIgnoreCase) =>
                new SnowflakeConnectionException(message, null, invalidOpEx),
            _ => new SnowflakeConnectionException(message, innerException),
        };
    }

    /// <summary>
    ///     Creates a Snowflake mapping exception from a generic exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    /// <returns>A Snowflake mapping exception.</returns>
    public static SnowflakeMappingException CreateMapping(string message, Exception? innerException = null)
    {
        if (innerException is SnowflakeMappingException mappingEx)
            return mappingEx;

        return innerException is null
            ? new SnowflakeMappingException(message)
            : new SnowflakeMappingException(message, innerException);
    }

    private static SnowflakeException CreateFromDbException(string message, DbException dbEx)
    {
        var errorCode = SnowflakeTransientErrorDetector.GetErrorCode(dbEx);
        var isTransient = SnowflakeTransientErrorDetector.IsTransient(dbEx);
        var errorCodeString = errorCode?.ToString();

        return new SnowflakeException(message, errorCodeString, isTransient, dbEx);
    }

    private static SnowflakeConnectionException CreateConnectionFromDbException(string message, DbException dbEx)
    {
        var errorCode = SnowflakeTransientErrorDetector.GetErrorCode(dbEx);
        var errorCodeString = errorCode?.ToString();

        return new SnowflakeConnectionException(message, errorCodeString, dbEx);
    }
}

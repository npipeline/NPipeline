using Microsoft.Data.SqlClient;
using NPipeline.Connectors.SqlServer.Configuration;

namespace NPipeline.Connectors.SqlServer.Exceptions;

/// <summary>
///     Utility methods for handling SQL Server exceptions and implementing retry logic.
/// </summary>
public static class SqlServerExceptionHandler
{
    /// <summary>
    ///     Human-readable descriptions for common SQL Server error codes.
    /// </summary>
    private static readonly Dictionary<int, string> ErrorDescriptions = new()
    {
        [-2] = "Timeout expired",
        [53] = "Named Pipes Provider error (network not found)",
        [64] = "Named Pipes Provider error (network disconnected)",
        [121] = "Named Pipes Provider error (network timeout)",
        [208] = "Invalid object name",
        [229] = "Permission denied",
        [1205] = "Deadlock victim",
        [2601] = "Cannot insert duplicate key row (unique constraint)",
        [2627] = "Violation of UNIQUE KEY constraint",
        [547] = "Foreign key constraint violation",
        [8152] = "String or binary data would be truncated",
        [40501] = "Azure SQL Database service busy",
        [40613] = "Azure SQL Database service unavailable",
        [49918] = "Azure SQL Database insufficient resources",
        [49919] = "Azure SQL Database insufficient resources",
        [49920] = "Azure SQL Database insufficient resources",
    };

    /// <summary>
    ///     Determines whether an exception should be retried based on the configuration.
    /// </summary>
    /// <param name="exception">The exception to evaluate.</param>
    /// <param name="configuration">The SQL Server configuration.</param>
    /// <returns>True if the exception should be retried; otherwise, false.</returns>
    public static bool ShouldRetry(Exception exception, SqlServerConfiguration configuration)
    {
        if (configuration.MaxRetryAttempts <= 0)
            return false;

        return SqlServerTransientErrorDetector.IsTransient(exception);
    }

    /// <summary>
    ///     Gets the retry delay for a given attempt using exponential backoff with jitter.
    /// </summary>
    /// <param name="exception">The exception that triggered the retry.</param>
    /// <param name="attemptCount">The current attempt count (1-based).</param>
    /// <param name="configuration">The SQL Server configuration.</param>
    /// <returns>The delay before the next retry attempt.</returns>
    public static TimeSpan GetRetryDelay(Exception exception, int attemptCount, SqlServerConfiguration configuration)
    {
        // Calculate exponential backoff: baseDelay * 2^(attemptCount - 1)
        var exponentialDelay = TimeSpan.FromSeconds(
            configuration.RetryDelay.TotalSeconds * Math.Pow(2, attemptCount - 1));

        // Add jitter to avoid thundering herd problem (±25%)
        var jitterFactor = 0.75 + Random.Shared.NextDouble() * 0.5;

        var delayWithJitter = TimeSpan.FromMilliseconds(
            exponentialDelay.TotalMilliseconds * jitterFactor);

        // Cap at a reasonable maximum (30 seconds)
        const int maxDelaySeconds = 30;

        if (delayWithJitter.TotalSeconds > maxDelaySeconds)
            delayWithJitter = TimeSpan.FromSeconds(maxDelaySeconds);

        return delayWithJitter;
    }

    /// <summary>
    ///     Handles an exception by either wrapping it in a SqlServerException or rethrowing it.
    /// </summary>
    /// <param name="exception">The exception to handle.</param>
    /// <param name="configuration">The SQL Server configuration.</param>
    /// <exception cref="SqlServerException">Thrown when the exception is not transient.</exception>
    /// <exception cref="SqlServerConnectionException">Thrown when the exception is a connection error.</exception>
    public static void Handle(Exception exception, SqlServerConfiguration configuration)
    {
        if (exception is SqlServerException or SqlServerConnectionException or SqlServerMappingException)
            throw exception;

        var isTransient = SqlServerTransientErrorDetector.IsTransient(exception);
        var errorCode = GetErrorCode(exception);

        if (IsConnectionError(exception))
        {
            throw SqlServerExceptionFactory.CreateConnection(
                $"Connection error: {exception.Message}",
                exception);
        }

        throw new SqlServerException(
            exception.Message,
            errorCode,
            isTransient,
            exception);
    }

    /// <summary>
    ///     Gets the SQL Server error code from an exception.
    /// </summary>
    /// <param name="exception">The exception to extract the error code from.</param>
    /// <returns>The error code as a string, or null if not found.</returns>
    public static string? GetErrorCode(Exception exception)
    {
        return exception switch
        {
            SqlException sqlEx => SqlServerTransientErrorDetector.GetErrorCode(sqlEx)?.ToString(),
            SqlServerException sqlServerEx => sqlServerEx.ErrorCode,
            _ => null,
        };
    }

    /// <summary>
    ///     Returns a human-readable description for a SQL Server error code.
    /// </summary>
    /// <param name="errorCode">The SQL Server error code.</param>
    /// <returns>A description of the error, or null if not found.</returns>
    public static string? GetErrorDescription(int errorCode)
    {
        return ErrorDescriptions.TryGetValue(errorCode, out var description)
            ? description
            : null;
    }

    /// <summary>
    ///     Determines if an exception represents a connection error.
    /// </summary>
    /// <param name="exception">The exception to check.</param>
    /// <returns>True if the exception is a connection error; otherwise, false.</returns>
    private static bool IsConnectionError(Exception exception)
    {
        return exception switch
        {
            SqlServerConnectionException => true,
            SqlException sqlEx when sqlEx.Errors.Count > 0 =>
                sqlEx.Errors.Cast<SqlError>().Any(e => IsConnectionErrorCode(e.Number)),
            InvalidOperationException invalidOpEx when
                invalidOpEx.Message.Contains("connection", StringComparison.OrdinalIgnoreCase) => true,
            _ => false,
        };
    }

    /// <summary>
    ///     Determines if a SQL Server error code represents a connection error.
    /// </summary>
    /// <param name="errorCode">The SQL Server error code.</param>
    /// <returns>True if the error code is a connection error; otherwise, false.</returns>
    private static bool IsConnectionErrorCode(int errorCode)
    {
        return errorCode is -2 or 53 or 64 or 121;
    }
}

using System.Data.Common;
using NPipeline.Connectors.Snowflake.Configuration;

namespace NPipeline.Connectors.Snowflake.Exceptions;

/// <summary>
///     Utility methods for handling Snowflake exceptions and implementing retry logic.
/// </summary>
public static class SnowflakeExceptionHandler
{
    /// <summary>
    ///     Human-readable descriptions for common Snowflake error codes.
    /// </summary>
    private static readonly Dictionary<int, string> ErrorDescriptions = new()
    {
        [390114] = "User temporarily locked",
        [390144] = "Service unavailable (maintenance)",
        [200002] = "Network error",
        [625] = "Statement timeout",
        [604] = "SQL execution internal error",
        [2003] = "Object does not exist or not authorized",
        [2025] = "Warehouse not found",
        [90105] = "Cannot perform operation; account is locked",
        [2043] = "Column not found",
        [100038] = "External function error",
    };

    /// <summary>
    ///     Determines whether an exception should be retried based on the configuration.
    /// </summary>
    /// <param name="exception">The exception to evaluate.</param>
    /// <param name="configuration">The Snowflake configuration.</param>
    /// <returns>True if the exception should be retried; otherwise, false.</returns>
    public static bool ShouldRetry(Exception exception, SnowflakeConfiguration configuration)
    {
        if (configuration.MaxRetryAttempts <= 0)
            return false;

        return SnowflakeTransientErrorDetector.IsTransient(exception);
    }

    /// <summary>
    ///     Gets the retry delay for a given attempt using exponential backoff with jitter.
    /// </summary>
    /// <param name="exception">The exception that triggered the retry.</param>
    /// <param name="attemptCount">The current attempt count (1-based).</param>
    /// <param name="configuration">The Snowflake configuration.</param>
    /// <returns>The delay before the next retry attempt.</returns>
    public static TimeSpan GetRetryDelay(Exception exception, int attemptCount, SnowflakeConfiguration configuration)
    {
        // Calculate exponential backoff: baseDelay * 2^(attemptCount - 1)
        var exponentialDelay = TimeSpan.FromSeconds(
            configuration.RetryDelay.TotalSeconds * Math.Pow(2, attemptCount - 1));

        // Add jitter to avoid thundering herd problem (±25%)
        var jitterFactor = 0.75 + Random.Shared.NextDouble() * 0.5;

        var delayWithJitter = TimeSpan.FromMilliseconds(
            exponentialDelay.TotalMilliseconds * jitterFactor);

        // Cap at 60 seconds for cloud service (higher than SqlServer's 30s due to Snowflake latency)
        const int maxDelaySeconds = 60;

        if (delayWithJitter.TotalSeconds > maxDelaySeconds)
            delayWithJitter = TimeSpan.FromSeconds(maxDelaySeconds);

        return delayWithJitter;
    }

    /// <summary>
    ///     Handles an exception by either wrapping it in a SnowflakeException or rethrowing it.
    /// </summary>
    /// <param name="exception">The exception to handle.</param>
    /// <param name="configuration">The Snowflake configuration.</param>
    /// <exception cref="SnowflakeException">Thrown when the exception is not transient.</exception>
    /// <exception cref="SnowflakeConnectionException">Thrown when the exception is a connection error.</exception>
    public static void Handle(Exception exception, SnowflakeConfiguration configuration)
    {
        if (exception is SnowflakeException or SnowflakeConnectionException or SnowflakeMappingException)
            throw exception;

        var isTransient = SnowflakeTransientErrorDetector.IsTransient(exception);
        var errorCode = GetErrorCode(exception);

        if (IsConnectionError(exception))
        {
            throw SnowflakeExceptionFactory.CreateConnection(
                $"Connection error: {exception.Message}",
                exception);
        }

        throw new SnowflakeException(
            exception.Message,
            errorCode,
            isTransient,
            exception);
    }

    /// <summary>
    ///     Gets the Snowflake error code from an exception.
    /// </summary>
    /// <param name="exception">The exception to extract the error code from.</param>
    /// <returns>The error code as a string, or null if not found.</returns>
    public static string? GetErrorCode(Exception exception)
    {
        return exception switch
        {
            DbException dbEx => SnowflakeTransientErrorDetector.GetErrorCode(dbEx)?.ToString(),
            SnowflakeException snowflakeEx => snowflakeEx.ErrorCode,
            _ => null,
        };
    }

    /// <summary>
    ///     Returns a human-readable description for a Snowflake error code.
    /// </summary>
    /// <param name="errorCode">The Snowflake error code.</param>
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
            SnowflakeConnectionException => true,
            DbException dbEx when IsConnectionErrorMessage(dbEx.Message) => true,
            InvalidOperationException invalidOpEx when
                invalidOpEx.Message.Contains("connection", StringComparison.OrdinalIgnoreCase) => true,
            _ => false,
        };
    }

    /// <summary>
    ///     Determines if an error message indicates a connection error.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <returns>True if the message indicates a connection error; otherwise, false.</returns>
    private static bool IsConnectionErrorMessage(string message)
    {
        if (string.IsNullOrWhiteSpace(message))
            return false;

        return message.Contains("connection", StringComparison.OrdinalIgnoreCase)
               || message.Contains("network", StringComparison.OrdinalIgnoreCase)
               || message.Contains("unable to connect", StringComparison.OrdinalIgnoreCase);
    }
}

using System.Data.Common;
using System.Net.Sockets;

namespace NPipeline.Connectors.Snowflake.Exceptions;

/// <summary>
///     Detects transient (retryable) Snowflake errors.
/// </summary>
public static class SnowflakeTransientErrorDetector
{
    /// <summary>
    ///     Snowflake error codes that are transient (retryable).
    /// </summary>
    /// <remarks>
    ///     390114: User temporarily locked
    ///     390144: Service unavailable (maintenance)
    ///     200002: Network error
    ///     000625: Statement timeout
    ///     000604: SQL execution internal error
    /// </remarks>
    private static readonly HashSet<int> TransientErrorCodes =
    [
        390114, // User temporarily locked
        390144, // Service unavailable (maintenance)
        200002, // Network error
        000625, // Statement timeout
        000604, // SQL execution internal error
    ];

    /// <summary>
    ///     Determines if an exception represents a transient (retryable) error.
    /// </summary>
    /// <param name="exception">The exception to check.</param>
    /// <returns>True if error is transient; otherwise, false.</returns>
    public static bool IsTransient(Exception exception)
    {
        return exception switch
        {
            TimeoutException => true,
            OperationCanceledException => true,
            HttpRequestException => true,
            SocketException => true,
            DbException dbEx when HasTransientErrorCode(dbEx) => true,
            DbException dbEx when IsTransientMessage(dbEx.Message) => true,
            InvalidOperationException invalidOpEx when
                invalidOpEx.Message.Contains("timeout", StringComparison.OrdinalIgnoreCase) ||
                invalidOpEx.Message.Contains("connection", StringComparison.OrdinalIgnoreCase) => true,
            _ when exception.InnerException != null => IsTransient(exception.InnerException),
            _ => false,
        };
    }

    /// <summary>
    ///     Gets the Snowflake error code from a DbException.
    /// </summary>
    /// <param name="exception">The DbException to extract the error code from.</param>
    /// <returns>The error code, or null if not found.</returns>
    public static int? GetErrorCode(DbException exception)
    {
        // Snowflake.Data driver sets ErrorCode on SnowflakeDbException
        return exception.ErrorCode != 0
            ? exception.ErrorCode
            : null;
    }

    /// <summary>
    ///     Determines if a specific Snowflake error code is transient.
    /// </summary>
    /// <param name="errorCode">The Snowflake error code.</param>
    /// <returns>True if the error code is transient; otherwise, false.</returns>
    public static bool IsTransientError(int errorCode)
    {
        return TransientErrorCodes.Contains(errorCode);
    }

    private static bool HasTransientErrorCode(DbException exception)
    {
        var errorCode = GetErrorCode(exception);
        return errorCode.HasValue && TransientErrorCodes.Contains(errorCode.Value);
    }

    private static bool IsTransientMessage(string message)
    {
        if (string.IsNullOrWhiteSpace(message))
            return false;

        return message.Contains("timeout", StringComparison.OrdinalIgnoreCase)
               || message.Contains("service unavailable", StringComparison.OrdinalIgnoreCase)
               || message.Contains("temporarily unavailable", StringComparison.OrdinalIgnoreCase)
               || message.Contains("network", StringComparison.OrdinalIgnoreCase)
               || message.Contains("throttled", StringComparison.OrdinalIgnoreCase)
               || message.Contains("429", StringComparison.OrdinalIgnoreCase);
    }
}

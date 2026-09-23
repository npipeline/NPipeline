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
    ///     Determines if a statement-level error reports that Snowflake is throttling the client (a throttling message on
    ///     an error the server returned), so it should back off for longer than for other transient errors. HTTP 429 is
    ///     not included: the driver retries it itself (see <see cref="IsRetriedByDriver" />).
    /// </summary>
    /// <param name="exception">The exception to check.</param>
    /// <returns>True if the exception is a statement-level throttling error; otherwise, false.</returns>
    public static bool IsThrottling(Exception exception)
    {
        return exception is DbException dbEx && !IsRetriedByDriver(dbEx) && IsThrottlingMessage(dbEx.Message);
    }

    /// <summary>
    ///     Determines if an exception is a failure the Snowflake.Data driver has already retried, or reports only after
    ///     giving up on its own retries, so retrying the statement again would only repeat that work.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Snowflake.Data retries every HTTP request of a statement itself: transport errors, HTTP timeouts, and
    ///         5xx, 403, 408, and 429 responses, up to <c>MAXHTTPRETRIES</c> times within <c>RETRY_TIMEOUT</c>. When it
    ///         gives up it reports one of these:
    ///     </para>
    ///     <list type="bullet">
    ///         <item>an <see cref="HttpRequestException" /> for the last failed response;</item>
    ///         <item>
    ///             an <see cref="OperationCanceledException" /> that the caller did not request, wrapping a driver
    ///             error (270007, request timeout);
    ///         </item>
    ///         <item>
    ///             a driver-raised error in the 270000-270999 range, such as 270007 (request timeout) or 270058 (I/O
    ///             error on PUT, after the driver's own five upload attempts).
    ///         </item>
    ///     </list>
    ///     <para>
    ///         390111 (session gone) is included too: the driver has already discarded the session, and a retry on the
    ///         same connection would meet the same error.
    ///     </para>
    /// </remarks>
    /// <param name="exception">The exception to check.</param>
    /// <returns>True if the driver has already retried the failure; otherwise, false.</returns>
    public static bool IsRetriedByDriver(Exception exception)
    {
        return exception switch
        {
            HttpRequestException => true,
            OperationCanceledException { InnerException: DbException inner } => IsDriverErrorCode(inner.ErrorCode),
            DbException dbEx => IsDriverErrorCode(dbEx.ErrorCode) || dbEx.ErrorCode == SessionGone,
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

    private const int SessionGone = 390111;

    private static bool IsDriverErrorCode(int errorCode)
    {
        // Snowflake.Data's own client-side errors (SFError) are numbered from 270000.
        return errorCode is >= 270000 and < 271000;
    }

    private static bool IsThrottlingMessage(string message)
    {
        if (string.IsNullOrWhiteSpace(message))
            return false;

        return message.Contains("throttled", StringComparison.OrdinalIgnoreCase)
               || message.Contains("429", StringComparison.OrdinalIgnoreCase)
               || message.Contains("too many requests", StringComparison.OrdinalIgnoreCase);
    }
}

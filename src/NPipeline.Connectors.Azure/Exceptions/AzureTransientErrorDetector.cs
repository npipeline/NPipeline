using System.Net;
using System.Net.Sockets;
using Azure;

namespace NPipeline.Connectors.Azure.Exceptions;

/// <summary>
///     Base implementation for detecting transient Azure errors.
///     Can be extended for service-specific error detection.
/// </summary>
public class AzureTransientErrorDetector : ITransientErrorDetector
{
    private static readonly HashSet<int> TransientStatusCodes =
    [
        (int)HttpStatusCode.RequestTimeout, // 408
        (int)HttpStatusCode.Gone, // 410
        (int)HttpStatusCode.ServiceUnavailable, // 503
        (int)HttpStatusCode.TooManyRequests, // 429
        449, // RetryWith
    ];

    /// <summary>
    ///     Determines if an exception represents a transient error that should be retried.
    /// </summary>
    /// <param name="exception">The exception to check.</param>
    /// <returns>True if the exception is transient; otherwise false.</returns>
    public virtual bool IsTransient(Exception? exception)
    {
        if (exception == null)
            return false;

        // Unwrap aggregate exceptions
        if (exception is AggregateException aggregateException)
            return aggregateException.InnerExceptions.Any(IsTransient);

        // Check inner exception recursively
        if (exception.InnerException != null && IsTransient(exception.InnerException))
            return true;

        return exception switch
        {
            RequestFailedException requestFailedEx => IsTransientStatus(requestFailedEx.Status),
            TimeoutException => true,
            SocketException => true,
            HttpRequestException httpEx => IsTransientHttpError(httpEx),
            TaskCanceledException taskCanceled => !taskCanceled.CancellationToken.IsCancellationRequested,
            OperationCanceledException operationCanceled => !operationCanceled.CancellationToken.IsCancellationRequested,
            _ => false,
        };
    }

    /// <summary>
    ///     Determines if an exception represents a rate-limiting error.
    /// </summary>
    /// <param name="exception">The exception to check.</param>
    /// <returns>True if the exception is a rate-limiting error; otherwise false.</returns>
    public virtual bool IsRateLimited(Exception? exception)
    {
        if (exception is RequestFailedException requestFailedEx)
            return requestFailedEx.Status == (int)HttpStatusCode.TooManyRequests;

        return false;
    }

    /// <summary>
    ///     Gets the suggested retry delay from an exception, if available.
    /// </summary>
    /// <param name="exception">The exception to check.</param>
    /// <returns>The suggested retry delay, or null if not available.</returns>
    public virtual TimeSpan? GetRetryDelay(Exception? exception)
    {
        // Base implementation doesn't extract retry delay - service-specific implementations should override
        return null;
    }

    /// <summary>
    ///     Gets the correlation/activity ID from an exception for tracing.
    /// </summary>
    /// <param name="exception">The exception to check.</param>
    /// <returns>The correlation ID, or null if not available.</returns>
    public virtual string? GetCorrelationId(Exception? exception)
    {
        // Base implementation doesn't extract correlation ID - service-specific implementations should override
        return null;
    }

    /// <summary>
    ///     Determines if an HTTP status code represents a transient error.
    /// </summary>
    /// <param name="statusCode">The HTTP status code.</param>
    /// <returns>True if the status code indicates a transient error.</returns>
    protected static bool IsTransientStatus(int statusCode)
    {
        return TransientStatusCodes.Contains(statusCode);
    }

    /// <summary>
    ///     Determines if an HTTP request exception represents a transient error.
    /// </summary>
    /// <param name="exception">The HTTP request exception.</param>
    /// <returns>True if the exception indicates a transient error.</returns>
    protected virtual bool IsTransientHttpError(HttpRequestException exception)
    {
        // Check inner exception first
        if (exception.InnerException != null)
            return IsTransient(exception.InnerException);

        // Check for known transient HTTP conditions
        return exception.Message.Contains("timeout", StringComparison.OrdinalIgnoreCase) ||
               exception.Message.Contains("timed out", StringComparison.OrdinalIgnoreCase) ||
               exception.Message.Contains("time out", StringComparison.OrdinalIgnoreCase) ||
               exception.Message.Contains("connection", StringComparison.OrdinalIgnoreCase) ||
               exception.Message.Contains("reset", StringComparison.OrdinalIgnoreCase);
    }
}

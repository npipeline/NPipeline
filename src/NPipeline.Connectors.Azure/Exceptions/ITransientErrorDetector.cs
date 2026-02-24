namespace NPipeline.Connectors.Azure.Exceptions;

/// <summary>
///     Detects transient errors that should be retried for Azure services.
/// </summary>
public interface ITransientErrorDetector
{
    /// <summary>
    ///     Determines if an exception represents a transient error that should be retried.
    /// </summary>
    /// <param name="exception">The exception to check.</param>
    /// <returns>True if the exception is transient; otherwise false.</returns>
    bool IsTransient(Exception? exception);

    /// <summary>
    ///     Determines if an exception represents a rate-limiting error.
    /// </summary>
    /// <param name="exception">The exception to check.</param>
    /// <returns>True if the exception is a rate-limiting error; otherwise false.</returns>
    bool IsRateLimited(Exception? exception);

    /// <summary>
    ///     Gets the suggested retry delay from an exception, if available.
    /// </summary>
    /// <param name="exception">The exception to check.</param>
    /// <returns>The suggested retry delay, or null if not available.</returns>
    TimeSpan? GetRetryDelay(Exception? exception);

    /// <summary>
    ///     Gets the correlation/activity ID from an exception for tracing.
    /// </summary>
    /// <param name="exception">The exception to check.</param>
    /// <returns>The correlation ID, or null if not available.</returns>
    string? GetCorrelationId(Exception? exception);
}

namespace NPipeline.Connectors.Http.Retry;

/// <summary>Decides whether and when to retry a failed HTTP request.</summary>
public interface IHttpRetryStrategy
{
    /// <summary>
    ///     Returns <c>true</c> if the request should be retried given the HTTP response (which may be
    ///     <c>null</c> on network failure) and the current 1-based <paramref name="attempt" /> number.
    /// </summary>
    /// <param name="response">The HTTP response, or <c>null</c> when a network-level exception occurred.</param>
    /// <param name="exception">The exception that was thrown, or <c>null</c> when a response was received.</param>
    /// <param name="attempt">The current (1-based) attempt number.</param>
    bool ShouldRetry(HttpResponseMessage? response, Exception? exception, int attempt);

    /// <summary>Gets the delay to wait before the next attempt.</summary>
    /// <param name="response">The HTTP response, or <c>null</c> when a network-level exception occurred.</param>
    /// <param name="attempt">The current (1-based) attempt number.</param>
    TimeSpan GetDelay(HttpResponseMessage? response, int attempt);
}

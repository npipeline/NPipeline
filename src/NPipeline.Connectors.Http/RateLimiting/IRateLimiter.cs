namespace NPipeline.Connectors.Http.RateLimiting;

/// <summary>Throttles outbound HTTP requests.</summary>
public interface IRateLimiter
{
    /// <summary>
    ///     Waits until a request token is available, then returns.
    ///     Implementations must respect <paramref name="cancellationToken" />.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    ValueTask WaitAsync(CancellationToken cancellationToken = default);
}

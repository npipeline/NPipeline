namespace NPipeline.Connectors.Http.Auth;

/// <summary>Attaches authentication to an outbound HTTP request.</summary>
public interface IHttpAuthProvider
{
    /// <summary>
    ///     Applies credentials to <paramref name="request" /> before it is sent.
    ///     Implementations should only modify headers or query-string, not the body.
    /// </summary>
    /// <param name="request">The HTTP request to authenticate.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    ValueTask ApplyAsync(HttpRequestMessage request, CancellationToken cancellationToken = default);
}

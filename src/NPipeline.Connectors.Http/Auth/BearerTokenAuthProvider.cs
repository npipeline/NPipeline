using System.Net.Http.Headers;

namespace NPipeline.Connectors.Http.Auth;

/// <summary>
///     Applies a Bearer token to each request via the <c>Authorization</c> header.
///     Accepts a factory delegate so that tokens can be refreshed at call time (e.g. OAuth 2.0 access tokens).
///     Token caching is the caller's responsibility.
/// </summary>
public sealed class BearerTokenAuthProvider : IHttpAuthProvider
{
    private readonly Func<CancellationToken, ValueTask<string>> _tokenFactory;

    /// <summary>
    ///     Creates a new instance with a static token value.
    /// </summary>
    /// <param name="token">The Bearer token to use for all requests.</param>
    public BearerTokenAuthProvider(string token)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(token);
        _tokenFactory = _ => ValueTask.FromResult(token);
    }

    /// <summary>
    ///     Creates a new instance with an async token factory invoked per request.
    /// </summary>
    /// <param name="tokenFactory">
    ///     A factory that returns the current token.  May be called once per page for paginated sources.
    /// </param>
    public BearerTokenAuthProvider(Func<CancellationToken, ValueTask<string>> tokenFactory)
    {
        _tokenFactory = tokenFactory ?? throw new ArgumentNullException(nameof(tokenFactory));
    }

    /// <inheritdoc />
    public async ValueTask ApplyAsync(HttpRequestMessage request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var token = await _tokenFactory(cancellationToken).ConfigureAwait(false);

        if (string.IsNullOrWhiteSpace(token))
        {
            throw new InvalidOperationException(
                "BearerTokenAuthProvider: token factory returned a null or empty token.");
        }

        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
    }
}

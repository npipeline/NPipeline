using System.Web;

namespace NPipeline.Connectors.Http.Auth;

/// <summary>
///     Applies an API key to each request, either as a request header or as a query-string parameter.
/// </summary>
public sealed class ApiKeyAuthProvider : IHttpAuthProvider
{
    private readonly string _key;
    private readonly ApiKeyLocation _location;
    private readonly string _parameterName;

    /// <summary>
    ///     Creates a new instance that sends the key as a request header.
    /// </summary>
    /// <param name="headerName">The header name to use. Defaults to <c>X-Api-Key</c>.</param>
    /// <param name="key">The API key value.</param>
    public ApiKeyAuthProvider(string headerName, string key)
        : this(headerName, key, ApiKeyLocation.Header)
    {
    }

    /// <summary>
    ///     Creates a new instance with configurable placement.
    /// </summary>
    /// <param name="parameterName">Header name or query-string parameter name.</param>
    /// <param name="key">The API key value.</param>
    /// <param name="location">Whether to place the key in a header or a query-string parameter.</param>
    public ApiKeyAuthProvider(string parameterName, string key, ApiKeyLocation location)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(parameterName);
        ArgumentException.ThrowIfNullOrWhiteSpace(key);

        _parameterName = parameterName;
        _key = key;
        _location = location;
    }

    /// <inheritdoc />
    public ValueTask ApplyAsync(HttpRequestMessage request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (_location == ApiKeyLocation.Header)
            request.Headers.TryAddWithoutValidation(_parameterName, _key);
        else
        {
            var uri = request.RequestUri
                      ?? throw new InvalidOperationException("ApiKeyAuthProvider: request has no URI.");

            var uriBuilder = new UriBuilder(uri);
            var query = HttpUtility.ParseQueryString(uriBuilder.Query);
            query[_parameterName] = _key;
            uriBuilder.Query = query.ToString();
            request.RequestUri = uriBuilder.Uri;
        }

        return ValueTask.CompletedTask;
    }
}

/// <summary>Specifies where an API key is placed on outbound requests.</summary>
public enum ApiKeyLocation
{
    /// <summary>The key is sent as a request header.</summary>
    Header,

    /// <summary>The key is appended as a query-string parameter.</summary>
    QueryString,
}

using System.Text.Json;
using NPipeline.Connectors.Http.Auth;
using NPipeline.Connectors.Http.Pagination;
using NPipeline.Connectors.Http.RateLimiting;
using NPipeline.Connectors.Http.Retry;

namespace NPipeline.Connectors.Http.Configuration;

/// <summary>Configuration for an <see cref="NPipeline.Connectors.Http.Nodes.HttpSourceNode{T}" />.</summary>
public sealed class HttpSourceConfiguration
{
    /// <summary>Base URI of the API endpoint (without pagination query parameters).</summary>
    public required Uri BaseUri { get; init; }

    /// <summary>
    ///     HTTP method used for source requests.  Defaults to <see cref="HttpMethod.Get" />.
    ///     Use <see cref="HttpMethod.Post" /> for APIs that accept a request body to describe the query.
    /// </summary>
    public HttpMethod RequestMethod { get; init; } = HttpMethod.Get;

    /// <summary>Additional fixed headers sent with every request.</summary>
    public Dictionary<string, string> Headers { get; init; } = [];

    /// <summary>
    ///     Factory for the request body.  Only used when <see cref="RequestMethod" /> is POST/PUT/PATCH.
    ///     Receives the current page URI so pagination tokens can be embedded in the body.
    ///     Return <c>null</c> to send no body.
    /// </summary>
    public Func<Uri, HttpContent?>? RequestBodyFactory { get; init; }

    /// <summary>
    ///     Named <see cref="HttpClient" /> to resolve from <see cref="IHttpClientFactory" />.
    ///     Leave <c>null</c> to use the unnamed default client.
    /// </summary>
    public string? HttpClientName { get; init; }

    /// <summary>Per-request timeout. Defaults to 30 seconds.</summary>
    public TimeSpan Timeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    ///     Dot-separated JSON property path to the array of items within each response.
    ///     Leave <c>null</c> when the root of the response body is the items array.
    ///     Example: <c>"data"</c> extracts from <c>{ "data": [...] }</c>.
    /// </summary>
    public string? ItemsJsonPath { get; init; }

    /// <summary>
    ///     <see cref="JsonSerializerOptions" /> used for response deserialisation.
    ///     Defaults to <see cref="JsonSerializerDefaults.Web" /> (camelCase, case-insensitive).
    /// </summary>
    public JsonSerializerOptions? JsonOptions { get; init; }

    /// <summary>Authentication provider. Defaults to <see cref="NullAuthProvider" />.</summary>
    public IHttpAuthProvider Auth { get; init; } = NullAuthProvider.Instance;

    /// <summary>Pagination strategy. Defaults to <see cref="NoPaginationStrategy" />.</summary>
    public IPaginationStrategy Pagination { get; init; } = new NoPaginationStrategy();

    /// <summary>Rate limiter. Defaults to <see cref="NullRateLimiter" />.</summary>
    public IRateLimiter RateLimiter { get; init; } = NullRateLimiter.Instance;

    /// <summary>Retry strategy. Defaults to <see cref="ExponentialBackoffHttpRetryStrategy.Default" />.</summary>
    public IHttpRetryStrategy RetryStrategy { get; init; } = ExponentialBackoffHttpRetryStrategy.Default;

    /// <summary>
    ///     Optional mutator invoked immediately before each send, including any retry attempts.
    ///     Useful for correlation IDs, tenant headers, or dynamic query parameters.
    /// </summary>
    public Func<HttpRequestMessage, CancellationToken, ValueTask>? RequestCustomizer { get; init; }

    /// <summary>
    ///     Maximum number of pages to fetch per run.  Acts as a safety guard against infinite pagination loops.
    ///     Leave <c>null</c> for no limit.
    /// </summary>
    public int? MaxPages { get; init; }

    /// <summary>
    ///     Optional upper bound for response payload size in bytes.
    ///     When the <c>Content-Length</c> header exceeds this value the source fails fast.
    ///     Leave <c>null</c> for no limit.
    /// </summary>
    public long? MaxResponseBytes { get; init; }

    /// <summary>Validates the configuration and throws a descriptive exception if it is invalid.</summary>
    internal void Validate()
    {
        if (!BaseUri.IsAbsoluteUri)
            throw new ArgumentException("HttpSourceConfiguration.BaseUri must be an absolute URI.", nameof(BaseUri));

        if (Timeout <= TimeSpan.Zero)
            throw new ArgumentException("HttpSourceConfiguration.Timeout must be a positive duration.", nameof(Timeout));

        if (MaxPages.HasValue && MaxPages.Value <= 0)
            throw new ArgumentException("HttpSourceConfiguration.MaxPages must be greater than zero when specified.", nameof(MaxPages));

        if (MaxResponseBytes.HasValue && MaxResponseBytes.Value <= 0)
            throw new ArgumentException("HttpSourceConfiguration.MaxResponseBytes must be greater than zero when specified.", nameof(MaxResponseBytes));
    }
}

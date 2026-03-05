using System.Text.Json;
using NPipeline.Connectors.Http.Auth;
using NPipeline.Connectors.Http.RateLimiting;
using NPipeline.Connectors.Http.Retry;

namespace NPipeline.Connectors.Http.Configuration;

/// <summary>Configuration for an <see cref="NPipeline.Connectors.Http.Nodes.HttpSinkNode{T}" />.</summary>
public sealed class HttpSinkConfiguration
{
    /// <summary>
    ///     Static target URI.  Ignored when <see cref="UriFactory" /> is set.
    ///     At least one of <see cref="Uri" /> or <see cref="UriFactory" /> must be supplied.
    /// </summary>
    public Uri? Uri { get; init; }

    /// <summary>
    ///     Per-item URI factory.  Takes precedence over <see cref="Uri" /> when non-null.
    ///     Use this when the endpoint varies per item (e.g. <c>PUT /items/{id}</c>).
    /// </summary>
    public Func<object, Uri>? UriFactory { get; init; }

    /// <summary>HTTP method to use when writing. Defaults to <see cref="SinkHttpMethod.Post" />.</summary>
    public SinkHttpMethod Method { get; init; } = SinkHttpMethod.Post;

    /// <summary>Additional fixed headers sent with every request.</summary>
    public Dictionary<string, string> Headers { get; init; } = [];

    /// <summary>
    ///     Named <see cref="HttpClient" /> to resolve from <see cref="IHttpClientFactory" />.
    ///     Leave <c>null</c> to use the unnamed default client.
    /// </summary>
    public string? HttpClientName { get; init; }

    /// <summary>Per-request timeout. Defaults to 30 seconds.</summary>
    public TimeSpan Timeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    ///     Maximum number of items to buffer before flushing as a batch.
    ///     Use <c>1</c> (default) for item-at-a-time writes.
    ///     Use a larger value for APIs that accept bulk payloads.
    /// </summary>
    public int BatchSize { get; init; } = 1;

    /// <summary>
    ///     When <see cref="BatchSize" /> is greater than 1, the JSON key used to wrap the batch array.
    ///     For example <c>"items"</c> produces <c>{"items":[...]}</c>.
    ///     Leave <c>null</c> to send a bare JSON array.
    /// </summary>
    public string? BatchWrapperKey { get; init; }

    /// <summary>
    ///     <see cref="JsonSerializerOptions" /> for request serialisation.
    ///     Defaults to <see cref="JsonSerializerDefaults.Web" />.
    /// </summary>
    public JsonSerializerOptions? JsonOptions { get; init; }

    /// <summary>
    ///     When <c>true</c>, non-2xx responses are captured in the result rather than throwing.
    ///     Defaults to <c>false</c>.
    /// </summary>
    public bool CaptureErrorResponses { get; init; }

    /// <summary>Authentication provider. Defaults to <see cref="NullAuthProvider" />.</summary>
    public IHttpAuthProvider Auth { get; init; } = NullAuthProvider.Instance;

    /// <summary>Rate limiter. Defaults to <see cref="NullRateLimiter" />.</summary>
    public IRateLimiter RateLimiter { get; init; } = NullRateLimiter.Instance;

    /// <summary>Retry strategy. Defaults to <see cref="ExponentialBackoffHttpRetryStrategy.Default" />.</summary>
    public IHttpRetryStrategy RetryStrategy { get; init; } = ExponentialBackoffHttpRetryStrategy.Default;

    /// <summary>
    ///     Optional mutator invoked immediately before each send.
    ///     Useful for idempotency keys, correlation IDs, and API-specific headers.
    /// </summary>
    public Func<HttpRequestMessage, CancellationToken, ValueTask>? RequestCustomizer { get; init; }

    /// <summary>
    ///     Optional factory that produces an idempotency key for each outbound request.
    ///     The key is sent in the header specified by <see cref="IdempotencyHeaderName" />.
    ///     Retries with the same key will be safe against duplicate side effects on supporting servers.
    /// </summary>
    public Func<object, string>? IdempotencyKeyFactory { get; init; }

    /// <summary>
    ///     Header name used with <see cref="IdempotencyKeyFactory" />.  Defaults to <c>Idempotency-Key</c>.
    /// </summary>
    public string IdempotencyHeaderName { get; init; } = "Idempotency-Key";

    /// <summary>Validates the configuration and throws a descriptive exception if it is invalid.</summary>
    internal void Validate()
    {
        if (Uri == null && UriFactory == null)
        {
            throw new ArgumentException(
                "HttpSinkConfiguration requires either Uri or UriFactory to be set.", nameof(Uri));
        }

        if (BatchSize < 1)
            throw new ArgumentException("HttpSinkConfiguration.BatchSize must be at least 1.", nameof(BatchSize));

        if (Timeout <= TimeSpan.Zero)
            throw new ArgumentException("HttpSinkConfiguration.Timeout must be a positive duration.", nameof(Timeout));

        if (IdempotencyKeyFactory != null &&
            string.IsNullOrWhiteSpace(IdempotencyHeaderName))
        {
            throw new ArgumentException(
                "HttpSinkConfiguration.IdempotencyHeaderName must not be empty when IdempotencyKeyFactory is set.",
                nameof(IdempotencyHeaderName));
        }
    }
}

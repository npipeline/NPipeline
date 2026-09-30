using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using System.Threading.RateLimiting;
using NPipeline.Connectors.Http.Auth;
using NPipeline.Connectors.Http.Reliability;
using NResilience;

namespace NPipeline.Connectors.Http.Configuration;

/// <summary>What an <see cref="Nodes.HttpSinkNode{T}" /> does with a request that still fails once retries are spent.</summary>
public enum HttpFailedRequestAction
{
    /// <summary>Fail the write with an <see cref="HttpRequestException" />.</summary>
    Fail,

    /// <summary>Log a warning and carry on with the next request. The failed items are lost.</summary>
    Skip,

    /// <summary>
    ///     Send an <see cref="Models.HttpRequestFailure{T}" /> holding the items to the pipeline's dead-letter sink, and carry
    ///     on. The write fails when the pipeline has no dead-letter sink.
    /// </summary>
    DeadLetter,
}

/// <summary>Options for an <see cref="Nodes.HttpSinkNode{T}" />. Create them with <see cref="HttpConnector" /> or directly.</summary>
/// <typeparam name="T">The item type.</typeparam>
public sealed record HttpSinkOptions<T>
{
    /// <summary>The target URI, when every item goes to the same endpoint. Ignored when <see cref="UriFactory" /> is set.</summary>
    public Uri? Uri { get; init; }

    /// <summary>
    ///     The target URI for each item, such as <c>PUT /items/{id}</c>. A batch holds consecutive items for one URI: a change
    ///     of URI sends the batch early, so no item is sent to another item's endpoint.
    /// </summary>
    public Func<T, Uri>? UriFactory { get; init; }

    /// <summary>The request method. Defaults to <see cref="SinkHttpMethod.Post" />.</summary>
    public SinkHttpMethod Method { get; init; } = SinkHttpMethod.Post;

    /// <summary>Headers sent with every request.</summary>
    public IReadOnlyDictionary<string, string> Headers { get; init; } = new Dictionary<string, string>();

    /// <summary>The named client to create from an <see cref="IHttpClientFactory" />; <c>null</c> uses the default client.</summary>
    public string? HttpClientName { get; init; }

    /// <summary>
    ///     The most items per request. Defaults to 1, which sends each item as a JSON object; larger batches are sent as a
    ///     JSON array, or wrapped as <see cref="BatchWrapperKey" /> says.
    /// </summary>
    public int BatchSize { get; init; } = 1;

    /// <summary>
    ///     The longest a batch waits to fill: a partial batch is sent once this long has passed since its first item.
    ///     Defaults to one second; <see cref="Timeout.InfiniteTimeSpan" /> waits for full batches.
    /// </summary>
    public TimeSpan BatchLinger { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>The property to wrap a batch in, such as <c>items</c> for <c>{"items":[…]}</c>. <c>null</c> sends a bare array.</summary>
    public string? BatchWrapperKey { get; init; }

    /// <summary>Serializer options for items. When <c>null</c>, System.Text.Json's web defaults (camelCase).</summary>
    public JsonSerializerOptions? JsonOptions { get; init; }

    /// <summary>Metadata to serialize items with, such as one from a source-generated <c>JsonSerializerContext</c>. Takes precedence over <see cref="JsonOptions" />.</summary>
    public JsonTypeInfo<T>? TypeInfo { get; init; }

    /// <summary>What to do with a request that still fails once retries are spent. Defaults to <see cref="HttpFailedRequestAction.Fail" />.</summary>
    public HttpFailedRequestAction FailedRequests { get; init; } = HttpFailedRequestAction.Fail;

    /// <summary>Authentication. Defaults to none.</summary>
    public IHttpAuthProvider Auth { get; init; } = NullAuthProvider.Instance;

    /// <summary>Throttles requests. A lease is acquired for every attempt, including retries. Defaults to none.</summary>
    public RateLimiter? RateLimiter { get; init; }

    /// <summary>
    ///     How each request is retried and timed out. Defaults to <see cref="HttpConnectorResilience.Default" />. POST and
    ///     PATCH are retried only with an <see cref="IdempotencyKeyFactory" />.
    /// </summary>
    public Resilience Resilience { get; init; } = HttpConnectorResilience.Default;

    /// <summary>Changes each request just before it is sent: correlation ids, API-specific headers.</summary>
    public Func<HttpRequestMessage, CancellationToken, ValueTask>? RequestCustomizer { get; init; }

    /// <summary>
    ///     The idempotency key for a request, given its items. With a key, POST and PATCH requests are retried, and servers
    ///     that support the header discard duplicates.
    /// </summary>
    public Func<IReadOnlyList<T>, string>? IdempotencyKeyFactory { get; init; }

    /// <summary>The header that carries the idempotency key. Defaults to <c>Idempotency-Key</c>.</summary>
    public string IdempotencyHeaderName { get; init; } = "Idempotency-Key";

    /// <summary>Checks the options. Nodes call it from their constructors.</summary>
    /// <exception cref="ArgumentException">An option is invalid.</exception>
    public void Validate()
    {
        if (Uri is null && UriFactory is null)
            throw new ArgumentException("Set Uri, or UriFactory for per-item endpoints.", nameof(Uri));

        if (Uri is { IsAbsoluteUri: false })
            throw new ArgumentException("Uri must be an absolute URI.", nameof(Uri));

        ArgumentOutOfRangeException.ThrowIfLessThan(BatchSize, 1, nameof(BatchSize));

        if (BatchLinger < TimeSpan.Zero && BatchLinger != Timeout.InfiniteTimeSpan)
            throw new ArgumentOutOfRangeException(nameof(BatchLinger), "BatchLinger must be zero or more, or Timeout.InfiniteTimeSpan.");
        ArgumentNullException.ThrowIfNull(Headers, nameof(Headers));
        ArgumentNullException.ThrowIfNull(Auth, nameof(Auth));
        ArgumentNullException.ThrowIfNull(Resilience, nameof(Resilience));
        Resilience.Validate();

        if (IdempotencyKeyFactory is not null)
            ArgumentException.ThrowIfNullOrWhiteSpace(IdempotencyHeaderName, nameof(IdempotencyHeaderName));
    }
}

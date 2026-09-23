using System.Diagnostics;
using System.Net.Http.Headers;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Metrics;
using NPipeline.Connectors.Http.Reliability;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NResilience;

namespace NPipeline.Connectors.Http.Nodes;

/// <summary>
///     A sink node that writes items to a REST API via POST, PUT, or PATCH.
///     Supports batching, auth, retry, rate limiting, idempotency keys and observability.
/// </summary>
/// <typeparam name="T">The item type to serialise and send.</typeparam>
public sealed partial class HttpSinkNode<T> : SinkNode<T>, IAsyncDisposable
{
    private readonly HttpSinkConfiguration _configuration;
    private readonly HttpClient _httpClient;
    private readonly HttpMethod _httpMethod;
    private readonly ILogger<HttpSinkNode<T>> _logger;
    private readonly IHttpConnectorMetrics _metrics;
    private readonly bool _ownsClient;
    private readonly ResilientHttpSender _sender;

    // The node sends one request at a time, so the retry listener reads the request in flight from here.
    private Uri? _currentUri;

    /// <summary>Creates a new instance sourcing an <see cref="HttpClient" /> from the provided factory.</summary>
    public HttpSinkNode(HttpSinkConfiguration configuration, IHttpClientFactory httpClientFactory)
        : this(configuration, httpClientFactory, NullHttpConnectorMetrics.Instance)
    {
    }

    /// <summary>Creates a new instance with full dependency injection.</summary>
    public HttpSinkNode(
        HttpSinkConfiguration configuration,
        IHttpClientFactory httpClientFactory,
        IHttpConnectorMetrics? metrics = null,
        ILogger<HttpSinkNode<T>>? logger = null)
        : this(
            configuration,
            CreateClient(configuration, httpClientFactory),
            metrics ?? NullHttpConnectorMetrics.Instance,
            logger,
            true)
    {
    }

    /// <summary>
    ///     Creates a new instance with a strongly-typed URI factory for per-item routing.
    /// </summary>
    public HttpSinkNode(
        HttpSinkConfiguration configuration,
        Func<T, Uri> uriFactory,
        IHttpClientFactory httpClientFactory,
        IHttpConnectorMetrics? metrics = null,
        ILogger<HttpSinkNode<T>>? logger = null)
        : this(
            CloneWithTypedUriFactory(configuration, uriFactory),
            CreateClient(configuration, httpClientFactory),
            metrics ?? NullHttpConnectorMetrics.Instance,
            logger,
            true)
    {
        ArgumentNullException.ThrowIfNull(uriFactory);
    }

    /// <summary>
    ///     Creates a new instance with a raw <see cref="HttpClient" />.
    ///     Useful in tests and minimal-host scenarios that do not use <see cref="IHttpClientFactory" />.
    /// </summary>
    public HttpSinkNode(
        HttpSinkConfiguration configuration,
        HttpClient httpClient,
        IHttpConnectorMetrics? metrics = null,
        ILogger<HttpSinkNode<T>>? logger = null)
        : this(configuration, httpClient, metrics ?? NullHttpConnectorMetrics.Instance, logger, false)
    {
    }

    private HttpSinkNode(
        HttpSinkConfiguration configuration,
        HttpClient httpClient,
        IHttpConnectorMetrics metrics,
        ILogger<HttpSinkNode<T>>? logger,
        bool ownsClient)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.Validate();

        _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        _metrics = metrics;
        _logger = logger ?? NullLogger<HttpSinkNode<T>>.Instance;
        _ownsClient = ownsClient;

        _httpMethod = _configuration.Method switch
        {
            SinkHttpMethod.Post => HttpMethod.Post,
            SinkHttpMethod.Put => HttpMethod.Put,
            SinkHttpMethod.Patch => HttpMethod.Patch,
            _ => HttpMethod.Post,
        };

        _sender = new ResilientHttpSender(_httpClient, _configuration.Resilience, false, _metrics, OnResilienceEvent);
    }

    /// <inheritdoc />
    public override async Task ConsumeAsync(
        IDataStream<T> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var batch = new List<T>(_configuration.BatchSize);

        await foreach (var item in input.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            batch.Add(item);

            if (batch.Count >= _configuration.BatchSize)
            {
                await FlushBatchAsync(batch, cancellationToken).ConfigureAwait(false);
                batch.Clear();
            }
        }

        if (batch.Count > 0)
            await FlushBatchAsync(batch, cancellationToken).ConfigureAwait(false);
    }

    private async Task FlushBatchAsync(List<T> items, CancellationToken cancellationToken)
    {
        var firstItem = items[0]!;
        var uri = ResolveUri(firstItem);

        var waitStart = Stopwatch.GetTimestamp();
        await _configuration.RateLimiter.WaitAsync(cancellationToken).ConfigureAwait(false);

        _metrics.RecordRateLimitWait(
            uri.ToString(),
            Stopwatch.GetElapsedTime(waitStart));

        try
        {
            await SendAsync(uri, items, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _metrics.RecordError(uri.ToString(), _httpMethod.Method, ex);
            throw;
        }
    }

    private async Task SendAsync(Uri uri, List<T> items, CancellationToken cancellationToken)
    {
        var jsonOptions = _configuration.JsonOptions
                          ?? new JsonSerializerOptions(JsonSerializerDefaults.Web);

        using var content = BuildContent(items, jsonOptions);
        using var request = new HttpRequestMessage(_httpMethod, uri) { Content = content };

        foreach (var (key, value) in _configuration.Headers)
        {
            request.Headers.TryAddWithoutValidation(key, value);
        }

        // An idempotency key makes a POST or PATCH safe to retry. Without one, the resilience handler sends those
        // methods once, because a retried write the server already applied is a duplicate.
        if (_configuration.IdempotencyKeyFactory != null)
            _ = request.MarkRepeatable(_configuration.IdempotencyKeyFactory(items[0]!), _configuration.IdempotencyHeaderName);

        await _configuration.Auth.ApplyAsync(request, cancellationToken).ConfigureAwait(false);

        if (_configuration.RequestCustomizer != null)
            await _configuration.RequestCustomizer(request, cancellationToken).ConfigureAwait(false);

        LogSendingRequest(_logger, typeof(T).Name, _httpMethod.Method, uri, items.Count);
        _currentUri = uri;

        using var response = await _sender.SendAsync(request, cancellationToken).ConfigureAwait(false);

        if (response.IsSuccessStatusCode)
        {
            _metrics.RecordSinkWritten(uri.ToString(), _httpMethod.Method, (int)response.StatusCode);
            return;
        }

        // Judged only after the resilience handler has spent its retries, so a transient 503 is retried rather than
        // captured on its first appearance.
        if (_configuration.CaptureErrorResponses)
        {
            LogCapturedError(_logger, typeof(T).Name, (int)response.StatusCode, uri);
            return;
        }

        var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);

        throw new HttpRequestException(
            $"HttpSinkNode<{typeof(T).Name}>: request to {uri} failed with " +
            $"{(int)response.StatusCode} {response.ReasonPhrase}. Body: {Truncate(body, 512)}",
            null,
            response.StatusCode);
    }

    private void OnResilienceEvent(CallEvent callEvent)
    {
        if (callEvent.Kind != CallEventKind.Retrying || _currentUri is not { } uri)
            return;

        _metrics.RecordRetry(uri.ToString(), _httpMethod.Method, callEvent.AttemptNumber);
        LogRetrying(_logger, typeof(T).Name, callEvent.AttemptNumber, uri);
    }

    private Uri ResolveUri(T item)
    {
        if (_configuration.UriFactory != null)
            return _configuration.UriFactory(item!);

        return _configuration.Uri!;
    }

    private HttpContent BuildContent(List<T> items, JsonSerializerOptions options)
    {
        var stream = new MemoryStream();

        if (items.Count == 1 && _configuration.BatchSize == 1)
            JsonSerializer.Serialize(stream, items[0], options);
        else if (_configuration.BatchWrapperKey != null)
        {
            using var writer = new Utf8JsonWriter(stream);
            writer.WriteStartObject();
            writer.WritePropertyName(_configuration.BatchWrapperKey);
            JsonSerializer.Serialize(writer, items, options);
            writer.WriteEndObject();
        }
        else
            JsonSerializer.Serialize(stream, items, options);

        stream.Position = 0;
        var content = new StreamContent(stream);

        content.Headers.ContentType = new MediaTypeHeaderValue("application/json")
        {
            CharSet = "utf-8",
        };

        return content;
    }

    private static string Truncate(string value, int maxLength)
    {
        return value.Length <= maxLength
            ? value
            : string.Concat(value.AsSpan(0, maxLength), "…");
    }

    private static HttpClient CreateClient(HttpSinkConfiguration configuration, IHttpClientFactory httpClientFactory)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(httpClientFactory);

        return configuration.HttpClientName != null
            ? httpClientFactory.CreateClient(configuration.HttpClientName)
            : httpClientFactory.CreateClient();
    }

    private static HttpSinkConfiguration CloneWithTypedUriFactory(
        HttpSinkConfiguration configuration,
        Func<T, Uri> uriFactory)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(uriFactory);

        return new HttpSinkConfiguration
        {
            Uri = configuration.Uri,
            UriFactory = item => uriFactory((T)item),
            Method = configuration.Method,
            Headers = new Dictionary<string, string>(configuration.Headers, StringComparer.Ordinal),
            HttpClientName = configuration.HttpClientName,
            BatchSize = configuration.BatchSize,
            BatchWrapperKey = configuration.BatchWrapperKey,
            JsonOptions = configuration.JsonOptions,
            CaptureErrorResponses = configuration.CaptureErrorResponses,
            Auth = configuration.Auth,
            RateLimiter = configuration.RateLimiter,
            Resilience = configuration.Resilience,
            RequestCustomizer = configuration.RequestCustomizer,
            IdempotencyKeyFactory = configuration.IdempotencyKeyFactory,
            IdempotencyHeaderName = configuration.IdempotencyHeaderName,
        };
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _sender.Dispose();

        if (_ownsClient)
            _httpClient.Dispose();

        return ValueTask.CompletedTask;
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "HttpSinkNode<{TypeName}>: sending {Method} {Uri} with {Count} item(s).")]
    private static partial void LogSendingRequest(ILogger logger, string typeName, string method, Uri uri, int count);

    [LoggerMessage(Level = LogLevel.Warning, Message = "HttpSinkNode<{TypeName}>: received {Status} from {Uri}; CaptureErrorResponses is enabled.")]
    private static partial void LogCapturedError(ILogger logger, string typeName, int status, Uri uri);

    [LoggerMessage(Level = LogLevel.Warning, Message = "HttpSinkNode<{TypeName}>: attempt {Attempt} failed for {Uri}, retrying.")]
    private static partial void LogRetrying(ILogger logger, string typeName, int attempt, Uri uri);
}

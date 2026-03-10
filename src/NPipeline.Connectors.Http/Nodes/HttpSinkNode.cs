using System.Diagnostics;
using System.Net.Http.Headers;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Metrics;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Http.Nodes;

/// <summary>
///     A sink node that writes items to a REST API via POST, PUT, or PATCH.
///     Supports batching, auth, retry, rate limiting, idempotency keys and observability.
/// </summary>
/// <typeparam name="T">The item type to serialise and send.</typeparam>
public sealed partial class HttpSinkNode<T> : SinkNode<T>
{
    private static readonly ActivitySource ActivitySource = new("NPipeline.Connectors.Http");

    private readonly HttpSinkConfiguration _configuration;
    private readonly HttpClient _httpClient;
    private readonly HttpMethod _httpMethod;
    private readonly ILogger<HttpSinkNode<T>> _logger;
    private readonly IHttpConnectorMetrics _metrics;
    private readonly bool _ownsClient;

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
            await SendWithRetryAsync(uri, items, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _metrics.RecordError(uri.ToString(), _httpMethod.Method, ex);
            throw;
        }
    }

    private async Task SendWithRetryAsync(Uri uri, List<T> items, CancellationToken cancellationToken)
    {
        var attempt = 0;

        var jsonOptions = _configuration.JsonOptions
                          ?? new JsonSerializerOptions(JsonSerializerDefaults.Web);

        string? idempotencyKey = null;

        if (_configuration.IdempotencyKeyFactory != null)
            idempotencyKey = _configuration.IdempotencyKeyFactory(items[0]!);

        while (true)
        {
            attempt++;
            HttpResponseMessage? response = null;
            Exception? lastException = null;

            using var activity = ActivitySource.StartActivity(
                $"HTTP {_httpMethod.Method} {uri.GetLeftPart(UriPartial.Path)}");

            activity?.SetTag("http.method", _httpMethod.Method);
            activity?.SetTag("http.url", uri.ToString());
            activity?.SetTag("http.attempt", attempt);

            var sw = Stopwatch.StartNew();

            try
            {
                using var content = BuildContent(items, jsonOptions);
                using var request = new HttpRequestMessage(_httpMethod, uri) { Content = content };

                foreach (var (key, value) in _configuration.Headers)
                {
                    request.Headers.TryAddWithoutValidation(key, value);
                }

                if (idempotencyKey != null)
                    request.Headers.TryAddWithoutValidation(_configuration.IdempotencyHeaderName, idempotencyKey);

                await _configuration.Auth.ApplyAsync(request, cancellationToken).ConfigureAwait(false);

                if (_configuration.RequestCustomizer != null)
                    await _configuration.RequestCustomizer(request, cancellationToken).ConfigureAwait(false);

                _metrics.RecordRequest(uri.ToString(), _httpMethod.Method);
                LogSendingRequest(_logger, typeof(T).Name, _httpMethod.Method, uri, items.Count, attempt);

                using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                timeoutCts.CancelAfter(_configuration.Timeout);

                response = await _httpClient.SendAsync(request, timeoutCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                lastException = ex;
            }

            sw.Stop();

            if (response != null)
            {
                _metrics.RecordResponse(
                    uri.ToString(),
                    _httpMethod.Method,
                    (int)response.StatusCode,
                    sw.Elapsed);

                activity?.SetTag("http.status_code", (int)response.StatusCode);

                if (response.IsSuccessStatusCode)
                {
                    _metrics.RecordSinkWritten(uri.ToString(), _httpMethod.Method, (int)response.StatusCode);
                    response.Dispose();
                    return;
                }

                if (_configuration.CaptureErrorResponses)
                {
                    LogCapturedError(_logger, typeof(T).Name, (int)response.StatusCode, uri);
                    response.Dispose();
                    return;
                }

                if (!_configuration.RetryStrategy.ShouldRetry(response, null, attempt))
                {
                    var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
                    response.Dispose();

                    throw new HttpRequestException(
                        $"HttpSinkNode<{typeof(T).Name}>: request to {uri} failed with " +
                        $"{(int)response.StatusCode} {response.ReasonPhrase}. Body: {Truncate(body, 512)}");
                }
            }
            else
            {
                _metrics.RecordError(uri.ToString(), _httpMethod.Method, lastException!);

                if (!_configuration.RetryStrategy.ShouldRetry(null, lastException, attempt))
                    throw lastException!;
            }

            _metrics.RecordRetry(uri.ToString(), _httpMethod.Method, attempt);
            LogRetrying(_logger, typeof(T).Name, attempt, uri);

            var delay = _configuration.RetryStrategy.GetDelay(response, attempt);
            response?.Dispose();
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
        }
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
            Timeout = configuration.Timeout,
            BatchSize = configuration.BatchSize,
            BatchWrapperKey = configuration.BatchWrapperKey,
            JsonOptions = configuration.JsonOptions,
            CaptureErrorResponses = configuration.CaptureErrorResponses,
            Auth = configuration.Auth,
            RateLimiter = configuration.RateLimiter,
            RetryStrategy = configuration.RetryStrategy,
            RequestCustomizer = configuration.RequestCustomizer,
            IdempotencyKeyFactory = configuration.IdempotencyKeyFactory,
            IdempotencyHeaderName = configuration.IdempotencyHeaderName,
        };
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        if (_ownsClient)
            _httpClient.Dispose();

        await base.DisposeAsync().ConfigureAwait(false);
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "HttpSinkNode<{TypeName}>: sending {Method} {Uri} with {Count} item(s) (attempt {Attempt}).")]
    private static partial void LogSendingRequest(ILogger logger, string typeName, string method, Uri uri, int count, int attempt);

    [LoggerMessage(Level = LogLevel.Warning, Message = "HttpSinkNode<{TypeName}>: received {Status} from {Uri}; CaptureErrorResponses is enabled.")]
    private static partial void LogCapturedError(ILogger logger, string typeName, int status, Uri uri);

    [LoggerMessage(Level = LogLevel.Warning, Message = "HttpSinkNode<{TypeName}>: attempt {Attempt} failed for {Uri}, retrying.")]
    private static partial void LogRetrying(ILogger logger, string typeName, int attempt, Uri uri);
}

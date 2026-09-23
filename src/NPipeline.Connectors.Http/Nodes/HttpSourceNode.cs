using System.Diagnostics;
using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Metrics;
using NPipeline.Connectors.Http.Reliability;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NResilience;

namespace NPipeline.Connectors.Http.Nodes;

/// <summary>
///     A source node that fetches items from a REST API, following pagination until all pages are exhausted.
///     Supports auth, retry, rate limiting and observability via pluggable abstractions.
/// </summary>
/// <typeparam name="T">The item type to deserialise from the API response.</typeparam>
public sealed partial class HttpSourceNode<T> : SourceNode<T>, IAsyncDisposable
{
    private readonly HttpSourceConfiguration _configuration;
    private readonly HttpClient _httpClient;
    private readonly ILogger<HttpSourceNode<T>> _logger;
    private readonly IHttpConnectorMetrics _metrics;
    private readonly bool _ownsClient;
    private readonly ResilientHttpSender _sender;

    // The node fetches one page at a time, so the retry listener reads the request in flight from here.
    private Uri? _currentUri;

    /// <summary>Creates a new instance sourcing an <see cref="HttpClient" /> from the provided factory.</summary>
    public HttpSourceNode(HttpSourceConfiguration configuration, IHttpClientFactory httpClientFactory)
        : this(configuration, httpClientFactory, NullHttpConnectorMetrics.Instance)
    {
    }

    /// <summary>Creates a new instance with full dependency injection.</summary>
    public HttpSourceNode(
        HttpSourceConfiguration configuration,
        IHttpClientFactory httpClientFactory,
        IHttpConnectorMetrics metrics,
        ILogger<HttpSourceNode<T>>? logger = null)
        : this(
            configuration,
            CreateClient(configuration, httpClientFactory),
            metrics,
            logger,
            true)
    {
    }

    /// <summary>
    ///     Creates a new instance with a raw <see cref="HttpClient" />.
    ///     Useful in tests and minimal-host scenarios that do not use <see cref="IHttpClientFactory" />.
    /// </summary>
    public HttpSourceNode(
        HttpSourceConfiguration configuration,
        HttpClient httpClient,
        IHttpConnectorMetrics? metrics = null,
        ILogger<HttpSourceNode<T>>? logger = null)
        : this(configuration, httpClient, metrics ?? NullHttpConnectorMetrics.Instance, logger, false)
    {
    }

    private HttpSourceNode(
        HttpSourceConfiguration configuration,
        HttpClient httpClient,
        IHttpConnectorMetrics metrics,
        ILogger<HttpSourceNode<T>>? logger,
        bool ownsClient)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.Validate();

        _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _logger = logger ?? NullLogger<HttpSourceNode<T>>.Instance;
        _ownsClient = ownsClient;

        // The source reads every page whole, so the body is read inside the attempt: a body that breaks off
        // part-way is retried like any other transient failure.
        _sender = new ResilientHttpSender(_httpClient, _configuration.Resilience, true, _metrics, OnResilienceEvent);
    }

    /// <inheritdoc />
    public override IDataStream<T> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        var stream = FetchAllPagesAsync(cancellationToken);
        return new DataStream<T>(stream, $"HttpSourceNode<{typeof(T).Name}>");
    }

    private async IAsyncEnumerable<T> FetchAllPagesAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var uri = _configuration.Pagination.BuildFirstPageUri(_configuration.BaseUri);
        var pageNumber = 0;

        var jsonOptions = _configuration.JsonOptions
                          ?? new JsonSerializerOptions(JsonSerializerDefaults.Web);

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (_configuration.MaxPages.HasValue && pageNumber >= _configuration.MaxPages.Value)
            {
                LogMaxPagesReached(_logger, typeof(T).Name, _configuration.MaxPages.Value);
                yield break;
            }

            var waitStart = Stopwatch.GetTimestamp();
            await _configuration.RateLimiter.WaitAsync(cancellationToken).ConfigureAwait(false);

            _metrics.RecordRateLimitWait(
                uri.ToString(),
                Stopwatch.GetElapsedTime(waitStart));

            HttpResponseMessage response;

            try
            {
                response = await SendAsync(uri, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _metrics.RecordError(uri.ToString(), _configuration.RequestMethod.Method, ex);
                throw;
            }

            using (response)
            {
                // Enforce response size limit
                if (_configuration.MaxResponseBytes.HasValue)
                {
                    var contentLength = response.Content.Headers.ContentLength;

                    if (contentLength.HasValue && contentLength.Value > _configuration.MaxResponseBytes.Value)
                    {
                        throw new InvalidOperationException(
                            $"HttpSourceNode<{typeof(T).Name}>: response from {uri} has Content-Length " +
                            $"{contentLength.Value} bytes which exceeds MaxResponseBytes limit of " +
                            $"{_configuration.MaxResponseBytes.Value}.");
                    }

                    await EnsureResponseBodyWithinLimitAsync(
                        response,
                        _configuration.MaxResponseBytes.Value,
                        cancellationToken).ConfigureAwait(false);
                }

                pageNumber++;

                var items = await DeserializeItemsAsync(response, jsonOptions, cancellationToken)
                    .ConfigureAwait(false);

                _metrics.RecordPageFetched(uri.ToString(), items.Count);
                LogPageFetched(_logger, typeof(T).Name, pageNumber, items.Count, uri);

                foreach (var item in items)
                {
                    yield return item;
                }

                var nextUri = await _configuration.Pagination
                    .GetNextPageUriAsync(uri, response, cancellationToken)
                    .ConfigureAwait(false);

                if (nextUri == null)
                    yield break;

                uri = nextUri;
            }
        }
    }

    private async Task<HttpResponseMessage> SendAsync(Uri uri, CancellationToken cancellationToken)
    {
        using var request = BuildRequest(uri);

        // A source only reads, so its request is safe to repeat even when it is a POST that carries a query.
        _ = request.MarkRepeatable();

        await _configuration.Auth.ApplyAsync(request, cancellationToken).ConfigureAwait(false);

        if (_configuration.RequestCustomizer != null)
            await _configuration.RequestCustomizer(request, cancellationToken).ConfigureAwait(false);

        LogSendingRequest(_logger, typeof(T).Name, _configuration.RequestMethod.Method, uri);
        _currentUri = uri;

        var response = await _sender.SendAsync(request, cancellationToken).ConfigureAwait(false);

        if (response.IsSuccessStatusCode)
        {
            // The resilience handler has already read the body into memory, but as a stream that can be read once.
            // Deserialization and pagination strategies both read it, so make it re-readable. This copies from memory.
#if NET9_0_OR_GREATER
            await response.Content.LoadIntoBufferAsync(cancellationToken).ConfigureAwait(false);
#else
            await response.Content.LoadIntoBufferAsync().ConfigureAwait(false);
#endif
            return response;
        }

        using (response)
        {
            var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);

            throw new HttpRequestException(
                $"HttpSourceNode<{typeof(T).Name}>: request to {uri} failed with " +
                $"{(int)response.StatusCode} {response.ReasonPhrase}. Body: {Truncate(body, 512)}",
                null,
                response.StatusCode);
        }
    }

    private void OnResilienceEvent(CallEvent callEvent)
    {
        if (callEvent.Kind != CallEventKind.Retrying || _currentUri is not { } uri)
            return;

        _metrics.RecordRetry(uri.ToString(), _configuration.RequestMethod.Method, callEvent.AttemptNumber);
        LogRetrying(_logger, typeof(T).Name, callEvent.AttemptNumber, uri);
    }

    private HttpRequestMessage BuildRequest(Uri uri)
    {
        var request = new HttpRequestMessage(_configuration.RequestMethod, uri);

        foreach (var (key, value) in _configuration.Headers)
        {
            request.Headers.TryAddWithoutValidation(key, value);
        }

        if (_configuration.RequestBodyFactory != null)
            request.Content = _configuration.RequestBodyFactory(uri);

        return request;
    }

    private async Task<List<T>> DeserializeItemsAsync(
        HttpResponseMessage response,
        JsonSerializerOptions jsonOptions,
        CancellationToken cancellationToken)
    {
        if (_configuration.ItemsJsonPath == null)
        {
            var items = new List<T>();

            await foreach (var item in response.Content
                               .ReadFromJsonAsAsyncEnumerable<T>(jsonOptions, cancellationToken)
                               .ConfigureAwait(false))
            {
                if (item != null)
                    items.Add(item);
            }

            return items;
        }

        var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        using var doc = JsonDocument.Parse(body);

        var element = doc.RootElement;
        var normalizedPath = NormalizeJsonPath(_configuration.ItemsJsonPath);

        foreach (var segment in normalizedPath.Split('.', StringSplitOptions.RemoveEmptyEntries))
        {
            if (!element.TryGetProperty(segment, out element))
                return [];
        }

        if (element.ValueKind != JsonValueKind.Array)
            return [];

        var result = new List<T>(element.GetArrayLength());

        foreach (var arrayElement in element.EnumerateArray())
        {
            var item = arrayElement.Deserialize<T>(jsonOptions);

            if (item != null)
                result.Add(item);
        }

        return result;
    }

    private static string NormalizeJsonPath(string path)
    {
        var trimmed = path.Trim();

        if (trimmed.StartsWith("$.", StringComparison.Ordinal))
            return trimmed[2..];

        if (trimmed == "$")
            return string.Empty;

        if (trimmed.StartsWith('$'))
            return trimmed[1..];

        return trimmed;
    }

    private static string Truncate(string value, int maxLength)
    {
        return value.Length <= maxLength
            ? value
            : string.Concat(value.AsSpan(0, maxLength), "…");
    }

    private static async Task EnsureResponseBodyWithinLimitAsync(
        HttpResponseMessage response,
        long maxBytes,
        CancellationToken cancellationToken)
    {
        var originalContent = response.Content;
        var bytes = await originalContent.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);

        if (bytes.LongLength > maxBytes)
        {
            throw new InvalidOperationException(
                $"HttpSourceNode response body exceeded MaxResponseBytes limit of {maxBytes} bytes. " +
                $"Actual body size: {bytes.LongLength} bytes.");
        }

        var bufferedContent = new ByteArrayContent(bytes);

        foreach (var header in originalContent.Headers)
        {
            _ = bufferedContent.Headers.TryAddWithoutValidation(header.Key, header.Value);
        }

        response.Content = bufferedContent;
        originalContent.Dispose();
    }

    private static HttpClient CreateClient(HttpSourceConfiguration configuration, IHttpClientFactory httpClientFactory)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(httpClientFactory);

        return configuration.HttpClientName != null
            ? httpClientFactory.CreateClient(configuration.HttpClientName)
            : httpClientFactory.CreateClient();
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _sender.Dispose();

        if (_ownsClient)
            _httpClient.Dispose();

        return ValueTask.CompletedTask;
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "HttpSourceNode<{TypeName}>: reached MaxPages limit of {MaxPages}, stopping.")]
    private static partial void LogMaxPagesReached(ILogger logger, string typeName, int maxPages);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HttpSourceNode<{TypeName}>: page {Page} fetched {Count} items from {Uri}.")]
    private static partial void LogPageFetched(ILogger logger, string typeName, int page, int count, Uri uri);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HttpSourceNode<{TypeName}>: sending {Method} {Uri}.")]
    private static partial void LogSendingRequest(ILogger logger, string typeName, string method, Uri uri);

    [LoggerMessage(Level = LogLevel.Warning, Message = "HttpSourceNode<{TypeName}>: attempt {Attempt} failed for {Uri}, retrying.")]
    private static partial void LogRetrying(ILogger logger, string typeName, int attempt, Uri uri);
}

using System.Buffers;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NPipeline.Connectors.Diagnostics;
using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Metrics;
using NPipeline.Connectors.Http.Models;
using NPipeline.Connectors.Http.Reliability;
using NPipeline.Connectors.Messaging;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NResilience;

namespace NPipeline.Connectors.Http.Nodes;

/// <summary>
///     A sink that writes items to a REST API with POST, PUT or PATCH, one at a time or in batches. Requests are retried as
///     the options' resilience allows; one that still fails is failed, skipped or dead-lettered as
///     <see cref="HttpSinkOptions{T}.FailedRequests" /> says.
/// </summary>
/// <typeparam name="T">The item type.</typeparam>
public sealed partial class HttpSinkNode<T> : SinkNode<T>, IAsyncDisposable, IReportsWrites
{
    private const int ResponseExcerptLength = 512;

    private static readonly MediaTypeHeaderValue JsonContentType = new("application/json") { CharSet = "utf-8" };

    private readonly HttpClient _httpClient;
    private readonly HttpMethod _httpMethod;
    private readonly ILogger<HttpSinkNode<T>> _logger;
    private readonly IHttpConnectorMetrics _metrics;
    private readonly HttpSinkOptions<T> _options;
    private readonly bool _ownsClient;
    private readonly ResilientHttpSender _sender;
    private readonly JsonTypeInfo<T> _typeInfo;
    private readonly JsonWriterOptions _writerOptions;
    private Func<long, CancellationToken, ValueTask>? _written;

    // The node sends one request at a time, so the retry listener reads the request in flight from here.
    private string? _currentEndpoint;

    /// <summary>Creates a sink that uses a client from <paramref name="httpClientFactory" /> (<see cref="HttpSinkOptions{T}.HttpClientName" />).</summary>
    public HttpSinkNode(
        HttpSinkOptions<T> options,
        IHttpClientFactory httpClientFactory,
        IHttpConnectorMetrics? metrics = null,
        ILogger<HttpSinkNode<T>>? logger = null)
        : this(options, CreateClient(options, httpClientFactory), metrics, logger, true)
    {
    }

    /// <summary>Creates a sink that uses <paramref name="httpClient" />, which the caller keeps ownership of.</summary>
    public HttpSinkNode(
        HttpSinkOptions<T> options,
        HttpClient httpClient,
        IHttpConnectorMetrics? metrics = null,
        ILogger<HttpSinkNode<T>>? logger = null)
        : this(options, httpClient, metrics, logger, false)
    {
    }

    private HttpSinkNode(
        HttpSinkOptions<T> options,
        HttpClient httpClient,
        IHttpConnectorMetrics? metrics,
        ILogger<HttpSinkNode<T>>? logger,
        bool ownsClient)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();

        _options = options;
        _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        _metrics = metrics ?? NullHttpConnectorMetrics.Instance;
        _logger = logger ?? NullLogger<HttpSinkNode<T>>.Instance;
        _ownsClient = ownsClient;

        _typeInfo = options.TypeInfo ?? HttpJsonDefaults.TypeInfo<T>(options.JsonOptions);
        var serializerOptions = _typeInfo.Options;
        _writerOptions = new JsonWriterOptions { Encoder = serializerOptions.Encoder, Indented = serializerOptions.WriteIndented, SkipValidation = true };

        _httpMethod = options.Method switch
        {
            SinkHttpMethod.Put => HttpMethod.Put,
            SinkHttpMethod.Patch => HttpMethod.Patch,
            _ => HttpMethod.Post,
        };

        _sender = new ResilientHttpSender(_httpClient, options.Resilience, false, _metrics, OnResilienceEvent, options.RateLimiter);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _sender.Dispose();

        if (_ownsClient)
            _httpClient.Dispose();

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public void ReportWritesTo(Func<long, CancellationToken, ValueTask> written) => _written = written ?? throw new ArgumentNullException(nameof(written));

    /// <inheritdoc />
    public override async Task ConsumeAsync(IDataStream<T> input, PipelineContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(input);

        var deadLetters = OpenDeadLetterChannel(context);
        var run = new List<T>(_options.BatchSize);
        var body = new ArrayBufferWriter<byte>(16 * 1024);
        long handled = 0;

        await foreach (var batch in input.BatchAsync(_options.BatchSize, _options.BatchLinger, cancellationToken).ConfigureAwait(false))
        {
            Uri? runUri = null;

            // A request carries items for one URI only, so a change of URI ends the request. Items stay in input order.
            foreach (var item in batch)
            {
                var uri = _options.UriFactory is { } factory ? factory(item) : _options.Uri!;

                if (run.Count > 0 && uri != runUri)
                    handled = await SendAsync(run, runUri!, body, deadLetters, handled, cancellationToken).ConfigureAwait(false);

                run.Add(item);
                runUri = uri;
            }

            if (run.Count > 0)
                handled = await SendAsync(run, runUri!, body, deadLetters, handled, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Sends one request's items and reports them handled: sent, skipped or dead-lettered.</summary>
    private async Task<long> SendAsync(List<T> run, Uri uri, ArrayBufferWriter<byte> body, DeadLetterChannel deadLetters, long handled,
        CancellationToken cancellationToken)
    {
        await SendBatchAsync(run, uri, body, deadLetters, cancellationToken).ConfigureAwait(false);
        handled += run.Count;
        run.Clear();

        if (_written is not null)
            await _written(handled, cancellationToken).ConfigureAwait(false);

        return handled;
    }

    private async Task SendBatchAsync(List<T> items, Uri uri, ArrayBufferWriter<byte> body, DeadLetterChannel deadLetters, CancellationToken cancellationToken)
    {
        var endpoint = HttpRedaction.Endpoint(uri);
        var described = HttpRedaction.Full(uri);

        try
        {
            Serialize(items, body);

            // The pooled buffer is reused for the next batch, so the request is finished before it is overwritten.
            using var request = new HttpRequestMessage(_httpMethod, uri) { Content = new ReadOnlyMemoryContent(body.WrittenMemory) };
            request.Content.Headers.ContentType = JsonContentType;

            foreach (var (key, value) in _options.Headers)
            {
                _ = request.Headers.TryAddWithoutValidation(key, value);
            }

            // An idempotency key makes a POST or PATCH safe to retry. Without one, the resilience handler sends those
            // methods once, because a retried write the server already applied is a duplicate.
            if (_options.IdempotencyKeyFactory is { } keyFactory)
                _ = request.MarkRepeatable(keyFactory(items), _options.IdempotencyHeaderName);

            await _options.Auth.ApplyAsync(request, cancellationToken).ConfigureAwait(false);

            if (_options.RequestCustomizer is { } customize)
                await customize(request, cancellationToken).ConfigureAwait(false);

            LogSendingRequest(_logger, typeof(T).Name, _httpMethod.Method, described, items.Count);
            _currentEndpoint = endpoint;

            using var response = await _sender.SendAsync(request, cancellationToken).ConfigureAwait(false);

            if (response.IsSuccessStatusCode)
            {
                _metrics.RecordSinkWritten(endpoint, _httpMethod.Method, (int)response.StatusCode);
                ConnectorDiagnostics.RecordRowsWritten("http", uri.Scheme, items.Count);
                return;
            }

            // Judged only after the resilience handler has spent its retries, so a transient 503 is retried first.
            var text = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            var excerpt = text.Length == 0 ? null : text.Length <= ResponseExcerptLength ? text : string.Concat(text.AsSpan(0, ResponseExcerptLength), "…");

            var failure = new HttpRequestException(
                $"HttpSinkNode<{typeof(T).Name}>: {_httpMethod.Method} {described} with {items.Count} item(s) failed with " +
                $"{(int)response.StatusCode} {response.ReasonPhrase}. Body: {excerpt}",
                null,
                response.StatusCode);

            switch (_options.FailedRequests)
            {
                case HttpFailedRequestAction.Skip:
                    LogSkippedFailure(_logger, typeof(T).Name, (int)response.StatusCode, described, items.Count);
                    return;
                case HttpFailedRequestAction.DeadLetter:
                    await deadLetters.SendAsync(
                        new HttpRequestFailure<T>(described, _httpMethod.Method, response.StatusCode, excerpt, [.. items]), failure, cancellationToken)
                        .ConfigureAwait(false);

                    return;
                default:
                    throw failure;
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException and not DeadLetterSinkNotConfiguredException)
        {
            _metrics.RecordError(endpoint, _httpMethod.Method, ex);
            throw;
        }
    }

    private void Serialize(List<T> items, ArrayBufferWriter<byte> body)
    {
        body.ResetWrittenCount();
        using var writer = new Utf8JsonWriter(body, _writerOptions);

        if (_options.BatchSize == 1)
            JsonSerializer.Serialize(writer, items[0], _typeInfo);
        else
        {
            if (_options.BatchWrapperKey is { } key)
            {
                writer.WriteStartObject();
                writer.WritePropertyName(key);
            }

            writer.WriteStartArray();

            foreach (var item in items)
            {
                JsonSerializer.Serialize(writer, item, _typeInfo);
            }

            writer.WriteEndArray();

            if (_options.BatchWrapperKey is not null)
                writer.WriteEndObject();
        }

        writer.Flush();
    }

    private void OnResilienceEvent(CallEvent callEvent)
    {
        if (callEvent.Kind != CallEventKind.Retrying || _currentEndpoint is not { } endpoint)
            return;

        _metrics.RecordRetry(endpoint, _httpMethod.Method, callEvent.AttemptNumber);
        LogRetrying(_logger, typeof(T).Name, callEvent.AttemptNumber, endpoint);
    }

    private static HttpClient CreateClient(HttpSinkOptions<T> options, IHttpClientFactory httpClientFactory)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(httpClientFactory);

        return options.HttpClientName is { } name
            ? httpClientFactory.CreateClient(name)
            : httpClientFactory.CreateClient();
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "HttpSinkNode<{TypeName}>: sending {Method} {Uri} with {Count} item(s).")]
    private static partial void LogSendingRequest(ILogger logger, string typeName, string method, string uri, int count);

    [LoggerMessage(Level = LogLevel.Warning, Message = "HttpSinkNode<{TypeName}>: {Status} from {Uri}; skipping {Count} item(s) as FailedRequests is Skip.")]
    private static partial void LogSkippedFailure(ILogger logger, string typeName, int status, string uri, int count);

    [LoggerMessage(Level = LogLevel.Warning, Message = "HttpSinkNode<{TypeName}>: attempt {Attempt} failed for {Uri}, retrying.")]
    private static partial void LogRetrying(ILogger logger, string typeName, int attempt, string uri);
}

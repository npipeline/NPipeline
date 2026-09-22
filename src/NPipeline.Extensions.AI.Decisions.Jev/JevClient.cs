using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace NPipeline.Extensions.AI.Decisions.Jev;

/// <summary>A native .NET client for the TypeSafe System One HTTP API.</summary>
public sealed class JevClient : IJevClient
{
    private const string RequestIdHeader = "x-typesafe-request-id";
    private const string RetryAfterMillisecondsHeader = "retry-after-ms";
    private readonly HttpClient _httpClient;
    private readonly JevClientOptions _options;
    private readonly Uri _systemOneEndpoint;

    /// <summary>Initializes a client. The caller retains ownership of <paramref name="httpClient" />.</summary>
    public JevClient(HttpClient httpClient, JevClientOptions options)
    {
        _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        _options = Validate(options);
        _systemOneEndpoint = new Uri(EnsureTrailingSlash(_options.BaseUri), "v1/systemone");
    }

    /// <inheritdoc />
    public string DefaultModel => _options.DefaultModel;

    /// <inheritdoc />
    public async ValueTask<JevSystemOneResponse> EvaluateAsync(
        JsonNode? state,
        IReadOnlyDictionary<string, JevQuestion> questions,
        string? model = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(questions);

        if (questions.Count == 0)
            throw new ArgumentException("At least one question is required.", nameof(questions));

        var resolvedModel = string.IsNullOrWhiteSpace(model)
            ? DefaultModel
            : model;

        var request = new JevSystemOneRequest(state, resolvedModel, questions);
        var payload = JsonSerializer.SerializeToUtf8Bytes(request, JevJsonContext.Default.JevSystemOneRequest);

        for (var attempt = 0;; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            using var attemptTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            attemptTimeout.CancelAfter(_options.AttemptTimeout);

            try
            {
                using var response = await SendAttemptAsync(payload, attemptTimeout.Token).ConfigureAwait(false);

                if (response.IsSuccessStatusCode)
                    return await ReadSuccessAsync(response, resolvedModel, attemptTimeout.Token).ConfigureAwait(false);

                if (attempt < _options.Retry.MaxRetries && IsRetryable(response.StatusCode))
                {
                    var delay = GetRetryDelay(response, attempt);
                    await Task.Delay(delay, _options.TimeProvider, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                throw await CreateApiExceptionAsync(response, resolvedModel, attemptTimeout.Token).ConfigureAwait(false);
            }
            catch (HttpRequestException) when (attempt < _options.Retry.MaxRetries)
            {
                await DelayForRetryAsync(attempt, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && attempt < _options.Retry.MaxRetries)
            {
                await DelayForRetryAsync(attempt, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException exception) when (!cancellationToken.IsCancellationRequested)
            {
                throw new TimeoutException($"The TypeSafe API attempt exceeded {_options.AttemptTimeout}.", exception);
            }
        }
    }

    private async Task<HttpResponseMessage> SendAttemptAsync(byte[] payload, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, _systemOneEndpoint);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _options.ApiKey);
        request.Content = new ByteArrayContent(payload);
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json") { CharSet = "utf-8" };

        return await _httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
    }

    private static async ValueTask<JevSystemOneResponse> ReadSuccessAsync(
        HttpResponseMessage response,
        string model,
        CancellationToken cancellationToken)
    {
        try
        {
            await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);

            var result = await JsonSerializer.DeserializeAsync(stream, JevJsonContext.Default.JevSystemOneResponse, cancellationToken)
                .ConfigureAwait(false);

            if (result is null)
                throw new JsonException("The TypeSafe API returned an empty JSON response.");

            return result with { RequestId = GetHeader(response, RequestIdHeader) };
        }
        catch (JsonException exception)
        {
            throw new JevApiException(
                response.StatusCode,
                "The TypeSafe API returned an invalid success response.",
                null,
                model,
                GetHeader(response, RequestIdHeader),
                CopyHeaders(response))
            {
                Data = { ["JsonException"] = exception.Message },
            };
        }
    }

    private async ValueTask DelayForRetryAsync(int attempt, CancellationToken cancellationToken)
    {
        await Task.Delay(GetBackoffDelay(attempt), _options.TimeProvider, cancellationToken).ConfigureAwait(false);
    }

    private TimeSpan GetRetryDelay(HttpResponseMessage response, int attempt)
    {
        var serverDelay = GetServerRetryDelay(response);

        return serverDelay is not null && serverDelay <= _options.Retry.MaxRetryAfter
            ? serverDelay.Value
            : GetBackoffDelay(attempt);
    }

    private TimeSpan GetBackoffDelay(int attempt)
    {
        var exponentialMilliseconds = _options.Retry.InitialDelay.TotalMilliseconds * Math.Pow(2, attempt);
        var boundedMilliseconds = Math.Min(exponentialMilliseconds, _options.Retry.MaxDelay.TotalMilliseconds);
        var jitter = boundedMilliseconds * _options.Retry.JitterFactor * Random.Shared.NextDouble();
        return TimeSpan.FromMilliseconds(Math.Max(0, boundedMilliseconds - jitter));
    }

    private TimeSpan? GetServerRetryDelay(HttpResponseMessage response)
    {
        if (response.Headers.TryGetValues(RetryAfterMillisecondsHeader, out var millisecondValues)
            && double.TryParse(millisecondValues.FirstOrDefault(), NumberStyles.Float, CultureInfo.InvariantCulture, out var milliseconds)
            && milliseconds >= 0)
            return TimeSpan.FromMilliseconds(milliseconds);

        var retryAfter = response.Headers.RetryAfter;

        if (retryAfter?.Delta is { } delta)
            return delta;

        if (retryAfter?.Date is { } date)
            return TimeSpan.FromTicks(Math.Max(0, (date - _options.TimeProvider.GetUtcNow()).Ticks));

        return null;
    }

    private static bool IsRetryable(HttpStatusCode statusCode)
    {
        var numericStatus = (int)statusCode;

        return statusCode is HttpStatusCode.RequestTimeout or HttpStatusCode.TooManyRequests
               || numericStatus is >= 500 and <= 599;
    }

    private static async ValueTask<JevApiException> CreateApiExceptionAsync(
        HttpResponseMessage response,
        string model,
        CancellationToken cancellationToken)
    {
        var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        var requestId = GetHeader(response, RequestIdHeader);
        var message = $"The TypeSafe API returned HTTP {(int)response.StatusCode} ({response.ReasonPhrase}).";
        return new JevApiException(response.StatusCode, message, body, model, requestId, CopyHeaders(response));
    }

    private static string? GetHeader(HttpResponseMessage response, string name) => response.Headers.TryGetValues(name, out var values)
        ? values.FirstOrDefault()
        : null;

    private static IReadOnlyDictionary<string, IReadOnlyList<string>> CopyHeaders(HttpResponseMessage response)
    {
        return response.Headers
            .Concat(response.Content.Headers)
            .ToDictionary(
                header => header.Key,
                header => (IReadOnlyList<string>)header.Value.ToArray(),
                StringComparer.OrdinalIgnoreCase);
    }

    private static JevClientOptions Validate(JevClientOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.ApiKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.DefaultModel);

        if (!options.BaseUri.IsAbsoluteUri)
            throw new ArgumentException("BaseUri must be absolute.", nameof(options));

        if (options.AttemptTimeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(options), "AttemptTimeout must be positive.");

        if (options.Retry.MaxRetries < 0)
            throw new ArgumentOutOfRangeException(nameof(options), "MaxRetries cannot be negative.");

        if (options.Retry.InitialDelay < TimeSpan.Zero || options.Retry.MaxDelay < TimeSpan.Zero || options.Retry.MaxRetryAfter < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(options), "Retry delays cannot be negative.");

        if (!double.IsFinite(options.Retry.JitterFactor) || options.Retry.JitterFactor is < 0 or > 1)
            throw new ArgumentOutOfRangeException(nameof(options), "JitterFactor must be a finite value from 0 to 1.");

        return options;
    }

    private static Uri EnsureTrailingSlash(Uri baseUri)
    {
        var value = baseUri.AbsoluteUri;

        return value.EndsWith("/", StringComparison.Ordinal)
            ? baseUri
            : new Uri(value + '/', UriKind.Absolute);
    }
}

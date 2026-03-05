using System.Net;

namespace NPipeline.Connectors.Http.Retry;

/// <summary>
///     An <see cref="IHttpRetryStrategy" /> that retries transient HTTP failures using exponential backoff with jitter.
///     Respects the <c>Retry-After</c> header on 429 responses.
/// </summary>
public sealed class ExponentialBackoffHttpRetryStrategy : IHttpRetryStrategy
{
    private TimeSpan _accumulatedDelay = TimeSpan.Zero;

    /// <summary>Maximum number of retry attempts. Defaults to <c>3</c>.</summary>
    public int MaxRetries { get; init; } = 3;

    /// <summary>Base delay in milliseconds for the first retry. Defaults to <c>200</c>.</summary>
    public int BaseDelayMs { get; init; } = 200;

    /// <summary>Maximum delay in milliseconds between retries. Defaults to <c>30 000</c>.</summary>
    public int MaxDelayMs { get; init; } = 30_000;

    /// <summary>Jitter factor (0.0–1.0) to add randomness to delays. Defaults to <c>0.2</c>.</summary>
    public double JitterFactor { get; init; } = 0.2;

    /// <summary>
    ///     Maximum total delay accumulated across all retries.
    ///     When reached, further retries are refused regardless of <see cref="MaxRetries" />.
    /// </summary>
    public TimeSpan? MaxTotalRetryDelay { get; init; }

    /// <summary>
    ///     HTTP status codes that should trigger a retry.
    ///     Defaults to <c>429, 500, 502, 503, 504</c>.
    /// </summary>
    public IReadOnlySet<HttpStatusCode> RetryableStatusCodes { get; init; } =
        new HashSet<HttpStatusCode>
        {
            HttpStatusCode.TooManyRequests,
            HttpStatusCode.InternalServerError,
            HttpStatusCode.BadGateway,
            HttpStatusCode.ServiceUnavailable,
            HttpStatusCode.GatewayTimeout,
        };

    /// <summary>Default instance with standard settings.</summary>
    public static ExponentialBackoffHttpRetryStrategy Default => new();

    /// <summary>Conservative instance with fewer, longer-delay retries.</summary>
    public static ExponentialBackoffHttpRetryStrategy Conservative => new()
    {
        MaxRetries = 2,
        BaseDelayMs = 1_000,
        MaxDelayMs = 60_000,
        JitterFactor = 0.3,
    };

    /// <inheritdoc />
    public bool ShouldRetry(HttpResponseMessage? response, Exception? exception, int attempt)
    {
        if (attempt > MaxRetries)
            return false;

        if (exception is HttpRequestException or TaskCanceledException or OperationCanceledException
            {
                InnerException: TimeoutException,
            })
            return true;

        if (response != null)
            return RetryableStatusCodes.Contains(response.StatusCode);

        return exception != null;
    }

    /// <inheritdoc />
    public TimeSpan GetDelay(HttpResponseMessage? response, int attempt)
    {
        TimeSpan delay;

        // Honour the Retry-After header on 429 responses
        if (response?.StatusCode == HttpStatusCode.TooManyRequests &&
            response.Headers.RetryAfter != null)
        {
            var retryAfter = response.Headers.RetryAfter;

            if (retryAfter.Delta.HasValue)
                delay = retryAfter.Delta.Value;
            else if (retryAfter.Date.HasValue)
            {
                var computed = retryAfter.Date.Value - DateTimeOffset.UtcNow;

                delay = computed <= TimeSpan.Zero
                    ? TimeSpan.Zero
                    : computed;
            }
            else
                delay = ComputeExponentialDelay(attempt);
        }
        else
            delay = ComputeExponentialDelay(attempt);

        if (MaxTotalRetryDelay.HasValue)
        {
            var remainingBudget = MaxTotalRetryDelay.Value - _accumulatedDelay;

            if (remainingBudget <= TimeSpan.Zero)
                return TimeSpan.Zero;

            if (delay > remainingBudget)
                delay = remainingBudget;
        }

        TrackDelay(delay);
        return delay;
    }

    private TimeSpan ComputeExponentialDelay(int attempt)
    {
        var exponential = BaseDelayMs * Math.Pow(2, attempt - 1);
        var capped = Math.Min(exponential, MaxDelayMs);

        if (JitterFactor > 0)
            capped += capped * JitterFactor * Random.Shared.NextDouble();

        return TimeSpan.FromMilliseconds(capped);
    }

    private void TrackDelay(TimeSpan delay)
    {
        _accumulatedDelay += delay;
    }
}

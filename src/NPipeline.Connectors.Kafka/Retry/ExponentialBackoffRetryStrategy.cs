namespace NPipeline.Connectors.Kafka.Retry;

/// <summary>
///     A retry strategy that uses exponential backoff with optional jitter.
/// </summary>
public sealed class ExponentialBackoffRetryStrategy : IRetryStrategy
{
    /// <summary>
    ///     Gets or sets the maximum number of retry attempts.
    /// </summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>
    ///     Gets or sets the base delay in milliseconds for the first retry.
    /// </summary>
    public int BaseDelayMs { get; set; } = 100;

    /// <summary>
    ///     Gets or sets the maximum delay in milliseconds between retries.
    /// </summary>
    public int MaxDelayMs { get; set; } = 30000;

    /// <summary>
    ///     Gets or sets the jitter factor (0.0 to 1.0) to add randomness to delays.
    /// </summary>
    public double JitterFactor { get; set; } = 0.2;

    /// <summary>
    ///     Gets or sets a predicate to determine if an exception is retryable.
    ///     If null, all exceptions are considered retryable.
    /// </summary>
    public Func<Exception, bool>? IsRetryable { get; set; }

    /// <summary>
    ///     Creates a new instance with default settings.
    /// </summary>
    public static ExponentialBackoffRetryStrategy Default => new();

    /// <summary>
    ///     Creates an instance configured for aggressive retries (more attempts, shorter delays).
    /// </summary>
    public static ExponentialBackoffRetryStrategy Aggressive => new()
    {
        MaxRetries = 5,
        BaseDelayMs = 50,
        MaxDelayMs = 5000,
        JitterFactor = 0.1,
    };

    /// <summary>
    ///     Creates an instance configured for conservative retries (fewer attempts, longer delays).
    /// </summary>
    public static ExponentialBackoffRetryStrategy Conservative => new()
    {
        MaxRetries = 2,
        BaseDelayMs = 1000,
        MaxDelayMs = 60000,
        JitterFactor = 0.3,
    };

    /// <inheritdoc />
    public bool ShouldRetry(Exception ex, int attempt)
    {
        if (attempt >= MaxRetries)
            return false;

        if (IsRetryable != null)
            return IsRetryable(ex);

        return true;
    }

    /// <inheritdoc />
    public TimeSpan GetDelay(int attempt)
    {
        // Calculate exponential delay
        var exponentialDelay = BaseDelayMs * Math.Pow(2, attempt - 1);

        // Cap at max delay
        var delay = Math.Min(exponentialDelay, MaxDelayMs);

        // Add jitter
        if (JitterFactor > 0)
        {
            var jitter = delay * JitterFactor * Random.Shared.NextDouble();
            delay += jitter;
        }

        return TimeSpan.FromMilliseconds(delay);
    }
}

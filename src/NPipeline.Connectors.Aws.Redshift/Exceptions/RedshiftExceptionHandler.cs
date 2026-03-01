namespace NPipeline.Connectors.Aws.Redshift.Exceptions;

/// <summary>
///     Handles retry logic for Redshift operations with exponential backoff and jitter.
/// </summary>
public class RedshiftExceptionHandler
{
    private readonly TimeSpan _baseDelay;
    private readonly TimeSpan _maxDelay;
    private readonly Random _random = new();

    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftExceptionHandler" /> class.
    /// </summary>
    /// <param name="maxRetryAttempts">Maximum number of retry attempts.</param>
    /// <param name="baseDelay">Base delay for exponential backoff.</param>
    /// <param name="maxDelay">Maximum delay cap (default: 60 seconds).</param>
    public RedshiftExceptionHandler(
        int maxRetryAttempts = 3,
        TimeSpan? baseDelay = null,
        TimeSpan? maxDelay = null)
    {
        MaxRetryAttempts = maxRetryAttempts;
        _baseDelay = baseDelay ?? TimeSpan.FromSeconds(2);
        _maxDelay = maxDelay ?? TimeSpan.FromSeconds(60);
    }

    /// <summary>
    ///     Gets the maximum number of retry attempts.
    /// </summary>
    public int MaxRetryAttempts { get; }

    /// <summary>
    ///     Gets the base delay for exponential backoff.
    /// </summary>
    public TimeSpan BaseDelay => _baseDelay;

    /// <summary>
    ///     Gets the maximum delay cap.
    /// </summary>
    public TimeSpan MaxDelay => _maxDelay;

    /// <summary>
    ///     Determines whether a retry should be attempted.
    /// </summary>
    /// <param name="exception">The exception that occurred.</param>
    /// <param name="attemptCount">Current attempt count (1-based).</param>
    /// <returns>True if a retry should be attempted.</returns>
    public bool ShouldRetry(Exception? exception, int attemptCount)
    {
        if (attemptCount >= MaxRetryAttempts)
            return false;

        if (MaxRetryAttempts <= 0)
            return false;

        return RedshiftTransientErrorDetector.IsTransient(exception);
    }

    /// <summary>
    ///     Gets the delay before the next retry attempt using exponential backoff with jitter.
    /// </summary>
    /// <param name="attemptCount">Current attempt count (1-based).</param>
    /// <returns>The delay duration.</returns>
    public TimeSpan GetRetryDelay(int attemptCount)
    {
        // Exponential backoff: baseDelay * 2^(attemptCount - 1)
        var exponentialDelay = TimeSpan.FromTicks(
            _baseDelay.Ticks * (long)Math.Pow(2, attemptCount - 1));

        // Cap at max delay
        if (exponentialDelay > _maxDelay)
            exponentialDelay = _maxDelay;

        // Add jitter: ±25% randomization
        var jitterRange = exponentialDelay.TotalMilliseconds * 0.25;
        var jitter = _random.NextDouble() * jitterRange * 2 - jitterRange;
        var delayWithJitter = exponentialDelay.TotalMilliseconds + jitter;

        // Ensure non-negative and cap at max delay
        delayWithJitter = Math.Max(0, Math.Min(delayWithJitter, _maxDelay.TotalMilliseconds));

        return TimeSpan.FromMilliseconds(delayWithJitter);
    }

    /// <summary>
    ///     Executes an async operation with automatic retry on transient errors.
    /// </summary>
    /// <typeparam name="T">The return type.</typeparam>
    /// <param name="operation">The operation to execute.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the operation.</returns>
    public async Task<T> ExecuteWithRetryAsync<T>(
        Func<Task<T>> operation,
        CancellationToken cancellationToken = default)
    {
        var attempt = 0;

        while (true)
        {
            attempt++;

            try
            {
                return await operation().ConfigureAwait(false);
            }
            catch (Exception ex) when (ShouldRetry(ex, attempt))
            {
                var delay = GetRetryDelay(attempt);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    ///     Executes an async operation with automatic retry on transient errors.
    /// </summary>
    /// <param name="operation">The operation to execute.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task ExecuteWithRetryAsync(
        Func<Task> operation,
        CancellationToken cancellationToken = default)
    {
        var attempt = 0;

        while (true)
        {
            attempt++;

            try
            {
                await operation().ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (ShouldRetry(ex, attempt))
            {
                var delay = GetRetryDelay(attempt);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}

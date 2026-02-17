namespace NPipeline.Connectors.Kafka.Retry;

/// <summary>
///     Defines a strategy for retrying failed operations.
/// </summary>
public interface IRetryStrategy
{
    /// <summary>
    ///     Determines whether a retry should be attempted for the given exception.
    /// </summary>
    /// <param name="ex">The exception that occurred.</param>
    /// <param name="attempt">The current attempt number (1-based).</param>
    /// <returns>True if a retry should be attempted; otherwise, false.</returns>
    bool ShouldRetry(Exception ex, int attempt);

    /// <summary>
    ///     Gets the delay before the next retry attempt.
    /// </summary>
    /// <param name="attempt">The current attempt number (1-based).</param>
    /// <returns>The delay duration before the next retry.</returns>
    TimeSpan GetDelay(int attempt);
}

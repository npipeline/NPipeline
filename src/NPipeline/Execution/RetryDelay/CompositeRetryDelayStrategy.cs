using NPipeline.Execution.RetryDelay.Backoff;
using NPipeline.Execution.RetryDelay.Jitter;

namespace NPipeline.Execution.RetryDelay;

/// <summary>
///     Combines backoff and jitter strategies to calculate retry delays.
/// </summary>
/// <remarks>
///     <para>
///         This composite strategy applies a backoff algorithm to calculate the base delay,
///         then applies jitter to add randomness and prevent thundering herd problems.
///         The jitter is applied using the provided Random instance for thread safety.
///     </para>
///     <para>
///         Either the backoff strategy or jitter strategy can be null, but not both.
///         If jitter is null, only the backoff delay is used.
///         If backoff is null, an exception is thrown.
///     </para>
/// </remarks>
public sealed class CompositeRetryDelayStrategy(
    IBackoffStrategy backoffStrategy,
    IJitterStrategy? jitterStrategy,
    Random random) : IRetryDelayStrategy
{
    private readonly IBackoffStrategy _backoffStrategy = backoffStrategy ?? throw new ArgumentNullException(nameof(backoffStrategy));
    private readonly IJitterStrategy? _jitterStrategy = jitterStrategy;
    private readonly Random _random = random ?? throw new ArgumentNullException(nameof(random));

    /// <summary>
    ///     Calculates the delay for a given attempt number using backoff and optional jitter.
    /// </summary>
    /// <param name="attemptNumber">The current attempt number (0-based, where 0 is the first retry).</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests.</param>
    /// <returns>The time span to wait before the next retry attempt.</returns>
    /// <remarks>
    ///     The method first calculates the base delay using the backoff strategy,
    ///     then applies jitter if a jitter strategy is provided.
    ///     Respects cancellation requests and returns immediately if cancelled.
    /// </remarks>
    public ValueTask<TimeSpan> GetDelayAsync(int attemptNumber, CancellationToken cancellationToken = default)
    {
        // Check for cancellation before performing calculations
        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromCanceled<TimeSpan>(cancellationToken);

        // Calculate base delay using backoff strategy
        var baseDelay = _backoffStrategy.CalculateDelay(attemptNumber);

        // Apply jitter if a jitter strategy is provided
        if (_jitterStrategy != null)
        {
            var jitteredDelay = _jitterStrategy.ApplyJitter(baseDelay, _random);
            return ValueTask.FromResult(jitteredDelay);
        }

        return ValueTask.FromResult(baseDelay);
    }
}

namespace NPipeline.Execution.RetryDelay;

/// <summary>
///     Defines the contract for calculating retry delays in pipeline execution.
/// </summary>
/// <remarks>
///     <para>
///         Retry delay strategies are used to determine how long to wait between retry attempts
///         when a node fails to process an item. Different strategies can be used to implement
///         various backoff algorithms like exponential backoff, linear backoff, or fixed delays.
///     </para>
///     <para>
///         Implementations should be thread-safe and stateless, as the same instance may be
///         used across multiple nodes and concurrent operations.
///     </para>
/// </remarks>
public interface IRetryDelayStrategy
{
    /// <summary>
    ///     Calculates the delay to wait before the next retry attempt.
    /// </summary>
    /// <param name="attemptNumber">The current attempt number (0-based, where 0 is the first retry).</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests.</param>
    /// <returns>The time span to wait before the next retry attempt.</returns>
    /// <remarks>
    ///     The implementation should handle edge cases such as:
    ///     - Negative attempt numbers (should return TimeSpan.Zero)
    ///     - Overflow conditions (should cap at a reasonable maximum)
    ///     - Cancellation (should respect the cancellation token)
    /// </remarks>
    ValueTask<TimeSpan> GetDelayAsync(int attemptNumber, CancellationToken cancellationToken = default);
}

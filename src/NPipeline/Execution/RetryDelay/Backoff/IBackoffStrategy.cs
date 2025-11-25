namespace NPipeline.Execution.RetryDelay.Backoff;

/// <summary>
///     Defines the contract for backoff algorithms used in retry strategies.
/// </summary>
/// <remarks>
///     <para>
///         Backoff strategies determine the base delay calculation without jitter.
///         They implement different algorithms like exponential backoff, linear backoff,
///         or fixed delays to control how retry delays grow over time.
///     </para>
///     <para>
///         These strategies are typically combined with jitter strategies to prevent
///         thundering herd problems in distributed systems.
///     </para>
/// </remarks>
public interface IBackoffStrategy
{
    /// <summary>
    ///     Calculates the base delay for a given attempt number.
    /// </summary>
    /// <param name="attemptNumber">The current attempt number (0-based, where 0 is the first retry).</param>
    /// <returns>The base delay time span before applying jitter.</returns>
    /// <remarks>
    ///     The implementation should handle edge cases such as:
    ///     - Negative attempt numbers (should return TimeSpan.Zero)
    ///     - Overflow conditions (should cap at a reasonable maximum)
    ///     - Invalid configuration parameters
    /// </remarks>
    TimeSpan CalculateDelay(int attemptNumber);
}

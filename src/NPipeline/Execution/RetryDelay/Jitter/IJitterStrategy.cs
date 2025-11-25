namespace NPipeline.Execution.RetryDelay.Jitter;

/// <summary>
///     Defines the contract for jitter algorithms used in retry strategies.
/// </summary>
/// <remarks>
///     <para>
///         Jitter strategies add randomness to retry delays to prevent thundering herd
///         problems where multiple clients retry simultaneously. They take a base delay
///         and apply a random variation within configured bounds.
///     </para>
///     <para>
///         Common jitter strategies include full jitter, equal jitter, and decorrelated jitter.
///         These help distribute retry attempts over time in distributed systems.
///     </para>
/// </remarks>
public interface IJitterStrategy
{
    /// <summary>
    ///     Applies jitter to a base delay.
    /// </summary>
    /// <param name="baseDelay">The base delay time span to apply jitter to.</param>
    /// <param name="random">The random instance to use for jitter calculations.</param>
    /// <returns>The jittered delay time span.</returns>
    /// <remarks>
    ///     The implementation should handle edge cases such as:
    ///     - Negative or zero base delays (should return the input as-is)
    ///     - Null random instance (should throw ArgumentNullException)
    ///     - Overflow conditions (should cap at a reasonable maximum)
    /// </remarks>
    TimeSpan ApplyJitter(TimeSpan baseDelay, Random random);
}

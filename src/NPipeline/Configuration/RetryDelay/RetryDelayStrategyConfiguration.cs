using NPipeline.Execution.RetryDelay;

namespace NPipeline.Configuration.RetryDelay;

/// <summary>
///     Simple configuration for retry delay strategies using delegates.
/// </summary>
/// <remarks>
///     <para>
///         This simplified configuration uses delegates directly instead of complex configuration classes.
///         It combines a backoff strategy delegate with an optional jitter strategy delegate.
///         This approach eliminates the need for multiple configuration classes while maintaining
///         all of the functionality.
///     </para>
///     <para>
///         Common combinations:
///         <list type="bullet">
///             <item>
///                 <description>Exponential backoff + Full jitter: Best for distributed systems with transient failures</description>
///             </item>
///             <item>
///                 <description>Linear backoff + Equal jitter: Good for predictable recovery with some randomness</description>
///             </item>
///             <item>
///                 <description>Fixed delay + No jitter: Simple deterministic retry behavior</description>
///             </item>
///         </list>
///     </para>
/// </remarks>
public sealed record RetryDelayStrategyConfiguration(
    BackoffStrategy BackoffStrategy,
    JitterStrategy? JitterStrategy = null)
{
    /// <summary>
    ///     Validates the retry delay strategy configuration.
    /// </summary>
    /// <exception cref="ArgumentNullException">Thrown when BackoffStrategy is null.</exception>
    public void Validate()
    {
        ArgumentNullException.ThrowIfNull(BackoffStrategy);
    }
}

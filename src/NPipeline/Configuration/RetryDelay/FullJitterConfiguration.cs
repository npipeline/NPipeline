namespace NPipeline.Configuration.RetryDelay;

/// <summary>
///     Configuration parameters for full jitter strategy.
/// </summary>
/// <remarks>
///     <para>
///         Full jitter applies maximum randomness to retry delays,
///         distributing them uniformly between 0 and the base delay.
///         This provides the best protection against thundering herd problems.
///     </para>
///     <para>
///         This strategy is recommended for distributed systems
///         where preventing synchronized retries is critical.
///     </para>
/// </remarks>
public sealed record FullJitterConfiguration : JitterStrategyConfiguration
{
    /// <inheritdoc />
    public override string StrategyType => "FullJitter";

    /// <inheritdoc />
    public override void Validate()
    {
        // Full jitter has no configurable parameters
        // No validation needed
    }
}

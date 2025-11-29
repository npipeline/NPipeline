namespace NPipeline.Configuration.RetryDelay;

/// <summary>
///     Configuration parameters for no jitter strategy.
/// </summary>
/// <remarks>
///     <para>
///         No jitter strategy applies no randomness to retry delays,
///         using the exact base delay calculated by the backoff strategy.
///         This provides deterministic, predictable retry timing.
///     </para>
///     <para>
///         This strategy is useful for testing or scenarios where
///         predictable behavior is required and thundering herd problems
///         are not a concern.
///     </para>
/// </remarks>
public sealed record NoJitterConfiguration : JitterStrategyConfiguration
{
    /// <inheritdoc />
    public override string StrategyType => "NoJitter";

    /// <inheritdoc />
    public override void Validate()
    {
        // No jitter has no configurable parameters
        // No validation needed
    }
}

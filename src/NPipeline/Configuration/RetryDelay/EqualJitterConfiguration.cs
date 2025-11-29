namespace NPipeline.Configuration.RetryDelay;

/// <summary>
///     Configuration parameters for equal jitter strategy.
/// </summary>
/// <remarks>
///     <para>
///         Equal jitter splits the delay into two equal parts,
///         keeping one part fixed and applying jitter to the other.
///         This provides a balance between predictability and randomness.
///     </para>
///     <para>
///         This strategy is useful when you want some jitter
///         but still maintain a minimum delay for responsiveness.
///     </para>
/// </remarks>
public sealed record EqualJitterConfiguration : JitterStrategyConfiguration
{
    /// <inheritdoc />
    public override string StrategyType => "EqualJitter";

    /// <inheritdoc />
    public override void Validate()
    {
        // Equal jitter has no configurable parameters
        // No validation needed
    }
}

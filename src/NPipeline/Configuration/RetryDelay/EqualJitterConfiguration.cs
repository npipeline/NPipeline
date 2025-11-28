namespace NPipeline.Configuration.RetryDelay;

/// <summary>
///     Configuration parameters for equal jitter strategy.
/// </summary>
/// <remarks>
///     <para>
///         Equal jitter splits the delay into two equal parts: one deterministic
///         and one random, providing a balance between predictability and randomness.
///         This strategy is effective when you want some jitter while maintaining
///         a minimum guaranteed delay between retries.
///     </para>
///     <para>
///         The formula used is: jitteredDelay = (baseDelay / 2) + random.Next(0, baseDelay / 2)
///     </para>
/// </remarks>
public sealed record EqualJitterConfiguration : JitterStrategyConfiguration
{
    /// <summary>
    ///     Gets the strategy type identifier for equal jitter.
    /// </summary>
    public override string StrategyType => "EqualJitter";

    /// <summary>
    ///     Validates the configuration parameters.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when any parameter is invalid.</exception>
    /// <remarks>
    ///     Equal jitter doesn't require any specific configuration parameters,
    ///     so this method is provided for consistency with other jitter strategies.
    /// </remarks>
    public override void Validate()
    {
        // Equal jitter doesn't require any configuration parameters
        // This method is provided for consistency with other jitter strategies
    }
}

namespace NPipeline.Configuration.RetryDelay;

/// <summary>
///     Configuration parameters for no jitter strategy.
/// </summary>
/// <remarks>
///     <para>
///         No jitter applies no randomness to the delay, using the exact
///         delay calculated by the backoff strategy. This provides deterministic
///         retry timing which can be useful for testing or scenarios where
///         predictable behavior is required.
///     </para>
///     <para>
///         The formula used is: jitteredDelay = baseDelay (no modification)
///     </para>
/// </remarks>
public sealed record NoJitterConfiguration : JitterStrategyConfiguration
{
    /// <summary>
    ///     Gets the strategy type identifier for no jitter.
    /// </summary>
    public override string StrategyType => "NoJitter";

    /// <summary>
    ///     Validates the configuration parameters.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when any parameter is invalid.</exception>
    /// <remarks>
    ///     No jitter doesn't require any specific configuration parameters,
    ///     so this method is provided for consistency with other jitter strategies.
    /// </remarks>
    public override void Validate()
    {
        // No jitter doesn't require any configuration parameters
        // This method is provided for consistency with other jitter strategies
    }
}

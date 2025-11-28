namespace NPipeline.Configuration.RetryDelay;

/// <summary>
///     Configuration parameters for full jitter strategy.
/// </summary>
/// <remarks>
///     <para>
///         Full jitter generates a random delay between 0 and the base delay.
///         This strategy is most effective at preventing thundering herd problems
///         where multiple clients retry simultaneously.
///     </para>
///     <para>
///         The formula used is: jitteredDelay = random.Next(0, baseDelay.TotalMilliseconds)
///     </para>
/// </remarks>
public sealed record FullJitterConfiguration : JitterStrategyConfiguration
{
    /// <summary>
    ///     Gets the strategy type identifier for full jitter.
    /// </summary>
    public override string StrategyType => "FullJitter";

    /// <summary>
    ///     Validates the configuration parameters.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when any parameter is invalid.</exception>
    /// <remarks>
    ///     Full jitter doesn't require any specific configuration parameters,
    ///     so this method is provided for consistency with other jitter strategies.
    /// </remarks>
    public override void Validate()
    {
        // Full jitter doesn't require any configuration parameters
        // This method is provided for consistency with other jitter strategies
    }
}

namespace NPipeline.Configuration.RetryDelay;

/// <summary>
///     Configuration parameters for fixed delay strategy.
/// </summary>
/// <remarks>
///     <para>
///         Fixed delay uses the same delay for all retry attempts,
///         providing consistent and predictable retry intervals.
///     </para>
///     <para>
///         This strategy is useful for simple retry scenarios where consistent
///         retry timing is desired and there's no need for increasing delays.
///     </para>
/// </remarks>
public sealed record FixedDelayConfiguration : BackoffStrategyConfiguration
{
    private static readonly TimeSpan DefaultDelay = TimeSpan.FromSeconds(1);

    public FixedDelayConfiguration(TimeSpan? delay = null)
    {
        Delay = delay ?? DefaultDelay;
    }

    /// <summary>
    ///     Gets the fixed delay for all retry attempts.
    /// </summary>
    /// <remarks>
    ///     Must be a positive TimeSpan. Default is 1 second.
    /// </remarks>
    public TimeSpan Delay { get; init; }

    /// <summary>
    ///     Gets the strategy type identifier for fixed delay.
    /// </summary>
    public override string StrategyType => "Fixed";

    /// <summary>
    ///     Validates the configuration parameters.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when any parameter is invalid.</exception>
    public override void Validate()
    {
        if (Delay <= TimeSpan.Zero)
            throw new ArgumentException("Delay must be a positive TimeSpan.", nameof(Delay));
    }
}

namespace NPipeline.Configuration.RetryDelay;

/// <summary>
///     Configuration parameters for decorrelated jitter strategy.
/// </summary>
/// <remarks>
///     <para>
///         Decorrelated jitter adapts the delay based on the previous delay,
///         providing a balance between randomness and controlled growth.
///         This strategy is effective for scenarios where you want to avoid
///         correlated retries while still maintaining reasonable delay bounds.
///     </para>
///     <para>
///         The formula used is: delay = random between baseDelay and (previousDelay * multiplier)
///     </para>
/// </remarks>
public sealed record DecorrelatedJitterConfiguration : JitterStrategyConfiguration
{
    private static readonly TimeSpan DefaultMaxDelay = TimeSpan.FromMinutes(1);
    private const double DefaultMultiplier = 3.0;

    public DecorrelatedJitterConfiguration(TimeSpan? maxDelay = null, double? multiplier = null)
    {
        MaxDelay = maxDelay ?? DefaultMaxDelay;
        Multiplier = multiplier ?? DefaultMultiplier;
    }

    /// <summary>
    ///     Gets the maximum delay allowed for the jittered value.
    /// </summary>
    public TimeSpan MaxDelay { get; init; }

    /// <summary>
    ///     Gets the multiplier applied to the previous delay to calculate the upper bound.
    /// </summary>
    /// <remarks>
    ///     Must be greater than or equal to 1.0. Default is 3.0.
    ///     Higher values provide more randomness but can lead to longer delays.
    /// </remarks>
    public double Multiplier { get; init; }

    /// <summary>
    ///     Gets the strategy type identifier for decorrelated jitter.
    /// </summary>
    public override string StrategyType => "DecorrelatedJitter";

    /// <summary>
    ///     Validates the configuration parameters.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when any parameter is invalid.</exception>
    public override void Validate()
    {
        if (MaxDelay <= TimeSpan.Zero)
            throw new ArgumentException("MaxDelay must be a positive TimeSpan.", nameof(MaxDelay));

        if (Multiplier < 1.0)
            throw new ArgumentException("Multiplier must be greater than or equal to 1.0.", nameof(Multiplier));
    }
}
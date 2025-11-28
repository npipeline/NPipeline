namespace NPipeline.Configuration.RetryDelay;

/// <summary>
///     Configuration parameters for exponential backoff strategy.
/// </summary>
/// <remarks>
///     <para>
///         Exponential backoff increases the delay between retries exponentially,
///         using the formula: delay = baseDelay * Math.Pow(multiplier, attemptNumber)
///     </para>
///     <para>
///         This strategy is effective for handling transient failures in distributed systems,
///         as it reduces the load on struggling services while allowing them time to recover.
///     </para>
/// </remarks>
public sealed record ExponentialBackoffConfiguration : BackoffStrategyConfiguration
{
    private const double DefaultMultiplier = 2.0;
    private static readonly TimeSpan DefaultBaseDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan DefaultMaxDelay = TimeSpan.FromMinutes(1);

    public ExponentialBackoffConfiguration(TimeSpan? baseDelay = null, double? multiplier = null, TimeSpan? maxDelay = null)
    {
        BaseDelay = baseDelay ?? DefaultBaseDelay;
        Multiplier = multiplier ?? DefaultMultiplier;
        MaxDelay = maxDelay ?? DefaultMaxDelay;
    }

    /// <summary>
    ///     Gets the base delay for the first retry attempt.
    /// </summary>
    /// <remarks>
    ///     Must be a positive TimeSpan. Default is 1 second.
    /// </remarks>
    public TimeSpan BaseDelay { get; init; }

    /// <summary>
    ///     Gets the multiplier applied to the delay for each subsequent retry.
    /// </summary>
    /// <remarks>
    ///     Must be greater than or equal to 1.0. Default is 2.0 (doubling each time).
    ///     A value of 1.0 results in fixed delay behavior.
    /// </remarks>
    public double Multiplier { get; init; }

    /// <summary>
    ///     Gets the maximum delay to prevent exponential growth from becoming excessive.
    /// </summary>
    /// <remarks>
    ///     Must be greater than or equal to BaseDelay. Default is 1 minute.
    ///     When calculated delay exceeds this value, the max delay is used instead.
    /// </remarks>
    public TimeSpan MaxDelay { get; init; }

    /// <summary>
    ///     Gets the strategy type identifier for exponential backoff.
    /// </summary>
    public override string StrategyType => "Exponential";

    /// <summary>
    ///     Validates the configuration parameters.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when any parameter is invalid.</exception>
    public override void Validate()
    {
        if (BaseDelay <= TimeSpan.Zero)
            throw new ArgumentException("BaseDelay must be a positive TimeSpan.", nameof(BaseDelay));

        if (Multiplier < 1.0)
            throw new ArgumentException("Multiplier must be greater than or equal to 1.0.", nameof(Multiplier));

        if (MaxDelay < BaseDelay)
            throw new ArgumentException("MaxDelay must be greater than or equal to BaseDelay.", nameof(MaxDelay));
    }
}

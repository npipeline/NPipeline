namespace NPipeline.Configuration.RetryDelay;

/// <summary>
///     Configuration parameters for linear backoff strategy.
/// </summary>
/// <remarks>
///     <para>
///         Linear backoff increases the delay between retries by a constant amount,
///         using the formula: delay = baseDelay + (increment * attemptNumber)
///     </para>
///     <para>
///         This strategy is useful for scenarios where recovery time is expected to be
///         predictable and gradual, providing a steady increase in retry intervals.
///     </para>
/// </remarks>
public sealed record LinearBackoffConfiguration : BackoffStrategyConfiguration
{
    private static readonly TimeSpan DefaultBaseDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan DefaultIncrement = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan DefaultMaxDelay = TimeSpan.FromMinutes(1);

    public LinearBackoffConfiguration(TimeSpan? baseDelay = null, TimeSpan? increment = null, TimeSpan? maxDelay = null)
    {
        BaseDelay = baseDelay ?? DefaultBaseDelay;
        Increment = increment ?? DefaultIncrement;
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
    ///     Gets the increment added to the delay for each subsequent retry.
    /// </summary>
    /// <remarks>
    ///     Must be a non-negative TimeSpan. Default is 1 second.
    ///     A value of zero results in fixed delay behavior.
    /// </remarks>
    public TimeSpan Increment { get; init; }

    /// <summary>
    ///     Gets the maximum delay to prevent linear growth from becoming excessive.
    /// </summary>
    /// <remarks>
    ///     Must be greater than or equal to BaseDelay. Default is 1 minute.
    ///     When calculated delay exceeds this value, the max delay is used instead.
    /// </remarks>
    public TimeSpan MaxDelay { get; init; }

    /// <summary>
    ///     Gets the strategy type identifier for linear backoff.
    /// </summary>
    public override string StrategyType => "Linear";

    /// <summary>
    ///     Validates the configuration parameters.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when any parameter is invalid.</exception>
    public override void Validate()
    {
        if (BaseDelay <= TimeSpan.Zero)
            throw new ArgumentException("BaseDelay must be a positive TimeSpan.", nameof(BaseDelay));

        if (Increment < TimeSpan.Zero)
            throw new ArgumentException("Increment must be a non-negative TimeSpan.", nameof(Increment));

        if (MaxDelay < BaseDelay)
            throw new ArgumentException("MaxDelay must be greater than or equal to BaseDelay.", nameof(MaxDelay));
    }
}

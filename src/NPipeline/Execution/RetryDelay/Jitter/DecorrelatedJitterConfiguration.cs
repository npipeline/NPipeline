namespace NPipeline.Execution.RetryDelay.Jitter;

/// <summary>
///     Configuration parameters for decorrelated jitter strategy.
/// </summary>
/// <remarks>
///     <para>
///         Decorrelated jitter takes into account the previous delay to create better distribution.
///         The formula used is: jitteredDelay = random.Next(baseDelay.TotalMilliseconds, previousDelay * 3.0)
///     </para>
///     <para>
///         This strategy requires tracking the previous delay value and should cap at the configured MaxDelay.
///         It provides better distribution than simple jitter while maintaining good performance.
///     </para>
/// </remarks>
public sealed class DecorrelatedJitterConfiguration
{
    /// <summary>
    ///     Gets or sets the maximum delay to prevent excessive growth.
    /// </summary>
    /// <remarks>
    ///     Must be a positive TimeSpan. Default is 1 minute.
    ///     When calculated delay exceeds this value, the max delay is used instead.
    /// </remarks>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    ///     Gets or sets the multiplier for the previous delay.
    /// </summary>
    /// <remarks>
    ///     Must be greater than or equal to 1.0. Default is 3.0.
    ///     This multiplier is applied to the previous delay to determine the upper bound.
    /// </remarks>
    public double Multiplier { get; set; } = 3.0;

    /// <summary>
    ///     Validates the configuration parameters.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when any parameter is invalid.</exception>
    public void Validate()
    {
        if (MaxDelay <= TimeSpan.Zero)
            throw new ArgumentException("MaxDelay must be a positive TimeSpan.", nameof(MaxDelay));

        if (Multiplier < 1.0)
            throw new ArgumentException("Multiplier must be greater than or equal to 1.0.", nameof(Multiplier));
    }
}

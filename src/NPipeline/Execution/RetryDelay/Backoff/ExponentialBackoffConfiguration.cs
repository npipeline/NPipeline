namespace NPipeline.Execution.RetryDelay.Backoff;

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
public sealed class ExponentialBackoffConfiguration
{
    /// <summary>
    ///     Gets or sets the base delay for the first retry attempt.
    /// </summary>
    /// <remarks>
    ///     Must be a positive TimeSpan. Default is 1 second.
    /// </remarks>
    public TimeSpan BaseDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    ///     Gets or sets the multiplier applied to the delay for each subsequent retry.
    /// </summary>
    /// <remarks>
    ///     Must be greater than or equal to 1.0. Default is 2.0 (doubling each time).
    ///     A value of 1.0 results in fixed delay behavior.
    /// </remarks>
    public double Multiplier { get; set; } = 2.0;

    /// <summary>
    ///     Gets or sets the maximum delay to prevent exponential growth from becoming excessive.
    /// </summary>
    /// <remarks>
    ///     Must be greater than or equal to BaseDelay. Default is 1 minute.
    ///     When calculated delay exceeds this value, the max delay is used instead.
    /// </remarks>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    ///     Validates the configuration parameters.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when any parameter is invalid.</exception>
    public void Validate()
    {
        if (BaseDelay <= TimeSpan.Zero)
            throw new ArgumentException("BaseDelay must be a positive TimeSpan.", nameof(BaseDelay));

        if (Multiplier < 1.0)
            throw new ArgumentException("Multiplier must be greater than or equal to 1.0.", nameof(Multiplier));

        if (MaxDelay < BaseDelay)
            throw new ArgumentException("MaxDelay must be greater than or equal to BaseDelay.", nameof(MaxDelay));
    }
}

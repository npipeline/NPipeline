namespace NPipeline.Execution.RetryDelay.Backoff;

/// <summary>
///     Implements exponential backoff strategy for retry delays.
/// </summary>
/// <remarks>
///     <para>
///         This strategy increases the delay between retries exponentially using the formula:
///         delay = baseDelay * Math.Pow(multiplier, attemptNumber)
///     </para>
///     <para>
///         The implementation handles edge cases such as:
///         - Negative attempt numbers (returns TimeSpan.Zero)
///         - Overflow conditions (caps at MaxDelay)
///         - Invalid configuration parameters (throws ArgumentException)
///     </para>
/// </remarks>
public sealed class ExponentialBackoffStrategy : IBackoffStrategy
{
    private readonly ExponentialBackoffConfiguration _configuration;

    /// <summary>
    ///     Initializes a new instance of the <see cref="ExponentialBackoffStrategy" /> class.
    /// </summary>
    /// <param name="configuration">The configuration for exponential backoff.</param>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <exception cref="ArgumentException">Thrown when configuration is invalid.</exception>
    public ExponentialBackoffStrategy(ExponentialBackoffConfiguration configuration)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.Validate();
    }

    /// <summary>
    ///     Calculates the delay for a given attempt number using exponential backoff.
    /// </summary>
    /// <param name="attemptNumber">The current attempt number (0-based, where 0 is the first retry).</param>
    /// <returns>The calculated delay time span.</returns>
    /// <remarks>
    ///     Uses the formula: delay = baseDelay * Math.Pow(multiplier, attemptNumber)
    ///     The result is capped at MaxDelay to prevent excessive delays.
    ///     Returns TimeSpan.Zero for negative attempt numbers.
    /// </remarks>
    public TimeSpan CalculateDelay(int attemptNumber)
    {
        // Handle invalid input
        if (attemptNumber < 0)
            return TimeSpan.Zero;

        try
        {
            var multiplierPower = Math.Pow(_configuration.Multiplier, attemptNumber);
            var candidateTicks = _configuration.BaseDelay.Ticks * multiplierPower;

            if (double.IsNaN(candidateTicks) || double.IsInfinity(candidateTicks) || candidateTicks < 0)
                return _configuration.MaxDelay;

            var calculatedDelayTicks = checked((long)Math.Round(candidateTicks, MidpointRounding.AwayFromZero));

            if (calculatedDelayTicks > _configuration.MaxDelay.Ticks)
                return _configuration.MaxDelay;

            return TimeSpan.FromTicks(calculatedDelayTicks);
        }
        catch (OverflowException)
        {
            return _configuration.MaxDelay;
        }
    }
}

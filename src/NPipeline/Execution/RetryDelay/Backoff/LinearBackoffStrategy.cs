namespace NPipeline.Execution.RetryDelay.Backoff;

/// <summary>
///     Configuration parameters for linear backoff strategy.
/// </summary>
/// <remarks>
///     <para>
///         Linear backoff increases the delay between retries linearly,
///         using the formula: delay = baseDelay + (attemptNumber * increment)
///     </para>
///     <para>
///         This strategy provides a predictable increase in delay time,
///         making it suitable for scenarios where a steady progression is preferred.
///     </para>
/// </remarks>
public sealed class LinearBackoffConfiguration
{
    /// <summary>
    ///     Gets or sets the base delay for the first retry attempt.
    /// </summary>
    /// <remarks>
    ///     Must be a positive TimeSpan. Default is 1 second.
    /// </remarks>
    public TimeSpan BaseDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    ///     Gets or sets the increment added to the delay for each subsequent retry.
    /// </summary>
    /// <remarks>
    ///     Must be a positive TimeSpan. Default is 1 second.
    ///     A value of zero results in fixed delay behavior.
    /// </remarks>
    public TimeSpan Increment { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    ///     Gets or sets the maximum delay to prevent linear growth from becoming excessive.
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

        if (Increment < TimeSpan.Zero)
            throw new ArgumentException("Increment must be a non-negative TimeSpan.", nameof(Increment));

        if (MaxDelay < BaseDelay)
            throw new ArgumentException("MaxDelay must be greater than or equal to BaseDelay.", nameof(MaxDelay));
    }
}

/// <summary>
///     Implements linear backoff strategy for retry delays.
/// </summary>
/// <remarks>
///     <para>
///         This strategy increases the delay between retries linearly using the formula:
///         delay = baseDelay + (attemptNumber * increment)
///     </para>
///     <para>
///         The implementation handles edge cases such as:
///         - Negative attempt numbers (returns TimeSpan.Zero)
///         - Overflow conditions (caps at MaxDelay)
///         - Invalid configuration parameters (throws ArgumentException)
///     </para>
/// </remarks>
public sealed class LinearBackoffStrategy : IBackoffStrategy
{
    private readonly LinearBackoffConfiguration _configuration;

    /// <summary>
    ///     Initializes a new instance of the <see cref="LinearBackoffStrategy" /> class.
    /// </summary>
    /// <param name="configuration">The configuration for linear backoff.</param>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <exception cref="ArgumentException">Thrown when configuration is invalid.</exception>
    public LinearBackoffStrategy(LinearBackoffConfiguration configuration)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.Validate();
    }

    /// <summary>
    ///     Calculates the delay for a given attempt number using linear backoff.
    /// </summary>
    /// <param name="attemptNumber">The current attempt number (0-based, where 0 is the first retry).</param>
    /// <returns>The calculated delay time span.</returns>
    /// <remarks>
    ///     Uses the formula: delay = baseDelay + (attemptNumber * increment)
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
            var incrementTicks = checked(_configuration.Increment.Ticks * attemptNumber);
            var totalTicks = checked(_configuration.BaseDelay.Ticks + incrementTicks);

            if (totalTicks > _configuration.MaxDelay.Ticks)
                return _configuration.MaxDelay;

            return TimeSpan.FromTicks(totalTicks);
        }
        catch (OverflowException)
        {
            return _configuration.MaxDelay;
        }
    }
}

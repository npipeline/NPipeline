namespace NPipeline.Execution.RetryDelay.Backoff;

/// <summary>
///     Configuration parameters for fixed delay strategy.
/// </summary>
/// <remarks>
///     <para>
///         Fixed delay strategy uses the same delay for all retry attempts.
///         This is the simplest backoff strategy and provides consistent retry timing.
///     </para>
///     <para>
///         This strategy is useful when you want to retry at regular intervals
///         without increasing the delay between attempts.
///     </para>
/// </remarks>
public sealed class FixedDelayConfiguration
{
    /// <summary>
    ///     Gets or sets the fixed delay for all retry attempts.
    /// </summary>
    /// <remarks>
    ///     Must be a positive TimeSpan. Default is 1 second.
    /// </remarks>
    public TimeSpan Delay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    ///     Validates the configuration parameters.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when any parameter is invalid.</exception>
    public void Validate()
    {
        if (Delay <= TimeSpan.Zero)
            throw new ArgumentException("Delay must be a positive TimeSpan.", nameof(Delay));
    }
}

/// <summary>
///     Implements fixed delay strategy for retry delays.
/// </summary>
/// <remarks>
///     <para>
///         This strategy uses the same delay for all retry attempts.
///         It provides consistent timing regardless of the attempt number.
///     </para>
///     <para>
///         The implementation handles edge cases such as:
///         - Negative attempt numbers (returns TimeSpan.Zero)
///         - Invalid configuration parameters (throws ArgumentException)
///     </para>
/// </remarks>
public sealed class FixedDelayStrategy : IBackoffStrategy
{
    private readonly FixedDelayConfiguration _configuration;

    /// <summary>
    ///     Initializes a new instance of the <see cref="FixedDelayStrategy" /> class.
    /// </summary>
    /// <param name="configuration">The configuration for fixed delay.</param>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <exception cref="ArgumentException">Thrown when configuration is invalid.</exception>
    public FixedDelayStrategy(FixedDelayConfiguration configuration)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.Validate();
    }

    /// <summary>
    ///     Returns the fixed delay for any attempt number.
    /// </summary>
    /// <param name="attemptNumber">The current attempt number (0-based, where 0 is the first retry).</param>
    /// <returns>The fixed delay time span.</returns>
    /// <remarks>
    ///     Returns the configured delay for all non-negative attempt numbers.
    ///     Returns TimeSpan.Zero for negative attempt numbers.
    /// </remarks>
    public TimeSpan CalculateDelay(int attemptNumber)
    {
        // Handle invalid input
        if (attemptNumber < 0)
            return TimeSpan.Zero;

        return _configuration.Delay;
    }
}

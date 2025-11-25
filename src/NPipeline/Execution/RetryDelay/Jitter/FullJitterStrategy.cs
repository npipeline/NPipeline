namespace NPipeline.Execution.RetryDelay.Jitter;

/// <summary>
///     Implements full jitter strategy for retry delays.
/// </summary>
/// <remarks>
///     <para>
///         This strategy generates a random delay between 0 and the base delay.
///         It is most effective at preventing thundering herd problems where multiple
///         clients retry simultaneously.
///     </para>
///     <para>
///         The formula used is: jitteredDelay = random.Next(0, baseDelay.TotalMilliseconds)
///     </para>
/// </remarks>
public sealed class FullJitterStrategy : IJitterStrategy
{
    private readonly FullJitterConfiguration _configuration;

    /// <summary>
    ///     Initializes a new instance of the <see cref="FullJitterStrategy" /> class.
    /// </summary>
    /// <param name="configuration">The configuration for full jitter.</param>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <exception cref="ArgumentException">Thrown when configuration is invalid.</exception>
    public FullJitterStrategy(FullJitterConfiguration configuration)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.Validate();
    }

    /// <summary>
    ///     Applies full jitter to a base delay.
    /// </summary>
    /// <param name="baseDelay">The base delay time span to apply jitter to.</param>
    /// <param name="random">The random instance to use for jitter calculations.</param>
    /// <returns>The jittered delay time span.</returns>
    /// <exception cref="ArgumentNullException">Thrown when random is null.</exception>
    /// <remarks>
    ///     Generates a random delay between 0 and the base delay.
    ///     Returns TimeSpan.Zero for negative or zero base delays.
    /// </remarks>
    public TimeSpan ApplyJitter(TimeSpan baseDelay, Random random)
    {
        ArgumentNullException.ThrowIfNull(random);

        // Handle invalid input
        if (baseDelay <= TimeSpan.Zero)
            return TimeSpan.Zero;

        // Generate random delay between 0 (inclusive) and baseDelay (inclusive)
        var jitterTicks = (long)Math.Round(baseDelay.Ticks * random.NextDouble(), MidpointRounding.AwayFromZero);

        if (jitterTicks > baseDelay.Ticks)
            jitterTicks = baseDelay.Ticks;

        return TimeSpan.FromTicks(jitterTicks);
    }
}

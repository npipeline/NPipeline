namespace NPipeline.Execution.RetryDelay.Jitter;

/// <summary>
///     Implements equal jitter strategy for retry delays.
/// </summary>
/// <remarks>
///     <para>
///         This strategy splits the delay equally between a fixed portion and random portion.
///         The formula used is: jitteredDelay = baseDelay/2 + random.Next(0, baseDelay/2)
///     </para>
///     <para>
///         This strategy provides a balance between predictability and randomness,
///         ensuring at least half of the base delay while still adding jitter to prevent
///         thundering herd problems.
///     </para>
/// </remarks>
public sealed class EqualJitterStrategy : IJitterStrategy
{
    private readonly EqualJitterConfiguration _configuration;

    /// <summary>
    ///     Initializes a new instance of the <see cref="EqualJitterStrategy" /> class.
    /// </summary>
    /// <param name="configuration">The configuration for equal jitter.</param>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <exception cref="ArgumentException">Thrown when configuration is invalid.</exception>
    public EqualJitterStrategy(EqualJitterConfiguration configuration)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.Validate();
    }

    /// <summary>
    ///     Applies equal jitter to a base delay.
    /// </summary>
    /// <param name="baseDelay">The base delay time span to apply jitter to.</param>
    /// <param name="random">The random instance to use for jitter calculations.</param>
    /// <returns>The jittered delay time span.</returns>
    /// <exception cref="ArgumentNullException">Thrown when random is null.</exception>
    /// <remarks>
    ///     Splits the delay equally between a fixed portion and random portion.
    ///     The formula used is: jitteredDelay = baseDelay/2 + random.Next(0, baseDelay/2)
    ///     Returns TimeSpan.Zero for negative or zero base delays.
    /// </remarks>
    public TimeSpan ApplyJitter(TimeSpan baseDelay, Random random)
    {
        ArgumentNullException.ThrowIfNull(random);

        // Handle invalid input
        if (baseDelay <= TimeSpan.Zero)
            return TimeSpan.Zero;

        // Split the delay: half fixed, half random using double precision to avoid truncation
        var baseTicks = baseDelay.Ticks;
        var deterministicTicks = baseTicks / 2.0;
        var jitterTicks = deterministicTicks * random.NextDouble();

        var totalTicks = (long)Math.Round(deterministicTicks + jitterTicks, MidpointRounding.AwayFromZero);

        if (totalTicks > baseTicks)
            totalTicks = baseTicks;

        return TimeSpan.FromTicks(totalTicks);
    }
}

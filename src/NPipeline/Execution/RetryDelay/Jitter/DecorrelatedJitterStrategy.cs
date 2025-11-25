namespace NPipeline.Execution.RetryDelay.Jitter;

/// <summary>
///     Implements decorrelated jitter strategy for retry delays.
/// </summary>
/// <remarks>
///     <para>
///         This strategy takes into account the previous delay to create better distribution.
///         The formula used is: jitteredDelay = random.Next(baseDelay.TotalMilliseconds, previousDelay * 3.0)
///     </para>
///     <para>
///         This strategy requires tracking the previous delay value and caps at the configured MaxDelay.
///         It provides better distribution than simple jitter while maintaining good performance.
///         The implementation is thread-safe and can be used concurrently.
///     </para>
/// </remarks>
public sealed class DecorrelatedJitterStrategy : IJitterStrategy
{
    private readonly DecorrelatedJitterConfiguration _configuration;
    private readonly object _lockObject = new();
    private TimeSpan _previousDelay;

    /// <summary>
    ///     Initializes a new instance of the <see cref="DecorrelatedJitterStrategy" /> class.
    /// </summary>
    /// <param name="configuration">The configuration for decorrelated jitter.</param>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <exception cref="ArgumentException">Thrown when configuration is invalid.</exception>
    public DecorrelatedJitterStrategy(DecorrelatedJitterConfiguration configuration)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.Validate();
        _previousDelay = TimeSpan.Zero;
    }

    /// <summary>
    ///     Applies decorrelated jitter to a base delay.
    /// </summary>
    /// <param name="baseDelay">The base delay time span to apply jitter to.</param>
    /// <param name="random">The random instance to use for jitter calculations.</param>
    /// <returns>The jittered delay time span.</returns>
    /// <exception cref="ArgumentNullException">Thrown when random is null.</exception>
    /// <remarks>
    ///     Generates a random delay between baseDelay and previousDelay * multiplier.
    ///     For the first call, uses baseDelay as the upper bound.
    ///     Caps the result at MaxDelay to prevent excessive delays.
    ///     Returns TimeSpan.Zero for negative or zero base delays.
    /// </remarks>
    public TimeSpan ApplyJitter(TimeSpan baseDelay, Random random)
    {
        ArgumentNullException.ThrowIfNull(random);

        // Handle invalid input
        if (baseDelay <= TimeSpan.Zero)
            return TimeSpan.Zero;

        lock (_lockObject)
        {
            var minTicks = Math.Max(0L, baseDelay.Ticks);

            long maxTicks;

            if (_previousDelay == TimeSpan.Zero)
                maxTicks = minTicks;
            else
            {
                var candidate = _previousDelay.Ticks * _configuration.Multiplier;

                var previousUpperTicks = double.IsInfinity(candidate) || candidate >= long.MaxValue
                    ? long.MaxValue
                    : (long)Math.Round(candidate, MidpointRounding.AwayFromZero);

                maxTicks = Math.Max(minTicks, previousUpperTicks);
            }

            if (maxTicks > _configuration.MaxDelay.Ticks)
                maxTicks = _configuration.MaxDelay.Ticks;

            if (maxTicks <= minTicks)
            {
                var clamped = TimeSpan.FromTicks(Math.Min(minTicks, _configuration.MaxDelay.Ticks));
                _previousDelay = clamped;
                return clamped;
            }

            var randomFraction = random.NextDouble();
            var jitterTicks = minTicks + (long)Math.Round((maxTicks - minTicks) * randomFraction, MidpointRounding.AwayFromZero);

            if (jitterTicks > _configuration.MaxDelay.Ticks)
                jitterTicks = _configuration.MaxDelay.Ticks;

            var result = TimeSpan.FromTicks(jitterTicks);
            _previousDelay = result;
            return result;
        }
    }
}

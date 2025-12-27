namespace NPipeline.Execution.RetryDelay;

/// <summary>
///     Defines a delegate type for jitter algorithms used in retry strategies.
/// </summary>
/// <remarks>
///     <para>
///         This delegate represents a function that applies jitter to a base delay.
///         It takes a base delay and a random instance, and returns the jittered delay.
///     </para>
///     <para>
///         This approach replaces the IJitterStrategy interface with a more concise
///         delegate-based implementation, reducing the need for multiple classes
///         for simple mathematical operations.
///     </para>
/// </remarks>
/// <param name="baseDelay">The base delay time span to apply jitter to.</param>
/// <param name="random">The random instance to use for jitter calculations.</param>
/// <returns>The jittered delay time span.</returns>
public delegate TimeSpan JitterStrategy(TimeSpan baseDelay, Random random);

/// <summary>
///     Provides static methods for common jitter strategies.
/// </summary>
/// <remarks>
///     <para>
///         This class contains implementations of common jitter algorithms as static methods.
///         Each method returns a JitterStrategy delegate that can be used to apply jitter.
///     </para>
///     <para>
///         These methods replace the individual jitter strategy classes with a more
///         concise and functional approach.
///     </para>
/// </remarks>
public static class JitterStrategies
{
    /// <summary>
    ///     Creates a full jitter strategy.
    /// </summary>
    /// <returns>A jitter strategy that applies full jitter.</returns>
    /// <remarks>
    ///     Full jitter generates a random delay between 0 and the base delay.
    ///     This strategy is most effective at preventing thundering herd problems
    ///     where multiple clients retry simultaneously.
    ///     The formula used is: jitteredDelay = random.Next(0, baseDelay.TotalMilliseconds)
    /// </remarks>
    public static JitterStrategy FullJitter()
    {
        return (baseDelay, random) =>
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
        };
    }

    /// <summary>
    ///     Creates an equal jitter strategy.
    /// </summary>
    /// <returns>A jitter strategy that applies equal jitter.</returns>
    /// <remarks>
    ///     Equal jitter splits the delay equally between a fixed portion and random portion.
    ///     The formula used is: jitteredDelay = baseDelay/2 + random.Next(0, baseDelay/2)
    ///     This strategy provides a balance between predictability and randomness,
    ///     ensuring at least half of the base delay while still adding jitter to prevent
    ///     thundering herd problems.
    /// </remarks>
    public static JitterStrategy EqualJitter()
    {
        return (baseDelay, random) =>
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
        };
    }

    /// <summary>
    ///     Creates a decorrelated jitter strategy with the specified parameters.
    /// </summary>
    /// <param name="maxDelay">The maximum delay to prevent excessive growth.</param>
    /// <param name="multiplier">The multiplier for the previous delay.</param>
    /// <returns>A jitter strategy that applies decorrelated jitter.</returns>
    /// <exception cref="ArgumentException">Thrown when maxDelay is not positive or multiplier is less than 1.0.</exception>
    /// <remarks>
    ///     Decorrelated jitter takes into account the previous delay to create better distribution.
    ///     The formula used is: jitteredDelay = random.Next(baseDelay.TotalMilliseconds, previousDelay * multiplier)
    ///     This strategy requires tracking the previous delay value and caps at the configured MaxDelay.
    ///     It provides better distribution than simple jitter while maintaining good performance.
    /// </remarks>
    public static JitterStrategy DecorrelatedJitter(TimeSpan maxDelay, double multiplier = 3.0)
    {
        if (maxDelay <= TimeSpan.Zero)
            throw new ArgumentException("MaxDelay must be a positive TimeSpan.", nameof(maxDelay));

        if (multiplier < 1.0)
            throw new ArgumentException("Multiplier must be greater than or equal to 1.0.", nameof(multiplier));

        var previousDelay = TimeSpan.Zero;
        var lockObject = new object();

        return (baseDelay, random) =>
        {
            ArgumentNullException.ThrowIfNull(random);

            // Handle invalid input
            if (baseDelay <= TimeSpan.Zero)
                return TimeSpan.Zero;

            lock (lockObject)
            {
                var minTicks = Math.Max(0L, baseDelay.Ticks);

                long maxTicks;

                if (previousDelay == TimeSpan.Zero)
                    maxTicks = minTicks;
                else
                {
                    var candidate = previousDelay.Ticks * multiplier;

                    var previousUpperTicks = double.IsInfinity(candidate) || candidate >= long.MaxValue
                        ? long.MaxValue
                        : (long)Math.Round(candidate, MidpointRounding.AwayFromZero);

                    maxTicks = Math.Max(minTicks, previousUpperTicks);
                }

                if (maxTicks > maxDelay.Ticks)
                    maxTicks = maxDelay.Ticks;

                if (maxTicks <= minTicks)
                {
                    var clamped = TimeSpan.FromTicks(Math.Min(minTicks, maxDelay.Ticks));
                    previousDelay = clamped;
                    return clamped;
                }

                var randomFraction = random.NextDouble();
                var jitterTicks = minTicks + (long)Math.Round((maxTicks - minTicks) * randomFraction, MidpointRounding.AwayFromZero);

                if (jitterTicks > maxDelay.Ticks)
                    jitterTicks = maxDelay.Ticks;

                var result = TimeSpan.FromTicks(jitterTicks);
                previousDelay = result;
                return result;
            }
        };
    }

    /// <summary>
    ///     Creates a no jitter strategy.
    /// </summary>
    /// <returns>A jitter strategy that doesn't apply any jitter.</returns>
    /// <remarks>
    ///     No jitter simply returns the base delay without any randomization.
    ///     This strategy is useful for testing or when deterministic behavior is needed.
    ///     The formula used is: jitteredDelay = baseDelay
    /// </remarks>
    public static JitterStrategy NoJitter()
    {
        return (baseDelay, random) =>
        {
            ArgumentNullException.ThrowIfNull(random);

            // Handle invalid input
            if (baseDelay <= TimeSpan.Zero)
                return TimeSpan.Zero;

            // No jitter - return the base delay as-is
            return baseDelay;
        };
    }
}

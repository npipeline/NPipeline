namespace NPipeline.Execution.RetryDelay;

/// <summary>
///     Provides factory methods for creating backoff strategies.
/// </summary>
/// <remarks>
///     <para>
///         This class contains static methods that create backoff strategies as delegates.
///         Each method returns a BackoffStrategy delegate that can be used to calculate
///         retry delays based on the attempt number.
///     </para>
///     <para>
///         These strategies are designed to be simple, stateless functions that calculate
///         delays based on mathematical formulas. They can be combined with jitter strategies
///         to prevent thundering herd problems in distributed systems.
///     </para>
/// </remarks>
public static class BackoffStrategies
{
    /// <summary>
    ///     Creates an exponential backoff strategy.
    /// </summary>
    /// <param name="baseDelay">The base delay for the first retry attempt.</param>
    /// <param name="multiplier">The multiplier applied to the delay for each subsequent retry.</param>
    /// <param name="maxDelay">The maximum delay to prevent exponential growth from becoming excessive.</param>
    /// <returns>A backoff strategy delegate that implements exponential backoff.</returns>
    /// <exception cref="ArgumentException">Thrown when any parameter is invalid.</exception>
    /// <remarks>
    ///     Exponential backoff increases the delay between retries exponentially using the formula:
    ///     delay = baseDelay * Math.Pow(multiplier, attemptNumber)
    ///     The result is capped at maxDelay to prevent excessive delays.
    ///     Returns TimeSpan.Zero for negative attempt numbers.
    /// </remarks>
    public static BackoffStrategy ExponentialBackoff(TimeSpan baseDelay, double multiplier = 2.0, TimeSpan? maxDelay = null)
    {
        if (baseDelay <= TimeSpan.Zero)
            throw new ArgumentException("BaseDelay must be a positive TimeSpan.", nameof(baseDelay));

        if (multiplier < 1.0)
            throw new ArgumentException("Multiplier must be greater than or equal to 1.0.", nameof(multiplier));

        var effectiveMaxDelay = maxDelay ?? TimeSpan.FromMinutes(1);

        if (effectiveMaxDelay < baseDelay)
            throw new ArgumentException("MaxDelay must be greater than or equal to BaseDelay.", nameof(maxDelay));

        return attemptNumber =>
        {
            // Handle invalid input
            if (attemptNumber < 0)
                return TimeSpan.Zero;

            try
            {
                var multiplierPower = Math.Pow(multiplier, attemptNumber);
                var candidateTicks = baseDelay.Ticks * multiplierPower;

                if (double.IsNaN(candidateTicks) || double.IsInfinity(candidateTicks) || candidateTicks < 0)
                    return effectiveMaxDelay;

                var calculatedDelayTicks = checked((long)Math.Round(candidateTicks, MidpointRounding.AwayFromZero));

                if (calculatedDelayTicks > effectiveMaxDelay.Ticks)
                    return effectiveMaxDelay;

                return TimeSpan.FromTicks(calculatedDelayTicks);
            }
            catch (OverflowException)
            {
                return effectiveMaxDelay;
            }
        };
    }

    /// <summary>
    ///     Creates a linear backoff strategy.
    /// </summary>
    /// <param name="baseDelay">The base delay for the first retry attempt.</param>
    /// <param name="increment">The increment added to the delay for each subsequent retry.</param>
    /// <param name="maxDelay">The maximum delay to prevent linear growth from becoming excessive.</param>
    /// <returns>A backoff strategy delegate that implements linear backoff.</returns>
    /// <exception cref="ArgumentException">Thrown when any parameter is invalid.</exception>
    /// <remarks>
    ///     Linear backoff increases the delay between retries linearly using the formula:
    ///     delay = baseDelay + (attemptNumber * increment)
    ///     The result is capped at maxDelay to prevent excessive delays.
    ///     Returns TimeSpan.Zero for negative attempt numbers.
    /// </remarks>
    public static BackoffStrategy LinearBackoff(TimeSpan baseDelay, TimeSpan? increment = null, TimeSpan? maxDelay = null)
    {
        if (baseDelay <= TimeSpan.Zero)
            throw new ArgumentException("BaseDelay must be a positive TimeSpan.", nameof(baseDelay));

        var effectiveIncrement = increment ?? TimeSpan.FromSeconds(1);

        if (effectiveIncrement < TimeSpan.Zero)
            throw new ArgumentException("Increment must be a non-negative TimeSpan.", nameof(increment));

        var effectiveMaxDelay = maxDelay ?? TimeSpan.FromMinutes(1);

        if (effectiveMaxDelay < baseDelay)
            throw new ArgumentException("MaxDelay must be greater than or equal to BaseDelay.", nameof(maxDelay));

        return attemptNumber =>
        {
            // Handle invalid input
            if (attemptNumber < 0)
                return TimeSpan.Zero;

            try
            {
                var incrementTicks = checked(effectiveIncrement.Ticks * attemptNumber);
                var totalTicks = checked(baseDelay.Ticks + incrementTicks);

                if (totalTicks > effectiveMaxDelay.Ticks)
                    return effectiveMaxDelay;

                return TimeSpan.FromTicks(totalTicks);
            }
            catch (OverflowException)
            {
                return effectiveMaxDelay;
            }
        };
    }

    /// <summary>
    ///     Creates a fixed delay strategy.
    /// </summary>
    /// <param name="delay">The fixed delay for all retry attempts.</param>
    /// <returns>A backoff strategy delegate that implements fixed delay.</returns>
    /// <exception cref="ArgumentException">Thrown when delay is not positive.</exception>
    /// <remarks>
    ///     Fixed delay strategy uses the same delay for all retry attempts.
    ///     Returns the configured delay for all non-negative attempt numbers.
    ///     Returns TimeSpan.Zero for negative attempt numbers.
    /// </remarks>
    public static BackoffStrategy FixedDelay(TimeSpan delay)
    {
        if (delay <= TimeSpan.Zero)
            throw new ArgumentException("Delay must be a positive TimeSpan.", nameof(delay));

        return attemptNumber =>
        {
            // Handle invalid input
            if (attemptNumber < 0)
                return TimeSpan.Zero;

            return delay;
        };
    }
}

/// <summary>
///     Represents a backoff strategy that calculates delay based on attempt number.
/// </summary>
/// <param name="attemptNumber">The current attempt number (0-based, where 0 is the first retry).</param>
/// <returns>The calculated delay time span.</returns>
/// <remarks>
///     This delegate type represents a function that calculates retry delays.
///     It replaces the IBackoffStrategy interface with a simpler function-based approach.
/// </remarks>
public delegate TimeSpan BackoffStrategy(int attemptNumber);

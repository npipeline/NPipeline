namespace NPipeline.Execution.RetryDelay.Jitter;

/// <summary>
///     Implements no jitter strategy for retry delays.
/// </summary>
/// <remarks>
///     <para>
///         This strategy simply returns the base delay without any randomization.
///         It is useful for testing or when deterministic behavior is needed.
///     </para>
///     <para>
///         The formula used is: jitteredDelay = baseDelay
///     </para>
/// </remarks>
public sealed class NoJitterStrategy : IJitterStrategy
{
    private readonly NoJitterConfiguration _configuration;

    /// <summary>
    ///     Initializes a new instance of the <see cref="NoJitterStrategy" /> class.
    /// </summary>
    /// <param name="configuration">The configuration for no jitter.</param>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <exception cref="ArgumentException">Thrown when configuration is invalid.</exception>
    public NoJitterStrategy(NoJitterConfiguration configuration)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.Validate();
    }

    /// <summary>
    ///     Applies no jitter to a base delay.
    /// </summary>
    /// <param name="baseDelay">The base delay time span to apply jitter to.</param>
    /// <param name="random">The random instance to use for jitter calculations.</param>
    /// <returns>The jittered delay time span (same as base delay).</returns>
    /// <exception cref="ArgumentNullException">Thrown when random is null.</exception>
    /// <remarks>
    ///     Simply returns the base delay without any randomization.
    ///     Returns TimeSpan.Zero for negative or zero base delays.
    ///     The random parameter is not used but is required by the interface.
    /// </remarks>
    public TimeSpan ApplyJitter(TimeSpan baseDelay, Random random)
    {
        ArgumentNullException.ThrowIfNull(random);

        // Handle invalid input
        if (baseDelay <= TimeSpan.Zero)
            return TimeSpan.Zero;

        // No jitter - return the base delay as-is
        return baseDelay;
    }
}

namespace NPipeline.Configuration;

/// <summary>
///     Configuration options for circuit breaker memory management and cleanup.
///     Controls how inactive circuit breakers are automatically removed to prevent memory leaks.
/// </summary>
/// <param name="CleanupInterval">How often to run the cleanup process. Default: 5 minutes.</param>
/// <param name="InactivityThreshold">How long a circuit breaker can be inactive before being removed. Default: 30 minutes.</param>
/// <param name="EnableAutomaticCleanup">Whether automatic cleanup is enabled. Default: true.</param>
/// <param name="MaxTrackedCircuitBreakers">Maximum number of circuit breakers to track. Default: 1000.</param>
public sealed record CircuitBreakerMemoryManagementOptions(
    TimeSpan CleanupInterval = default,
    TimeSpan InactivityThreshold = default,
    bool EnableAutomaticCleanup = true,
    int MaxTrackedCircuitBreakers = 1000)
{
    /// <summary>
    ///     Default instance with sensible defaults.
    /// </summary>
    public static CircuitBreakerMemoryManagementOptions Default { get; } = new(
        TimeSpan.FromMinutes(5),
        TimeSpan.FromMinutes(30));

    /// <summary>
    ///     Disabled instance with automatic cleanup turned off.
    /// </summary>
    public static CircuitBreakerMemoryManagementOptions Disabled { get; } = new(
        TimeSpan.FromHours(1),
        TimeSpan.FromHours(24),
        false,
        int.MaxValue);

    /// <summary>
    ///     Gets the effective cleanup interval, using default if not specified.
    /// </summary>
    public TimeSpan EffectiveCleanupInterval => CleanupInterval == default
        ? TimeSpan.FromMinutes(5)
        : CleanupInterval;

    /// <summary>
    ///     Gets the effective inactivity threshold, using default if not specified.
    /// </summary>
    public TimeSpan EffectiveInactivityThreshold => InactivityThreshold == default
        ? TimeSpan.FromMinutes(30)
        : InactivityThreshold;

    /// <summary>
    ///     Validates the configuration options.
    /// </summary>
    /// <returns>The validated options.</returns>
    public CircuitBreakerMemoryManagementOptions Validate()
    {
        var cleanupInterval = EffectiveCleanupInterval;
        var inactivityThreshold = EffectiveInactivityThreshold;

        if (cleanupInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(CleanupInterval), "CleanupInterval must be greater than zero");

        if (inactivityThreshold <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(InactivityThreshold), "InactivityThreshold must be greater than zero");

        if (MaxTrackedCircuitBreakers <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxTrackedCircuitBreakers), "MaxTrackedCircuitBreakers must be greater than zero");

        if (cleanupInterval > inactivityThreshold)
            throw new ArgumentException("CleanupInterval should not be greater than InactivityThreshold", nameof(CleanupInterval));

        return this;
    }
}

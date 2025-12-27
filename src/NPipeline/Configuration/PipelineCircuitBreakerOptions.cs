namespace NPipeline.Configuration;

/// <summary>
///     Options controlling a simple pipeline-level circuit breaker applied to transformation / join restart attempts.
///     When the number of consecutive failed restart attempts for a node reaches <see cref="FailureThreshold" />,
///     the breaker opens and aborts the pipeline early to prevent cascading waste.
/// </summary>
/// <param name="FailureThreshold">Number of consecutive failures (after restart decisions) before tripping the breaker. Must be >= 1.</param>
/// <param name="OpenDuration">How long the breaker remains open before transitioning to Half-Open state.</param>
/// <param name="SamplingWindow">Time window for rolling window failure tracking.</param>
/// <param name="Enabled">Whether the circuit breaker is active.</param>
/// <param name="ThresholdType">Type of failure threshold to use for circuit breaking.</param>
/// <param name="FailureRateThreshold">Failure rate threshold (0.0-1.0) when using RollingWindowRate or Hybrid threshold types.</param>
/// <param name="HalfOpenSuccessThreshold">Number of consecutive successes required in Half-Open state to transition to Closed.</param>
/// <param name="HalfOpenMaxAttempts">Maximum number of operation attempts allowed in Half-Open state.</param>
/// <param name="TrackOperationsInWindow">Whether to track operations in the rolling window for statistics.</param>
public sealed record PipelineCircuitBreakerOptions(
    int FailureThreshold,
    TimeSpan OpenDuration,
    TimeSpan SamplingWindow,
    bool Enabled = true,
    CircuitBreakerThresholdType ThresholdType = CircuitBreakerThresholdType.ConsecutiveFailures,
    double FailureRateThreshold = 0.5,
    int HalfOpenSuccessThreshold = 1,
    int HalfOpenMaxAttempts = 5,
    bool TrackOperationsInWindow = true)
{
    /// <summary>
    ///     Gets a disabled circuit breaker configuration that effectively disables the circuit breaker functionality.
    /// </summary>
    public static PipelineCircuitBreakerOptions Disabled { get; } = new(int.MaxValue, TimeSpan.Zero, TimeSpan.Zero, false, TrackOperationsInWindow: false);

    /// <summary>
    ///     Gets the default circuit breaker configuration with sensible default values.
    /// </summary>
    public static PipelineCircuitBreakerOptions Default { get; } = new(5, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(5));


    /// <summary>
    ///     Validates the circuit breaker configuration and throws exceptions for invalid settings.
    /// </summary>
    /// <returns>The validated configuration instance.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when any configuration parameter is out of valid range.</exception>
    /// <exception cref="InvalidOperationException">Thrown when configuration settings are incompatible.</exception>
    public PipelineCircuitBreakerOptions Validate()
    {
        if (!Enabled)
            return this;

        if (FailureThreshold < 1)
            throw new ArgumentOutOfRangeException(nameof(FailureThreshold), "FailureThreshold must be >= 1");

        if (FailureRateThreshold is < 0.0 or > 1.0)
            throw new ArgumentOutOfRangeException(nameof(FailureRateThreshold), "FailureRateThreshold must be between 0.0 and 1.0");

        if (SamplingWindow < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(SamplingWindow), "SamplingWindow cannot be negative");

        if (TrackOperationsInWindow && SamplingWindow == TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(SamplingWindow), "SamplingWindow must be greater than zero when tracking operations");

        if (HalfOpenSuccessThreshold < 1)
            throw new ArgumentOutOfRangeException(nameof(HalfOpenSuccessThreshold), "HalfOpenSuccessThreshold must be >= 1");

        if (HalfOpenMaxAttempts < 1)
            throw new ArgumentOutOfRangeException(nameof(HalfOpenMaxAttempts), "HalfOpenMaxAttempts must be >= 1");

        if (HalfOpenSuccessThreshold > HalfOpenMaxAttempts)
        {
            throw new ArgumentOutOfRangeException(nameof(HalfOpenSuccessThreshold),
                $"HalfOpenSuccessThreshold ({HalfOpenSuccessThreshold}) cannot be greater than HalfOpenMaxAttempts ({HalfOpenMaxAttempts})");
        }

        if (!TrackOperationsInWindow && ThresholdType != CircuitBreakerThresholdType.ConsecutiveFailures)
            throw new InvalidOperationException("TrackOperationsInWindow must be enabled for rolling window or rate-based thresholds.");

        return this;
    }
}

/// <summary>
///     Defines the type of failure threshold used for circuit breaking.
/// </summary>
public enum CircuitBreakerThresholdType
{
    /// <summary>
    ///     Uses consecutive failure count (current behavior).
    /// </summary>
    ConsecutiveFailures,

    /// <summary>
    ///     Uses failure count within the sampling window.
    /// </summary>
    RollingWindowCount,

    /// <summary>
    ///     Uses failure rate within the sampling window.
    /// </summary>
    RollingWindowRate,

    /// <summary>
    ///     Uses both count and rate thresholds.
    /// </summary>
    Hybrid,
}

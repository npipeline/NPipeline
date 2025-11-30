using NPipeline.Configuration.RetryDelay;

namespace NPipeline.Execution.RetryDelay;

/// <summary>
///     Defines the contract for creating retry delay strategies.
/// </summary>
/// <remarks>
///     <para>
///         This factory provides a centralized way to create retry delay strategies
///         using delegates directly instead of complex configuration classes.
///         It supports creating various backoff strategies and combining them with jitter strategies.
///     </para>
///     <para>
///         The factory pattern ensures consistent configuration across the pipeline
///         and provides a single point of control for retry behavior.
///     </para>
/// </remarks>
public interface IRetryDelayStrategyFactory
{
    /// <summary>
    ///     Creates a retry delay strategy from configuration.
    /// </summary>
    /// <param name="configuration">The retry delay strategy configuration.</param>
    /// <returns>A configured retry delay strategy.</returns>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <exception cref="ArgumentException">Thrown when configuration is invalid.</exception>
    IRetryDelayStrategy CreateStrategy(RetryDelayStrategyConfiguration configuration);

    /// <summary>
    ///     Creates an exponential backoff strategy with specified parameters.
    /// </summary>
    /// <param name="baseDelay">The base delay for the first retry attempt.</param>
    /// <param name="multiplier">The multiplier applied to delay for each subsequent retry.</param>
    /// <param name="maxDelay">The maximum delay to prevent exponential growth.</param>
    /// <param name="jitterStrategy">Optional jitter strategy to apply.</param>
    /// <returns>A configured retry delay strategy.</returns>
    /// <exception cref="ArgumentException">Thrown when any parameter is invalid.</exception>
    IRetryDelayStrategy CreateExponentialBackoff(
        TimeSpan baseDelay,
        double multiplier = 2.0,
        TimeSpan? maxDelay = null,
        JitterStrategy? jitterStrategy = null);

    /// <summary>
    ///     Creates a linear backoff strategy with specified parameters.
    /// </summary>
    /// <param name="baseDelay">The base delay for the first retry attempt.</param>
    /// <param name="increment">The increment added to the delay for each subsequent retry.</param>
    /// <param name="maxDelay">The maximum delay to prevent linear growth.</param>
    /// <param name="jitterStrategy">Optional jitter strategy to apply.</param>
    /// <returns>A configured retry delay strategy.</returns>
    /// <exception cref="ArgumentException">Thrown when any parameter is invalid.</exception>
    IRetryDelayStrategy CreateLinearBackoff(
        TimeSpan baseDelay,
        TimeSpan? increment = null,
        TimeSpan? maxDelay = null,
        JitterStrategy? jitterStrategy = null);

    /// <summary>
    ///     Creates a fixed delay strategy with specified parameters.
    /// </summary>
    /// <param name="delay">The fixed delay for all retry attempts.</param>
    /// <param name="jitterStrategy">Optional jitter strategy to apply.</param>
    /// <returns>A configured retry delay strategy.</returns>
    /// <exception cref="ArgumentException">Thrown when delay is not positive.</exception>
    IRetryDelayStrategy CreateFixedDelay(
        TimeSpan delay,
        JitterStrategy? jitterStrategy = null);

    /// <summary>
    ///     Creates a no-operation retry delay strategy.
    /// </summary>
    /// <returns>A no-operation retry delay strategy.</returns>
    /// <remarks>
    ///     This strategy always returns TimeSpan.Zero and is useful for testing
    ///     or when delays should be handled elsewhere.
    /// </remarks>
    IRetryDelayStrategy CreateNoOp();

    /// <summary>
    ///     Creates a full jitter strategy.
    /// </summary>
    /// <returns>A jitter strategy that applies full jitter.</returns>
    /// <remarks>
    ///     This method creates a full jitter strategy.
    /// </remarks>
    JitterStrategy CreateFullJitter();

    /// <summary>
    ///     Creates a decorrelated jitter strategy with the specified parameters.
    /// </summary>
    /// <param name="maxDelay">The maximum delay to prevent excessive growth.</param>
    /// <param name="multiplier">The multiplier for the previous delay.</param>
    /// <returns>A jitter strategy that applies decorrelated jitter.</returns>
    /// <exception cref="ArgumentException">Thrown when maxDelay is not positive or multiplier is less than 1.0.</exception>
    /// <remarks>
    ///     This method creates a decorrelated jitter strategy.
    /// </remarks>
    JitterStrategy CreateDecorrelatedJitter(TimeSpan maxDelay, double multiplier = 3.0);

    /// <summary>
    ///     Creates an equal jitter strategy.
    /// </summary>
    /// <returns>A jitter strategy that applies equal jitter.</returns>
    /// <remarks>
    ///     This method creates an equal jitter strategy.
    /// </remarks>
    JitterStrategy CreateEqualJitter();

    /// <summary>
    ///     Creates a no jitter strategy.
    /// </summary>
    /// <returns>A jitter strategy that doesn't apply any jitter.</returns>
    /// <remarks>
    ///     This method creates a no jitter strategy.
    /// </remarks>
    JitterStrategy CreateNoJitter();
}

using NPipeline.Configuration.RetryDelay;
using NPipeline.Execution.RetryDelay.Jitter;
using DecorrelatedJitterConfiguration = NPipeline.Execution.RetryDelay.Jitter.DecorrelatedJitterConfiguration;
using EqualJitterConfiguration = NPipeline.Execution.RetryDelay.Jitter.EqualJitterConfiguration;
using FullJitterConfiguration = NPipeline.Execution.RetryDelay.Jitter.FullJitterConfiguration;
using NoJitterConfiguration = NPipeline.Execution.RetryDelay.Jitter.NoJitterConfiguration;

namespace NPipeline.Execution.RetryDelay;

/// <summary>
///     Defines the contract for creating retry delay strategies.
/// </summary>
/// <remarks>
///     <para>
///         This factory provides a centralized way to create retry delay strategies
///         with proper configuration and validation. It supports creating various
///         backoff strategies and combining them with jitter strategies.
///     </para>
///     <para>
///         The factory pattern ensures consistent configuration across the pipeline
///         and provides a single point of control for retry behavior.
///     </para>
/// </remarks>
public interface IRetryDelayStrategyFactory
{
    /// <summary>
    ///     Creates an exponential backoff strategy with specified configuration.
    /// </summary>
    /// <param name="configuration">The configuration for exponential backoff.</param>
    /// <param name="jitterStrategy">Optional jitter strategy to apply.</param>
    /// <returns>A configured retry delay strategy.</returns>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <exception cref="ArgumentException">Thrown when configuration is invalid.</exception>
    IRetryDelayStrategy CreateExponentialBackoff(
        ExponentialBackoffConfiguration configuration,
        IJitterStrategy? jitterStrategy = null);

    /// <summary>
    ///     Creates a linear backoff strategy with specified configuration.
    /// </summary>
    /// <param name="configuration">The configuration for linear backoff.</param>
    /// <param name="jitterStrategy">Optional jitter strategy to apply.</param>
    /// <returns>A configured retry delay strategy.</returns>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <exception cref="ArgumentException">Thrown when configuration is invalid.</exception>
    IRetryDelayStrategy CreateLinearBackoff(
        LinearBackoffConfiguration configuration,
        IJitterStrategy? jitterStrategy = null);

    /// <summary>
    ///     Creates a fixed delay strategy with specified configuration.
    /// </summary>
    /// <param name="configuration">The configuration for fixed delay.</param>
    /// <param name="jitterStrategy">Optional jitter strategy to apply.</param>
    /// <returns>A configured retry delay strategy.</returns>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <exception cref="ArgumentException">Thrown when configuration is invalid.</exception>
    IRetryDelayStrategy CreateFixedDelay(
        FixedDelayConfiguration configuration,
        IJitterStrategy? jitterStrategy = null);

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
    ///     Creates a full jitter strategy with the specified configuration.
    /// </summary>
    /// <param name="configuration">The configuration for full jitter.</param>
    /// <returns>A configured full jitter strategy.</returns>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <exception cref="ArgumentException">Thrown when configuration is invalid.</exception>
    IJitterStrategy CreateFullJitter(FullJitterConfiguration configuration);

    /// <summary>
    ///     Creates a decorrelated jitter strategy with the specified configuration.
    /// </summary>
    /// <param name="configuration">The configuration for decorrelated jitter.</param>
    /// <returns>A configured decorrelated jitter strategy.</returns>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <exception cref="ArgumentException">Thrown when configuration is invalid.</exception>
    IJitterStrategy CreateDecorrelatedJitter(DecorrelatedJitterConfiguration configuration);

    /// <summary>
    ///     Creates an equal jitter strategy with the specified configuration.
    /// </summary>
    /// <param name="configuration">The configuration for equal jitter.</param>
    /// <returns>A configured equal jitter strategy.</returns>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <exception cref="ArgumentException">Thrown when configuration is invalid.</exception>
    IJitterStrategy CreateEqualJitter(EqualJitterConfiguration configuration);

    /// <summary>
    ///     Creates a no jitter strategy with the specified configuration.
    /// </summary>
    /// <param name="configuration">The configuration for no jitter.</param>
    /// <returns>A configured no jitter strategy.</returns>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <exception cref="ArgumentException">Thrown when configuration is invalid.</exception>
    IJitterStrategy CreateNoJitter(NoJitterConfiguration configuration);

    /// <summary>
    ///     Creates a retry delay strategy from configuration.
    /// </summary>
    /// <param name="configuration">The retry delay strategy configuration.</param>
    /// <returns>A configured retry delay strategy.</returns>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <exception cref="ArgumentException">Thrown when configuration is invalid.</exception>
    IRetryDelayStrategy CreateStrategy(RetryDelayStrategyConfiguration configuration);
}

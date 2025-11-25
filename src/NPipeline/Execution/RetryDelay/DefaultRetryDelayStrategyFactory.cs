using NPipeline.Configuration.RetryDelay;
using NPipeline.Execution.RetryDelay.Backoff;
using NPipeline.Execution.RetryDelay.Jitter;
using DecorrelatedJitterConfiguration = NPipeline.Execution.RetryDelay.Jitter.DecorrelatedJitterConfiguration;
using EqualJitterConfiguration = NPipeline.Execution.RetryDelay.Jitter.EqualJitterConfiguration;
using ExponentialBackoffConfiguration = NPipeline.Configuration.RetryDelay.ExponentialBackoffConfiguration;
using FixedDelayConfiguration = NPipeline.Configuration.RetryDelay.FixedDelayConfiguration;
using FullJitterConfiguration = NPipeline.Execution.RetryDelay.Jitter.FullJitterConfiguration;
using LinearBackoffConfiguration = NPipeline.Configuration.RetryDelay.LinearBackoffConfiguration;
using NoJitterConfiguration = NPipeline.Execution.RetryDelay.Jitter.NoJitterConfiguration;

namespace NPipeline.Execution.RetryDelay;

/// <summary>
///     Default implementation of IRetryDelayStrategyFactory that creates retry delay strategies.
/// </summary>
/// <remarks>
///     <para>
///         This factory creates retry delay strategies based on configuration objects.
///         It supports both backoff strategies (which determine how delays increase over time)
///         and jitter strategies (which add randomness to prevent thundering herd problems).
///     </para>
///     <para>
///         The factory follows a strategy pattern where each configuration type maps to
///         a specific strategy implementation. This makes it easy to add new strategies
///         without modifying existing code.
///     </para>
/// </remarks>
public class DefaultRetryDelayStrategyFactory : IRetryDelayStrategyFactory
{
    private readonly Random _random;

    public DefaultRetryDelayStrategyFactory(Random? random = null)
    {
        _random = random ?? Random.Shared;
    }

    /// <summary>
    ///     Creates a retry delay strategy from the specified configuration.
    /// </summary>
    /// <param name="configuration">The configuration for the retry delay strategy.</param>
    /// <returns>A retry delay strategy implementation.</returns>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <exception cref="ArgumentException">Thrown when configuration is invalid.</exception>
    /// <exception cref="NotSupportedException">Thrown when the strategy type is not supported.</exception>
    /// <remarks>
    ///     <para>
    ///         This method creates a retry delay strategy by combining a backoff strategy
    ///         with a jitter strategy. The backoff strategy determines how delays increase
    ///         over time, while the jitter strategy adds randomness to prevent thundering herd problems.
    ///     </para>
    ///     <para>
    ///         The method validates the configuration before creating the strategy and throws
    ///         appropriate exceptions for invalid configurations.
    ///     </para>
    /// </remarks>
    public IRetryDelayStrategy CreateStrategy(RetryDelayStrategyConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        // Validate the configuration
        configuration.Validate();

        // Create the backoff strategy
        var backoffStrategy = CreateBackoffStrategy(configuration.BackoffConfiguration);

        // Create the jitter strategy
        var jitterStrategy = CreateJitterStrategy(configuration.JitterConfiguration);

        // Combine them into a retry delay strategy
        return new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, _random);
    }

    /// <summary>
    ///     Creates an exponential backoff strategy with the specified parameters.
    /// </summary>
    /// <param name="configuration">The exponential backoff configuration.</param>
    /// <param name="jitterStrategy">Optional jitter strategy to apply.</param>
    /// <returns>A retry delay strategy that uses exponential backoff.</returns>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <exception cref="ArgumentException">Thrown when configuration is invalid.</exception>
    /// <remarks>
    ///     This method creates an exponential backoff strategy with optional jitter.
    /// </remarks>
    public IRetryDelayStrategy CreateExponentialBackoff(
        ExponentialBackoffConfiguration configuration,
        IJitterStrategy? jitterStrategy = null)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        configuration.Validate();

        var backoffStrategy = new ExponentialBackoffStrategy(new Backoff.ExponentialBackoffConfiguration
        {
            BaseDelay = configuration.BaseDelay,
            Multiplier = configuration.Multiplier,
            MaxDelay = configuration.MaxDelay,
        });

        return new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy ?? new NoJitterStrategy(new NoJitterConfiguration()), _random);
    }

    /// <summary>
    ///     Creates a linear backoff strategy with the specified parameters.
    /// </summary>
    /// <param name="configuration">The linear backoff configuration.</param>
    /// <param name="jitterStrategy">Optional jitter strategy to apply.</param>
    /// <returns>A retry delay strategy that uses linear backoff.</returns>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <exception cref="ArgumentException">Thrown when configuration is invalid.</exception>
    /// <remarks>
    ///     This method creates a linear backoff strategy with optional jitter.
    /// </remarks>
    public IRetryDelayStrategy CreateLinearBackoff(
        LinearBackoffConfiguration configuration,
        IJitterStrategy? jitterStrategy = null)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        configuration.Validate();

        var backoffStrategy = new LinearBackoffStrategy(new Backoff.LinearBackoffConfiguration
        {
            BaseDelay = configuration.BaseDelay,
            Increment = configuration.Increment,
            MaxDelay = configuration.MaxDelay,
        });

        return new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy ?? new NoJitterStrategy(new NoJitterConfiguration()), _random);
    }

    /// <summary>
    ///     Creates a fixed delay strategy with the specified parameters.
    /// </summary>
    /// <param name="configuration">The fixed delay configuration.</param>
    /// <param name="jitterStrategy">Optional jitter strategy to apply.</param>
    /// <returns>A retry delay strategy that uses fixed delay.</returns>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <exception cref="ArgumentException">Thrown when configuration is invalid.</exception>
    /// <remarks>
    ///     This method creates a fixed delay strategy with optional jitter.
    /// </remarks>
    public IRetryDelayStrategy CreateFixedDelay(
        FixedDelayConfiguration configuration,
        IJitterStrategy? jitterStrategy = null)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        configuration.Validate();

        var backoffStrategy = new FixedDelayStrategy(new Backoff.FixedDelayConfiguration
        {
            Delay = configuration.Delay,
        });

        return new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy ?? new NoJitterStrategy(new NoJitterConfiguration()), _random);
    }

    /// <summary>
    ///     Creates a no-operation retry delay strategy.
    /// </summary>
    /// <returns>A retry delay strategy that doesn't apply any delay.</returns>
    /// <remarks>
    ///     This method creates a no-op strategy that returns TimeSpan.Zero for all attempts.
    /// </remarks>
    public IRetryDelayStrategy CreateNoOp()
    {
        return NoOpRetryDelayStrategy.Instance;
    }

    /// <summary>
    ///     Creates a full jitter strategy.
    /// </summary>
    /// <param name="configuration">The full jitter configuration.</param>
    /// <returns>A jitter strategy that applies full jitter.</returns>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <remarks>
    ///     This method creates a full jitter strategy.
    /// </remarks>
    public IJitterStrategy CreateFullJitter(FullJitterConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        configuration.Validate();
        return new FullJitterStrategy(configuration);
    }

    /// <summary>
    ///     Creates a decorrelated jitter strategy.
    /// </summary>
    /// <param name="configuration">The decorrelated jitter configuration.</param>
    /// <returns>A jitter strategy that applies decorrelated jitter.</returns>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <remarks>
    ///     This method creates a decorrelated jitter strategy.
    /// </remarks>
    public IJitterStrategy CreateDecorrelatedJitter(DecorrelatedJitterConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        configuration.Validate();
        return new DecorrelatedJitterStrategy(configuration);
    }

    /// <summary>
    ///     Creates an equal jitter strategy.
    /// </summary>
    /// <param name="configuration">The equal jitter configuration.</param>
    /// <returns>A jitter strategy that applies equal jitter.</returns>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <remarks>
    ///     This method creates an equal jitter strategy.
    /// </remarks>
    public IJitterStrategy CreateEqualJitter(EqualJitterConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        configuration.Validate();
        return new EqualJitterStrategy(configuration);
    }

    /// <summary>
    ///     Creates a no jitter strategy.
    /// </summary>
    /// <param name="configuration">The no jitter configuration.</param>
    /// <returns>A jitter strategy that doesn't apply any jitter.</returns>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <remarks>
    ///     This method creates a no jitter strategy.
    /// </remarks>
    public IJitterStrategy CreateNoJitter(NoJitterConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        configuration.Validate();
        return new NoJitterStrategy(configuration);
    }

    /// <summary>
    ///     Creates a backoff strategy from the specified configuration.
    /// </summary>
    /// <param name="configuration">The backoff strategy configuration.</param>
    /// <returns>A backoff strategy implementation.</returns>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <exception cref="NotSupportedException">Thrown when the backoff strategy type is not supported.</exception>
    /// <remarks>
    ///     This method uses a switch expression to map configuration types to strategy implementations,
    ///     making it easy to add new backoff strategies in the future.
    /// </remarks>
    private static IBackoffStrategy CreateBackoffStrategy(BackoffStrategyConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        return configuration switch
        {
            ExponentialBackoffConfiguration expConfig =>
                new ExponentialBackoffStrategy(new Backoff.ExponentialBackoffConfiguration
                {
                    BaseDelay = expConfig.BaseDelay,
                    Multiplier = expConfig.Multiplier,
                    MaxDelay = expConfig.MaxDelay,
                }),

            LinearBackoffConfiguration linearConfig =>
                new LinearBackoffStrategy(new Backoff.LinearBackoffConfiguration
                {
                    BaseDelay = linearConfig.BaseDelay,
                    Increment = linearConfig.Increment,
                    MaxDelay = linearConfig.MaxDelay,
                }),

            FixedDelayConfiguration fixedConfig =>
                new FixedDelayStrategy(new Backoff.FixedDelayConfiguration
                {
                    Delay = fixedConfig.Delay,
                }),

            _ => throw new NotSupportedException($"Backoff strategy type '{configuration.StrategyType}' is not supported."),
        };
    }

    /// <summary>
    ///     Creates a jitter strategy from the specified configuration.
    /// </summary>
    /// <param name="configuration">The jitter strategy configuration.</param>
    /// <returns>A jitter strategy implementation.</returns>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <exception cref="NotSupportedException">Thrown when the jitter strategy type is not supported.</exception>
    /// <remarks>
    ///     This method uses a switch expression to map configuration types to strategy implementations,
    ///     making it easy to add new jitter strategies in the future.
    /// </remarks>
    private static IJitterStrategy CreateJitterStrategy(JitterStrategyConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        return configuration switch
        {
            Configuration.RetryDelay.NoJitterConfiguration =>
                new NoJitterStrategy(new NoJitterConfiguration()),

            Configuration.RetryDelay.FullJitterConfiguration =>
                new FullJitterStrategy(new FullJitterConfiguration()),

            Configuration.RetryDelay.EqualJitterConfiguration =>
                new EqualJitterStrategy(new EqualJitterConfiguration()),

            Configuration.RetryDelay.DecorrelatedJitterConfiguration decorrelatedConfig =>
                new DecorrelatedJitterStrategy(new DecorrelatedJitterConfiguration
                {
                    MaxDelay = decorrelatedConfig.MaxDelay,
                    Multiplier = decorrelatedConfig.Multiplier,
                }),

            _ => throw new NotSupportedException($"Jitter strategy type '{configuration.StrategyType}' is not supported."),
        };
    }
}

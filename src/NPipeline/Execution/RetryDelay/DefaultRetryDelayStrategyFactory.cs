using NPipeline.Configuration.RetryDelay;

namespace NPipeline.Execution.RetryDelay;

/// <summary>
///     Default implementation of retry delay strategy factory.
/// </summary>
/// <remarks>
///     <para>
///         This factory creates retry delay strategies based on configuration objects.
///         It supports all built-in backoff and jitter strategies and can be
///         extended with custom implementations through the BackoffStrategy and
///         JitterStrategy delegates.
///     </para>
///     <para>
///         The factory follows a consistent pattern:
///         <list type="number">
///             <item>
///                 <description>Create backoff strategy from BackoffStrategyConfiguration</description>
///             </item>
///             <item>
///                 <description>Create jitter strategy from JitterStrategyConfiguration</description>
///             </item>
///             <item>
///                 <description>Combine into CompositeRetryDelayStrategy if both are present</description>
///             </item>
///             <item>
///                 <description>Return single strategy if only one is present</description>
///             </item>
///         </list>
///     </para>
/// </remarks>
public sealed class DefaultRetryDelayStrategyFactory(Random? random = null) : IRetryDelayStrategyFactory
{
    private readonly Random _random = random ?? Random.Shared;

    /// <inheritdoc />
    public IRetryDelayStrategy CreateStrategy(RetryDelayStrategyConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        // Create backoff strategy
        var backoffStrategy = CreateBackoffStrategy(configuration.BackoffConfiguration);

        // Create jitter strategy
        var jitterStrategy = CreateJitterStrategy(configuration.JitterConfiguration);

        // Combine strategies
        return new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, _random);
    }

    /// <inheritdoc />
    public IRetryDelayStrategy CreateExponentialBackoff(
        ExponentialBackoffConfiguration configuration,
        JitterStrategy? jitterStrategy = null)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var backoffStrategy = BackoffStrategies.ExponentialBackoff(
            configuration.BaseDelay,
            configuration.Multiplier,
            configuration.MaxDelay);

        return jitterStrategy is not null
            ? new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, _random)
            : new CompositeRetryDelayStrategy(backoffStrategy, null, _random);
    }

    /// <inheritdoc />
    public IRetryDelayStrategy CreateLinearBackoff(
        LinearBackoffConfiguration configuration,
        JitterStrategy? jitterStrategy = null)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var backoffStrategy = BackoffStrategies.LinearBackoff(
            configuration.BaseDelay,
            configuration.Increment,
            configuration.MaxDelay);

        return jitterStrategy is not null
            ? new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, _random)
            : new CompositeRetryDelayStrategy(backoffStrategy, null, _random);
    }

    /// <inheritdoc />
    public IRetryDelayStrategy CreateFixedDelay(
        FixedDelayConfiguration configuration,
        JitterStrategy? jitterStrategy = null)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var backoffStrategy = BackoffStrategies.FixedDelay(configuration.Delay);

        return jitterStrategy is not null
            ? new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, _random)
            : new CompositeRetryDelayStrategy(backoffStrategy, null, _random);
    }

    /// <inheritdoc />
    public IRetryDelayStrategy CreateNoOp()
    {
        return NoOpRetryDelayStrategy.Instance;
    }

    /// <inheritdoc />
    public JitterStrategy CreateFullJitter()
    {
        return JitterStrategies.FullJitter();
    }

    /// <inheritdoc />
    public JitterStrategy CreateDecorrelatedJitter(TimeSpan maxDelay, double multiplier = 3.0)
    {
        return JitterStrategies.DecorrelatedJitter(maxDelay, multiplier);
    }

    /// <inheritdoc />
    public JitterStrategy CreateEqualJitter()
    {
        return JitterStrategies.EqualJitter();
    }

    /// <inheritdoc />
    public JitterStrategy CreateNoJitter()
    {
        return JitterStrategies.NoJitter();
    }

    /// <inheritdoc />
    public BackoffStrategy CreateBackoffStrategy(BackoffStrategyConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        return configuration switch
        {
            ExponentialBackoffConfiguration exponentialConfig =>
                BackoffStrategies.ExponentialBackoff(
                    exponentialConfig.BaseDelay,
                    exponentialConfig.Multiplier,
                    exponentialConfig.MaxDelay),
            LinearBackoffConfiguration linearConfig =>
                BackoffStrategies.LinearBackoff(
                    linearConfig.BaseDelay,
                    linearConfig.Increment,
                    linearConfig.MaxDelay),
            FixedDelayConfiguration fixedConfig =>
                BackoffStrategies.FixedDelay(fixedConfig.Delay),
            _ => throw new ArgumentException($"Unknown backoff strategy type: {configuration.GetType().Name}"),
        };
    }

    /// <inheritdoc />
    public JitterStrategy CreateJitterStrategy(JitterStrategyConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        return configuration switch
        {
            DelegateJitterStrategyConfiguration delegateConfig => delegateConfig.JitterStrategy,
            DecorrelatedJitterConfiguration decorrelatedConfig => JitterStrategies.DecorrelatedJitter(decorrelatedConfig.MaxDelay,
                decorrelatedConfig.Multiplier),
            FullJitterConfiguration => JitterStrategies.FullJitter(),
            EqualJitterConfiguration => JitterStrategies.EqualJitter(),
            NoJitterConfiguration => JitterStrategies.NoJitter(),
            _ => throw new ArgumentException($"Unknown jitter strategy configuration type: {configuration.GetType().Name}"),
        };
    }
}

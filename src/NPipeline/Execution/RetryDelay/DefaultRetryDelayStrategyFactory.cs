using NPipeline.Configuration.RetryDelay;

namespace NPipeline.Execution.RetryDelay;

/// <summary>
///     Simplified implementation of retry delay strategy factory.
/// </summary>
/// <remarks>
///     <para>
///         This factory creates retry delay strategies based on the simplified
///         RetryDelayStrategyConfiguration that uses delegates directly.
///         It eliminates the need for complex configuration class parsing
///         while maintaining all functionality.
///     </para>
///     <para>
///         The factory follows a simple pattern:
///         <list type="number">
///             <item>
///                 <description>Create CompositeRetryDelayStrategy from backoff and jitter delegates</description>
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

        // Create backoff strategy from delegate
        var backoffStrategy = configuration.BackoffStrategy;

        // Get jitter strategy delegate if provided
        var jitterStrategy = configuration.JitterStrategy;

        // Combine into CompositeRetryDelayStrategy
        return new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, _random);
    }

    /// <inheritdoc />
    public IRetryDelayStrategy CreateExponentialBackoff(
        TimeSpan baseDelay,
        double multiplier = 2.0,
        TimeSpan? maxDelay = null,
        JitterStrategy? jitterStrategy = null)
    {
        var backoffStrategy = BackoffStrategies.ExponentialBackoff(baseDelay, multiplier, maxDelay);

        return jitterStrategy is not null
            ? new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, _random)
            : new CompositeRetryDelayStrategy(backoffStrategy, null, _random);
    }

    /// <inheritdoc />
    public IRetryDelayStrategy CreateLinearBackoff(
        TimeSpan baseDelay,
        TimeSpan? increment = null,
        TimeSpan? maxDelay = null,
        JitterStrategy? jitterStrategy = null)
    {
        var backoffStrategy = BackoffStrategies.LinearBackoff(baseDelay, increment, maxDelay);

        return jitterStrategy is not null
            ? new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, _random)
            : new CompositeRetryDelayStrategy(backoffStrategy, null, _random);
    }

    /// <inheritdoc />
    public IRetryDelayStrategy CreateFixedDelay(
        TimeSpan delay,
        JitterStrategy? jitterStrategy = null)
    {
        var backoffStrategy = BackoffStrategies.FixedDelay(delay);

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

    // Legacy methods for backward compatibility - marked as obsolete
    [Obsolete("Use CreateStrategy(RetryDelayStrategyConfiguration) instead")]
    public IRetryDelayStrategy CreateExponentialBackoff(
        object configuration,
        JitterStrategy? jitterStrategy = null)
    {
        throw new NotSupportedException("This method is obsolete. Use the new delegate-based API instead.");
    }

    [Obsolete("Use CreateStrategy(RetryDelayStrategyConfiguration) instead")]
    public IRetryDelayStrategy CreateLinearBackoff(
        object configuration,
        JitterStrategy? jitterStrategy = null)
    {
        throw new NotSupportedException("This method is obsolete. Use the new delegate-based API instead.");
    }

    [Obsolete("Use CreateStrategy(RetryDelayStrategyConfiguration) instead")]
    public IRetryDelayStrategy CreateFixedDelay(
        object configuration,
        JitterStrategy? jitterStrategy = null)
    {
        throw new NotSupportedException("This method is obsolete. Use the new delegate-based API instead.");
    }

    [Obsolete("Configuration classes are no longer supported")]
    public BackoffStrategy CreateBackoffStrategy(object configuration)
    {
        throw new NotSupportedException("Configuration classes are no longer supported. Use BackoffStrategies static methods directly.");
    }

    [Obsolete("Configuration classes are no longer supported")]
    public JitterStrategy CreateJitterStrategy(object configuration)
    {
        throw new NotSupportedException("Configuration classes are no longer supported. Use JitterStrategies static methods directly.");
    }
}

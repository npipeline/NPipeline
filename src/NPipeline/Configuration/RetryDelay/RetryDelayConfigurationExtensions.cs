using NPipeline.Execution.RetryDelay;

namespace NPipeline.Configuration.RetryDelay;

/// <summary>
///     Simple wrapper configuration for JitterStrategy delegates.
/// </summary>
public sealed record DelegateJitterStrategyConfiguration(JitterStrategy JitterStrategy) : JitterStrategyConfiguration
{
    /// <inheritdoc />
    public override string StrategyType => "Delegate";

    /// <inheritdoc />
    public override void Validate()
    {
        if (JitterStrategy is null)
            throw new ArgumentNullException(nameof(JitterStrategy), "JitterStrategy cannot be null.");
    }
}

/// <summary>
///     Extension methods for configuring retry delay strategies on PipelineRetryOptions.
/// </summary>
/// <remarks>
///     <para>
///         These extensions provide a fluent API for configuring retry delay strategies
///         with backoff and jitter combinations. They make it easy to set up common
///         retry patterns while maintaining backward compatibility with existing code.
///     </para>
///     <para>
///         Example usage:
///         <code>
///             var retryOptions = PipelineRetryOptions.Default
///                 .WithDelayStrategy(
///                     new ExponentialBackoffConfiguration(
///                         baseDelay: TimeSpan.FromSeconds(1),
///                         multiplier: 2.0,
///                         maxDelay: TimeSpan.FromMinutes(1)),
///                     JitterStrategies.FullJitter())
///                 .WithExponentialBackoffAndFullJitter();
///         </code>
///     </para>
/// </remarks>
public static class RetryDelayConfigurationExtensions
{
    /// <summary>
    ///     Configures a custom delay strategy with specified backoff and jitter configurations.
    /// </summary>
    /// <param name="retryOptions">The retry options to configure.</param>
    /// <param name="backoffConfiguration">The backoff strategy configuration.</param>
    /// <param name="jitterStrategy">The jitter strategy delegate.</param>
    /// <returns>A new PipelineRetryOptions instance with configured delay strategy.</returns>
    /// <exception cref="ArgumentNullException">Thrown when retryOptions, backoffConfiguration, or jitterStrategy is null.</exception>
    /// <exception cref="ArgumentException">Thrown when any configuration parameter is invalid.</exception>
    /// <remarks>
    ///     This method creates a new RetryDelayStrategyConfiguration with specified
    ///     backoff and jitter strategies, validates configuration, and returns
    ///     a new PipelineRetryOptions instance with delay strategy applied.
    /// </remarks>
    public static PipelineRetryOptions WithDelayStrategy(
        this PipelineRetryOptions retryOptions,
        BackoffStrategyConfiguration backoffConfiguration,
        JitterStrategy jitterStrategy)
    {
        ArgumentNullException.ThrowIfNull(retryOptions);
        ArgumentNullException.ThrowIfNull(backoffConfiguration);
        ArgumentNullException.ThrowIfNull(jitterStrategy);

        var jitterConfig = new DelegateJitterStrategyConfiguration(jitterStrategy);

        var delayStrategyConfiguration = new RetryDelayStrategyConfiguration(
            backoffConfiguration,
            jitterConfig);

        // Validate configuration
        delayStrategyConfiguration.Validate();

        // Create new retry options with delay strategy
        return retryOptions with { DelayStrategyConfiguration = delayStrategyConfiguration };
    }

    /// <summary>
    ///     Configures exponential backoff with full jitter as delay strategy.
    /// </summary>
    /// <param name="retryOptions">The retry options to configure.</param>
    /// <param name="baseDelay">The base delay for first retry attempt. Default is 1 second.</param>
    /// <param name="multiplier">The multiplier applied to delay for each subsequent retry. Default is 2.0.</param>
    /// <param name="maxDelay">The maximum delay to prevent exponential growth. Default is 1 minute.</param>
    /// <returns>A new PipelineRetryOptions instance with exponential backoff and full jitter configured.</returns>
    /// <exception cref="ArgumentNullException">Thrown when retryOptions is null.</exception>
    /// <exception cref="ArgumentException">Thrown when any parameter is invalid.</exception>
    /// <remarks>
    ///     This is a convenience method that creates exponential backoff configuration
    ///     with full jitter, which is a common and effective combination for distributed systems
    ///     with transient failures. The full jitter helps prevent thundering herd problems
    ///     while exponential backoff provides controlled delay growth.
    /// </remarks>
    public static PipelineRetryOptions WithExponentialBackoffAndFullJitter(
        this PipelineRetryOptions retryOptions,
        TimeSpan? baseDelay = null,
        double? multiplier = null,
        TimeSpan? maxDelay = null)
    {
        ArgumentNullException.ThrowIfNull(retryOptions);

        var backoffConfig = new ExponentialBackoffConfiguration(
            baseDelay ?? TimeSpan.FromSeconds(1),
            multiplier ?? 2.0,
            maxDelay ?? TimeSpan.FromMinutes(1));

        var jitterStrategy = JitterStrategies.FullJitter();

        return retryOptions.WithDelayStrategy(backoffConfig, jitterStrategy);
    }

    /// <summary>
    ///     Configures linear backoff with equal jitter as delay strategy.
    /// </summary>
    /// <param name="retryOptions">The retry options to configure.</param>
    /// <param name="baseDelay">The base delay for first retry attempt. Default is 1 second.</param>
    /// <param name="increment">The increment added to the delay for each subsequent retry. Default is 1 second.</param>
    /// <param name="maxDelay">The maximum delay to prevent linear growth. Default is 30 seconds.</param>
    /// <returns>A new PipelineRetryOptions instance with linear backoff and equal jitter configured.</returns>
    /// <exception cref="ArgumentNullException">Thrown when retryOptions is null.</exception>
    /// <exception cref="ArgumentException">Thrown when any parameter is invalid.</exception>
    /// <remarks>
    ///     This convenience method creates linear backoff configuration with equal jitter,
    ///     which provides predictable gradual recovery with some randomness to prevent
    ///     synchronized retries.
    /// </remarks>
    public static PipelineRetryOptions WithLinearBackoffAndEqualJitter(
        this PipelineRetryOptions retryOptions,
        TimeSpan? baseDelay = null,
        TimeSpan? increment = null,
        TimeSpan? maxDelay = null)
    {
        ArgumentNullException.ThrowIfNull(retryOptions);

        var backoffConfig = new LinearBackoffConfiguration(
            baseDelay ?? TimeSpan.FromSeconds(1),
            increment ?? TimeSpan.FromSeconds(1),
            maxDelay ?? TimeSpan.FromSeconds(30));

        var jitterStrategy = JitterStrategies.EqualJitter();

        return retryOptions.WithDelayStrategy(backoffConfig, jitterStrategy);
    }

    /// <summary>
    ///     Configures fixed delay with no jitter as delay strategy.
    /// </summary>
    /// <param name="retryOptions">The retry options to configure.</param>
    /// <param name="delay">The fixed delay for all retry attempts. Default is 1 second.</param>
    /// <returns>A new PipelineRetryOptions instance with fixed delay and no jitter configured.</returns>
    /// <exception cref="ArgumentNullException">Thrown when retryOptions is null.</exception>
    /// <exception cref="ArgumentException">Thrown when any parameter is invalid.</exception>
    /// <remarks>
    ///     This convenience method creates fixed delay configuration with no jitter,
    ///     which provides simple, deterministic retry timing suitable for testing
    ///     or scenarios where predictable behavior is required.
    /// </remarks>
    public static PipelineRetryOptions WithFixedDelayNoJitter(
        this PipelineRetryOptions retryOptions,
        TimeSpan? delay = null)
    {
        ArgumentNullException.ThrowIfNull(retryOptions);

        var backoffConfig = new FixedDelayConfiguration(delay ?? TimeSpan.FromSeconds(1));
        var jitterStrategy = JitterStrategies.NoJitter();

        return retryOptions.WithDelayStrategy(backoffConfig, jitterStrategy);
    }
}

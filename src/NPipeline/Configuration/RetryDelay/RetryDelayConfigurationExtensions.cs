using NPipeline.Execution.RetryDelay;

namespace NPipeline.Configuration.RetryDelay;

/// <summary>
///     Extension methods for configuring retry delay strategies on PipelineRetryOptions.
/// </summary>
/// <remarks>
///     <para>
///         These extensions provide a fluent API for configuring retry delay strategies
///         with backoff and jitter combinations using delegates directly.
///         This approach eliminates the need for complex configuration classes
///         while maintaining all functionality.
///     </para>
///     <para>
///         Example usage:
///         <code>
///             var retryOptions = PipelineRetryOptions.Default
///                 .WithExponentialBackoffAndFullJitter()
///                 .WithCustomStrategy(
///                     BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1), 2.0, TimeSpan.FromMinutes(1)),
///                     JitterStrategies.FullJitter());
///         </code>
///     </para>
/// </remarks>
public static class RetryDelayConfigurationExtensions
{
    /// <summary>
    ///     Configures a custom delay strategy with specified backoff and jitter delegates.
    /// </summary>
    /// <param name="retryOptions">The retry options to configure.</param>
    /// <param name="backoffStrategy">The backoff strategy delegate.</param>
    /// <param name="jitterStrategy">The optional jitter strategy delegate.</param>
    /// <returns>A new PipelineRetryOptions instance with configured delay strategy.</returns>
    /// <exception cref="ArgumentNullException">Thrown when retryOptions or backoffStrategy is null.</exception>
    /// <remarks>
    ///     This method creates a new RetryDelayStrategyConfiguration with specified
    ///     backoff and jitter strategies, validates configuration, and returns
    ///     a new PipelineRetryOptions instance with delay strategy applied.
    /// </remarks>
    public static PipelineRetryOptions WithCustomStrategy(
        this PipelineRetryOptions retryOptions,
        BackoffStrategy backoffStrategy,
        JitterStrategy? jitterStrategy = null)
    {
        ArgumentNullException.ThrowIfNull(retryOptions);
        ArgumentNullException.ThrowIfNull(backoffStrategy);

        var delayStrategyConfiguration = new RetryDelayStrategyConfiguration(
            backoffStrategy,
            jitterStrategy);

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

        var backoffStrategy = BackoffStrategies.ExponentialBackoff(
            baseDelay ?? TimeSpan.FromSeconds(1),
            multiplier ?? 2.0,
            maxDelay ?? TimeSpan.FromMinutes(1));

        var jitterStrategy = JitterStrategies.FullJitter();

        return retryOptions.WithCustomStrategy(backoffStrategy, jitterStrategy);
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

        var backoffStrategy = BackoffStrategies.LinearBackoff(
            baseDelay ?? TimeSpan.FromSeconds(1),
            increment ?? TimeSpan.FromSeconds(1),
            maxDelay ?? TimeSpan.FromSeconds(30));

        var jitterStrategy = JitterStrategies.EqualJitter();

        return retryOptions.WithCustomStrategy(backoffStrategy, jitterStrategy);
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

        var backoffStrategy = BackoffStrategies.FixedDelay(delay ?? TimeSpan.FromSeconds(1));

        return retryOptions.WithCustomStrategy(backoffStrategy);
    }

    /// <summary>
    ///     Configures exponential backoff with decorrelated jitter as delay strategy.
    /// </summary>
    /// <param name="retryOptions">The retry options to configure.</param>
    /// <param name="baseDelay">The base delay for first retry attempt. Default is 1 second.</param>
    /// <param name="multiplier">The multiplier applied to delay for each subsequent retry. Default is 2.0.</param>
    /// <param name="maxDelay">The maximum delay to prevent exponential growth. Default is 1 minute.</param>
    /// <param name="jitterMaxDelay">The maximum delay for jitter calculations. Default is 1 minute.</param>
    /// <param name="jitterMultiplier">The multiplier for decorrelated jitter. Default is 3.0.</param>
    /// <returns>A new PipelineRetryOptions instance with exponential backoff and decorrelated jitter configured.</returns>
    /// <exception cref="ArgumentNullException">Thrown when retryOptions is null.</exception>
    /// <exception cref="ArgumentException">Thrown when any parameter is invalid.</exception>
    /// <remarks>
    ///     This convenience method creates exponential backoff configuration with decorrelated jitter,
    ///     which provides adaptive randomness based on previous delays, offering better distribution
    ///     than simple jitter while maintaining good performance.
    /// </remarks>
    public static PipelineRetryOptions WithExponentialBackoffAndDecorrelatedJitter(
        this PipelineRetryOptions retryOptions,
        TimeSpan? baseDelay = null,
        double? multiplier = null,
        TimeSpan? maxDelay = null,
        TimeSpan? jitterMaxDelay = null,
        double? jitterMultiplier = null)
    {
        ArgumentNullException.ThrowIfNull(retryOptions);

        var backoffStrategy = BackoffStrategies.ExponentialBackoff(
            baseDelay ?? TimeSpan.FromSeconds(1),
            multiplier ?? 2.0,
            maxDelay ?? TimeSpan.FromMinutes(1));

        var jitterStrategy = JitterStrategies.DecorrelatedJitter(
            jitterMaxDelay ?? TimeSpan.FromMinutes(1),
            jitterMultiplier ?? 3.0);

        return retryOptions.WithCustomStrategy(backoffStrategy, jitterStrategy);
    }

    /// <summary>
    ///     Configures exponential backoff with no jitter as delay strategy.
    /// </summary>
    /// <param name="retryOptions">The retry options to configure.</param>
    /// <param name="baseDelay">The base delay for first retry attempt. Default is 1 second.</param>
    /// <param name="multiplier">The multiplier applied to delay for each subsequent retry. Default is 2.0.</param>
    /// <param name="maxDelay">The maximum delay to prevent exponential growth. Default is 1 minute.</param>
    /// <returns>A new PipelineRetryOptions instance with exponential backoff and no jitter configured.</returns>
    /// <exception cref="ArgumentNullException">Thrown when retryOptions is null.</exception>
    /// <exception cref="ArgumentException">Thrown when any parameter is invalid.</exception>
    /// <remarks>
    ///     This convenience method creates exponential backoff configuration with no jitter,
    ///     which provides controlled delay growth without randomness, suitable for scenarios
    ///     where predictable retry timing is desired.
    /// </remarks>
    public static PipelineRetryOptions WithExponentialBackoffNoJitter(
        this PipelineRetryOptions retryOptions,
        TimeSpan? baseDelay = null,
        double? multiplier = null,
        TimeSpan? maxDelay = null)
    {
        ArgumentNullException.ThrowIfNull(retryOptions);

        var backoffStrategy = BackoffStrategies.ExponentialBackoff(
            baseDelay ?? TimeSpan.FromSeconds(1),
            multiplier ?? 2.0,
            maxDelay ?? TimeSpan.FromMinutes(1));

        return retryOptions.WithCustomStrategy(backoffStrategy);
    }
}

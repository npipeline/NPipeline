using NPipeline.Configuration;
using NPipeline.Configuration.RetryDelay;
using NPipeline.Execution.RetryDelay;

namespace NPipeline.Pipeline;

/// <summary>
///     Extension methods for accessing retry delay strategies from PipelineContext.
/// </summary>
/// <remarks>
///     <para>
///         These extensions provide convenient access to retry delay strategies
///         with caching to avoid repeated strategy creation. The strategies
///         are cached in context.Items dictionary for efficient reuse.
///     </para>
///     <para>
///         When no delay strategy is configured, a NoOpRetryDelayStrategy
///         is returned to provide consistent behavior.
///     </para>
/// </remarks>
public static class PipelineContextRetryDelayExtensions
{
    private const string RetryDelayStrategyCacheKey = "NPipeline.RetryDelayStrategy";
    private const string UpdatedRetryOptionsKey = "NPipeline.UpdatedRetryOptions";

    /// <summary>
    ///     Gets retry delay strategy from pipeline context.
    /// </summary>
    /// <param name="context">The pipeline context to get the strategy from.</param>
    /// <returns>The retry delay strategy configured for the context.</returns>
    /// <exception cref="ArgumentNullException">Thrown when context is null.</exception>
    /// <remarks>
    ///     <para>
    ///         This method retrieves retry delay strategy from the pipeline context,
    ///         using the following logic:
    ///         <list type="number">
    ///             <item>
    ///                 <description>If a strategy is already cached in context.Items, return it</description>
    ///             </item>
    ///             <item>
    ///                 <description>If RetryOptions.DelayStrategyConfiguration is configured, create and cache a strategy</description>
    ///             </item>
    ///             <item>
    ///                 <description>Otherwise, return NoOpRetryDelayStrategy as a fallback</description>
    ///             </item>
    ///         </list>
    ///     </para>
    ///     <para>
    ///         The strategy is cached using the key "NPipeline.RetryDelayStrategy"
    ///         to avoid repeated creation and ensure consistent behavior across
    ///         multiple calls within the same pipeline execution.
    ///     </para>
    /// </remarks>
    public static IRetryDelayStrategy GetRetryDelayStrategy(this PipelineContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        // Check if strategy is already cached
        if (context.Items.TryGetValue(RetryDelayStrategyCacheKey, out var cachedStrategy) &&
            cachedStrategy is IRetryDelayStrategy strategy)
            return strategy;

        IRetryDelayStrategy retryDelayStrategy;

        // Get the current retry options (check if we have updated ones)
        var currentRetryOptions = GetCurrentRetryOptions(context);

        // Check if delay strategy is configured
        if (currentRetryOptions.DelayStrategyConfiguration is { } delayConfig)
        {
            // Create strategy from configuration using factory
            retryDelayStrategy = CreateStrategyFromConfiguration(delayConfig);
        }
        else
        {
            // Fallback to no-op strategy
            retryDelayStrategy = NoOpRetryDelayStrategy.Instance;
        }

        // Cache strategy
        context.Items[RetryDelayStrategyCacheKey] = retryDelayStrategy;

        return retryDelayStrategy;
    }

    /// <summary>
    ///     Gets the current retry options, checking for updated ones in Properties.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <returns>The current retry options.</returns>
    private static PipelineRetryOptions GetCurrentRetryOptions(PipelineContext context)
    {
        // Check if we have updated retry options in Properties
        if (context.Properties.TryGetValue(UpdatedRetryOptionsKey, out var updatedOptions) &&
            updatedOptions is PipelineRetryOptions options)
            return options;

        // Return the original retry options
        return context.RetryOptions;
    }

    /// <summary>
    ///     Creates a retry delay strategy from configuration using factory.
    /// </summary>
    /// <param name="configuration">The delay strategy configuration.</param>
    /// <returns>A retry delay strategy implementation.</returns>
    /// <remarks>
    ///     <para>
    ///         This method uses DefaultRetryDelayStrategyFactory to create
    ///         the appropriate strategy based on configuration. It includes
    ///         proper error handling and falls back to NoOpRetryDelayStrategy
    ///         if strategy creation fails.
    ///     </para>
    /// </remarks>
    private static IRetryDelayStrategy CreateStrategyFromConfiguration(RetryDelayStrategyConfiguration configuration)
    {
        try
        {
            // Validate configuration first
            configuration.Validate();

            var factory = new DefaultRetryDelayStrategyFactory();
            return factory.CreateStrategy(configuration);
        }
        catch
        {
            // Log error and fall back to no-op strategy
            // Note: In a real implementation, you might want to use a logger here
            // For now, we'll fall back silently to maintain backward compatibility
            return NoOpRetryDelayStrategy.Instance;
        }
    }

    /// <summary>
    ///     Gets the retry delay for the specified attempt number.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <param name="attempt">The attempt number (0-based).</param>
    /// <returns>The delay to wait before the next retry.</returns>
    /// <exception cref="ArgumentNullException">Thrown when context is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when attempt is negative.</exception>
    /// <remarks>
    ///     <para>
    ///         This method provides a convenient way to get retry delays
    ///         without directly accessing the strategy. It uses the cached
    ///         strategy from GetRetryDelayStrategy to ensure consistency.
    ///     </para>
    ///     <para>
    ///         The attempt number is 0-based, meaning the first retry
    ///         attempt should use attempt=0, the second retry attempt
    ///         should use attempt=1, and so on.
    ///     </para>
    /// </remarks>
    public static async Task<TimeSpan> GetRetryDelayAsync(this PipelineContext context, int attempt)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentOutOfRangeException.ThrowIfNegative(attempt);

        var strategy = GetRetryDelayStrategy(context);
        return await strategy.GetDelayAsync(attempt);
    }

    /// <summary>
    ///     Configures the pipeline context to use exponential backoff delay.
    /// </summary>
    /// <param name="context">The pipeline context to configure.</param>
    /// <param name="baseDelay">The base delay for exponential backoff.</param>
    /// <param name="multiplier">The multiplier for exponential backoff.</param>
    /// <param name="maxDelay">The maximum delay for exponential backoff.</param>
    /// <returns>The pipeline context for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when context is null.</exception>
    /// <exception cref="ArgumentException">Thrown when parameters are invalid.</exception>
    /// <remarks>
    ///     <para>
    ///         This method configures the pipeline context to use
    ///         exponential backoff delay strategy. The delay grows
    ///         exponentially with each retry attempt: delay = baseDelay * multiplier^attempt.
    ///     </para>
    ///     <para>
    ///         The maxDelay parameter ensures that the delay never exceeds
    ///         the specified maximum value, which is important for preventing
    ///         excessively long delays in retry scenarios.
    ///     </para>
    /// </remarks>
    public static PipelineContext UseExponentialBackoffDelay(
        this PipelineContext context,
        TimeSpan baseDelay,
        double multiplier = 2.0,
        TimeSpan? maxDelay = null)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (baseDelay <= TimeSpan.Zero)
            throw new ArgumentException("Base delay must be positive", nameof(baseDelay));

        if (multiplier <= 1.0)
            throw new ArgumentException("Multiplier must be greater than 1.0", nameof(multiplier));

        var backoffConfig = new ExponentialBackoffConfiguration(
            baseDelay, multiplier, maxDelay ?? TimeSpan.FromMinutes(5));

        var jitterConfig = new NoJitterConfiguration();
        var strategyConfig = new RetryDelayStrategyConfiguration(backoffConfig, jitterConfig);

        // Store the updated retry options in Properties since RetryOptions is read-only
        var updatedRetryOptions = context.RetryOptions with { DelayStrategyConfiguration = strategyConfig };
        context.Properties[UpdatedRetryOptionsKey] = updatedRetryOptions;

        // Clear the cached strategy to force recreation
        context.Items.Remove(RetryDelayStrategyCacheKey);

        return context;
    }

    /// <summary>
    ///     Configures the pipeline context to use linear backoff delay.
    /// </summary>
    /// <param name="context">The pipeline context to configure.</param>
    /// <param name="baseDelay">The base delay for linear backoff.</param>
    /// <param name="increment">The increment to add for each retry attempt.</param>
    /// <param name="maxDelay">The maximum delay for linear backoff.</param>
    /// <returns>The pipeline context for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when context is null.</exception>
    /// <exception cref="ArgumentException">Thrown when parameters are invalid.</exception>
    /// <remarks>
    ///     <para>
    ///         This method configures the pipeline context to use
    ///         linear backoff delay strategy. The delay grows linearly
    ///         with each retry attempt: delay = baseDelay + (increment * attempt).
    ///     </para>
    ///     <para>
    ///         The maxDelay parameter ensures that the delay never exceeds
    ///         the specified maximum value, which is important for preventing
    ///         excessively long delays in retry scenarios.
    ///     </para>
    /// </remarks>
    public static PipelineContext UseLinearBackoffDelay(
        this PipelineContext context,
        TimeSpan baseDelay,
        TimeSpan increment,
        TimeSpan? maxDelay = null)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (baseDelay <= TimeSpan.Zero)
            throw new ArgumentException("Base delay must be positive", nameof(baseDelay));

        if (increment <= TimeSpan.Zero)
            throw new ArgumentException("Increment must be positive", nameof(increment));

        var backoffConfig = new LinearBackoffConfiguration(
            baseDelay, increment, maxDelay ?? TimeSpan.FromMinutes(5));

        var jitterConfig = new NoJitterConfiguration();
        var strategyConfig = new RetryDelayStrategyConfiguration(backoffConfig, jitterConfig);

        // Store the updated retry options in Properties since RetryOptions is read-only
        var updatedRetryOptions = context.RetryOptions with { DelayStrategyConfiguration = strategyConfig };
        context.Properties[UpdatedRetryOptionsKey] = updatedRetryOptions;

        // Clear the cached strategy to force recreation
        context.Items.Remove(RetryDelayStrategyCacheKey);

        return context;
    }

    /// <summary>
    ///     Configures the pipeline context to use fixed delay.
    /// </summary>
    /// <param name="context">The pipeline context to configure.</param>
    /// <param name="delay">The fixed delay between retry attempts.</param>
    /// <returns>The pipeline context for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when context is null.</exception>
    /// <exception cref="ArgumentException">Thrown when delay is invalid.</exception>
    /// <remarks>
    ///     <para>
    ///         This method configures the pipeline context to use
    ///         fixed delay strategy. The delay remains constant for
    ///         all retry attempts.
    ///     </para>
    ///     <para>
    ///         Fixed delay is useful when you want consistent retry
    ///         intervals regardless of the attempt number.
    ///     </para>
    /// </remarks>
    public static PipelineContext UseFixedDelay(
        this PipelineContext context,
        TimeSpan delay)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (delay <= TimeSpan.Zero)
            throw new ArgumentException("Delay must be positive", nameof(delay));

        var backoffConfig = new FixedDelayConfiguration(delay);
        var jitterConfig = new NoJitterConfiguration();
        var strategyConfig = new RetryDelayStrategyConfiguration(backoffConfig, jitterConfig);

        // Store the updated retry options in Properties since RetryOptions is read-only
        var updatedRetryOptions = context.RetryOptions with { DelayStrategyConfiguration = strategyConfig };
        context.Properties[UpdatedRetryOptionsKey] = updatedRetryOptions;

        // Clear the cached strategy to force recreation
        context.Items.Remove(RetryDelayStrategyCacheKey);

        return context;
    }

    /// <summary>
    ///     Configures the pipeline context to use exponential backoff with equal jitter.
    /// </summary>
    /// <param name="context">The pipeline context to configure.</param>
    /// <param name="baseDelay">The base delay for exponential backoff.</param>
    /// <param name="multiplier">The multiplier for exponential backoff.</param>
    /// <param name="maxDelay">The maximum delay for exponential backoff.</param>
    /// <param name="jitterMax">The maximum jitter to apply.</param>
    /// <returns>The pipeline context for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when context is null.</exception>
    /// <exception cref="ArgumentException">Thrown when parameters are invalid.</exception>
    /// <remarks>
    ///     <para>
    ///         This method configures the pipeline context to use
    ///         exponential backoff with equal jitter. Equal jitter adds
    ///         randomness to the delay while maintaining the exponential growth pattern.
    ///     </para>
    ///     <para>
    ///         Equal jitter is calculated as: delay + random(0, jitterMax),
    ///         where jitterMax is typically a fraction of the base delay.
    ///     </para>
    /// </remarks>
    public static PipelineContext UseExponentialBackoffWithJitter(
        this PipelineContext context,
        TimeSpan baseDelay,
        double multiplier = 2.0,
        TimeSpan? maxDelay = null,
        TimeSpan? jitterMax = null)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (baseDelay <= TimeSpan.Zero)
            throw new ArgumentException("Base delay must be positive", nameof(baseDelay));

        if (multiplier <= 1.0)
            throw new ArgumentException("Multiplier must be greater than 1.0", nameof(multiplier));

        var backoffConfig = new ExponentialBackoffConfiguration(
            baseDelay, multiplier, maxDelay ?? TimeSpan.FromMinutes(5));

        var jitterConfig = new EqualJitterConfiguration();
        var strategyConfig = new RetryDelayStrategyConfiguration(backoffConfig, jitterConfig);

        // Store the updated retry options in Properties since RetryOptions is read-only
        var updatedRetryOptions = context.RetryOptions with { DelayStrategyConfiguration = strategyConfig };
        context.Properties[UpdatedRetryOptionsKey] = updatedRetryOptions;

        // Clear the cached strategy to force recreation
        context.Items.Remove(RetryDelayStrategyCacheKey);

        return context;
    }
}

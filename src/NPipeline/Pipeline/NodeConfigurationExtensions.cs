using NPipeline.Configuration;
using NPipeline.Execution;
using NPipeline.Graph;

namespace NPipeline.Pipeline;

/// <summary>
///     Fluent configuration extension methods for pipeline nodes.
///     These extensions provide a convenient way to configure nodes immediately after adding them to the pipeline builder.
/// </summary>
/// <remarks>
///     Usage pattern:
///     <code>
/// var builder = new PipelineBuilder();
/// var transform = builder.AddTransform&lt;MyTransform, int, string&gt;("transform")
///     .WithRetries(3)
///     .WithParallelism(4);
/// </code>
///     Note: These extension methods must be used within the context of the builder's fluent API.
///     The builder instance should be available in the scope where these are called.
/// </remarks>
public static class NodeConfigurationExtensions
{
    /// <summary>
    ///     Configures retry behavior for a source node.
    /// </summary>
    /// <typeparam name="TOut">The output type of the source node.</typeparam>
    /// <param name="handle">The source node handle.</param>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="maxRetries">Maximum number of retry attempts.</param>
    /// <param name="delayMilliseconds">Delay in milliseconds between retry attempts. Default is 0.</param>
    /// <returns>The same source node handle for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when builder is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when maxRetries is negative.</exception>
    public static SourceNodeHandle<TOut> WithRetries<TOut>(
        this SourceNodeHandle<TOut> handle,
        PipelineBuilder builder,
        int maxRetries,
        int delayMilliseconds = 0)
    {
        ArgumentNullException.ThrowIfNull(builder);

        if (maxRetries < 0)
            throw new ArgumentOutOfRangeException(nameof(maxRetries), "Max retries cannot be negative.");

        var retryOptions = PipelineRetryOptions.Default.With(maxRetries);
        builder.WithRetryOptions(handle, retryOptions);
        return handle;
    }

    /// <summary>
    ///     Configures retry behavior for a transform node.
    /// </summary>
    /// <typeparam name="TIn">The input type of the transform node.</typeparam>
    /// <typeparam name="TOut">The output type of the transform node.</typeparam>
    /// <param name="handle">The transform node handle.</param>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="maxRetries">Maximum number of retry attempts.</param>
    /// <param name="delayMilliseconds">Delay in milliseconds between retry attempts. Default is 0.</param>
    /// <returns>The same transform node handle for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when builder is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when maxRetries is negative.</exception>
    public static TransformNodeHandle<TIn, TOut> WithRetries<TIn, TOut>(
        this TransformNodeHandle<TIn, TOut> handle,
        PipelineBuilder builder,
        int maxRetries,
        int delayMilliseconds = 0)
    {
        ArgumentNullException.ThrowIfNull(builder);

        if (maxRetries < 0)
            throw new ArgumentOutOfRangeException(nameof(maxRetries), "Max retries cannot be negative.");

        var retryOptions = PipelineRetryOptions.Default.With(maxRetries);
        builder.WithRetryOptions(handle, retryOptions);
        return handle;
    }

    /// <summary>
    ///     Configures retry behavior for a sink node.
    /// </summary>
    /// <typeparam name="TIn">The input type of the sink node.</typeparam>
    /// <param name="handle">The sink node handle.</param>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="maxRetries">Maximum number of retry attempts.</param>
    /// <param name="delayMilliseconds">Delay in milliseconds between retry attempts. Default is 0.</param>
    /// <returns>The same sink node handle for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when builder is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when maxRetries is negative.</exception>
    public static SinkNodeHandle<TIn> WithRetries<TIn>(
        this SinkNodeHandle<TIn> handle,
        PipelineBuilder builder,
        int maxRetries,
        int delayMilliseconds = 0)
    {
        ArgumentNullException.ThrowIfNull(builder);

        if (maxRetries < 0)
            throw new ArgumentOutOfRangeException(nameof(maxRetries), "Max retries cannot be negative.");

        var retryOptions = PipelineRetryOptions.Default.With(maxRetries);
        builder.WithRetryOptions(handle, retryOptions);
        return handle;
    }

    /// <summary>
    ///     Configures retry behavior for an aggregate node.
    /// </summary>
    /// <typeparam name="TIn">The input type of the aggregate node.</typeparam>
    /// <typeparam name="TOut">The output type of the aggregate node.</typeparam>
    /// <param name="handle">The aggregate node handle.</param>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="maxRetries">Maximum number of retry attempts.</param>
    /// <param name="delayMilliseconds">Delay in milliseconds between retry attempts. Default is 0.</param>
    /// <returns>The same aggregate node handle for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when builder is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when maxRetries is negative.</exception>
    public static AggregateNodeHandle<TIn, TOut> WithRetries<TIn, TOut>(
        this AggregateNodeHandle<TIn, TOut> handle,
        PipelineBuilder builder,
        int maxRetries,
        int delayMilliseconds = 0)
    {
        ArgumentNullException.ThrowIfNull(builder);

        if (maxRetries < 0)
            throw new ArgumentOutOfRangeException(nameof(maxRetries), "Max retries cannot be negative.");

        var retryOptions = PipelineRetryOptions.Default.With(maxRetries);
        builder.WithRetryOptions(handle, retryOptions);
        return handle;
    }

    /// <summary>
    ///     Applies retry defaults for this source node using the builder's active optimization profile.
    ///     In <see cref="PipelineOptimizationProfile.Default" /> this enables retries with sensible defaults;
    ///     in <see cref="PipelineOptimizationProfile.HighThroughput" /> this applies strict baseline defaults.
    /// </summary>
    /// <typeparam name="TOut">The output type of the source node.</typeparam>
    /// <param name="handle">The source node handle.</param>
    /// <param name="builder">The pipeline builder.</param>
    /// <returns>The same source node handle for method chaining.</returns>
    public static SourceNodeHandle<TOut> WithRetry<TOut>(
        this SourceNodeHandle<TOut> handle,
        PipelineBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        return handle.WithRetry(builder, builder.CurrentOptimizationProfile);
    }

    /// <summary>
    ///     Applies retry defaults for this source node using the specified optimization profile.
    /// </summary>
    /// <typeparam name="TOut">The output type of the source node.</typeparam>
    /// <param name="handle">The source node handle.</param>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="profile">The optimization profile whose retry defaults should be applied.</param>
    /// <returns>The same source node handle for method chaining.</returns>
    public static SourceNodeHandle<TOut> WithRetry<TOut>(
        this SourceNodeHandle<TOut> handle,
        PipelineBuilder builder,
        PipelineOptimizationProfile profile)
    {
        ArgumentNullException.ThrowIfNull(builder);

        ApplyProfileRetryDefaults(builder, handle, profile);
        return handle;
    }

    /// <summary>
    ///     Applies retry defaults for this transform node using the builder's active optimization profile.
    ///     In <see cref="PipelineOptimizationProfile.Default" /> this enables retries with sensible defaults;
    ///     in <see cref="PipelineOptimizationProfile.HighThroughput" /> this applies strict baseline defaults.
    /// </summary>
    /// <typeparam name="TIn">The input type of the transform node.</typeparam>
    /// <typeparam name="TOut">The output type of the transform node.</typeparam>
    /// <param name="handle">The transform node handle.</param>
    /// <param name="builder">The pipeline builder.</param>
    /// <returns>The same transform node handle for method chaining.</returns>
    public static TransformNodeHandle<TIn, TOut> WithRetry<TIn, TOut>(
        this TransformNodeHandle<TIn, TOut> handle,
        PipelineBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        return handle.WithRetry(builder, builder.CurrentOptimizationProfile);
    }

    /// <summary>
    ///     Applies retry defaults for this transform node using the specified optimization profile.
    /// </summary>
    /// <typeparam name="TIn">The input type of the transform node.</typeparam>
    /// <typeparam name="TOut">The output type of the transform node.</typeparam>
    /// <param name="handle">The transform node handle.</param>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="profile">The optimization profile whose retry defaults should be applied.</param>
    /// <returns>The same transform node handle for method chaining.</returns>
    public static TransformNodeHandle<TIn, TOut> WithRetry<TIn, TOut>(
        this TransformNodeHandle<TIn, TOut> handle,
        PipelineBuilder builder,
        PipelineOptimizationProfile profile)
    {
        ArgumentNullException.ThrowIfNull(builder);

        ApplyProfileRetryDefaults(builder, handle, profile);
        return handle;
    }

    /// <summary>
    ///     Applies retry defaults for this sink node using the builder's active optimization profile.
    ///     In <see cref="PipelineOptimizationProfile.Default" /> this enables retries with sensible defaults;
    ///     in <see cref="PipelineOptimizationProfile.HighThroughput" /> this applies strict baseline defaults.
    /// </summary>
    /// <typeparam name="TIn">The input type of the sink node.</typeparam>
    /// <param name="handle">The sink node handle.</param>
    /// <param name="builder">The pipeline builder.</param>
    /// <returns>The same sink node handle for method chaining.</returns>
    public static SinkNodeHandle<TIn> WithRetry<TIn>(
        this SinkNodeHandle<TIn> handle,
        PipelineBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        return handle.WithRetry(builder, builder.CurrentOptimizationProfile);
    }

    /// <summary>
    ///     Applies retry defaults for this sink node using the specified optimization profile.
    /// </summary>
    /// <typeparam name="TIn">The input type of the sink node.</typeparam>
    /// <param name="handle">The sink node handle.</param>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="profile">The optimization profile whose retry defaults should be applied.</param>
    /// <returns>The same sink node handle for method chaining.</returns>
    public static SinkNodeHandle<TIn> WithRetry<TIn>(
        this SinkNodeHandle<TIn> handle,
        PipelineBuilder builder,
        PipelineOptimizationProfile profile)
    {
        ArgumentNullException.ThrowIfNull(builder);

        ApplyProfileRetryDefaults(builder, handle, profile);
        return handle;
    }

    /// <summary>
    ///     Applies retry defaults for this aggregate node using the builder's active optimization profile.
    ///     In <see cref="PipelineOptimizationProfile.Default" /> this enables retries with sensible defaults;
    ///     in <see cref="PipelineOptimizationProfile.HighThroughput" /> this applies strict baseline defaults.
    /// </summary>
    /// <typeparam name="TIn">The input type of the aggregate node.</typeparam>
    /// <typeparam name="TOut">The output type of the aggregate node.</typeparam>
    /// <param name="handle">The aggregate node handle.</param>
    /// <param name="builder">The pipeline builder.</param>
    /// <returns>The same aggregate node handle for method chaining.</returns>
    public static AggregateNodeHandle<TIn, TOut> WithRetry<TIn, TOut>(
        this AggregateNodeHandle<TIn, TOut> handle,
        PipelineBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        return handle.WithRetry(builder, builder.CurrentOptimizationProfile);
    }

    /// <summary>
    ///     Applies retry defaults for this aggregate node using the specified optimization profile.
    /// </summary>
    /// <typeparam name="TIn">The input type of the aggregate node.</typeparam>
    /// <typeparam name="TOut">The output type of the aggregate node.</typeparam>
    /// <param name="handle">The aggregate node handle.</param>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="profile">The optimization profile whose retry defaults should be applied.</param>
    /// <returns>The same aggregate node handle for method chaining.</returns>
    public static AggregateNodeHandle<TIn, TOut> WithRetry<TIn, TOut>(
        this AggregateNodeHandle<TIn, TOut> handle,
        PipelineBuilder builder,
        PipelineOptimizationProfile profile)
    {
        ArgumentNullException.ThrowIfNull(builder);

        ApplyProfileRetryDefaults(builder, handle, profile);
        return handle;
    }

    private static void ApplyProfileRetryDefaults(PipelineBuilder builder, NodeHandle handle, PipelineOptimizationProfile profile)
    {
        var retryOptions = OptimizationProfileBehaviorRegistry.For(profile).RetryDefaults;
        builder.WithRetryOptions(handle, retryOptions);
    }

    /// <summary>
    ///     Configures the execution strategy for a transform node.
    /// </summary>
    /// <typeparam name="TIn">The input type of the transform node.</typeparam>
    /// <typeparam name="TOut">The output type of the transform node.</typeparam>
    /// <param name="handle">The transform node handle.</param>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="strategy">The execution strategy to apply.</param>
    /// <returns>The same transform node handle for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when builder or strategy is null.</exception>
    public static TransformNodeHandle<TIn, TOut> WithExecutionStrategy<TIn, TOut>(
        this TransformNodeHandle<TIn, TOut> handle,
        PipelineBuilder builder,
        IExecutionStrategy strategy)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(strategy);

        builder.WithExecutionStrategy(handle, strategy);
        return handle;
    }

    /// <summary>
    ///     Enables resilient execution for a transform node, wrapping the current execution strategy with retry/fallback logic.
    /// </summary>
    /// <typeparam name="TIn">The input type of the transform node.</typeparam>
    /// <typeparam name="TOut">The output type of the transform node.</typeparam>
    /// <param name="handle">The transform node handle.</param>
    /// <param name="builder">The pipeline builder.</param>
    /// <returns>The same transform node handle for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when builder is null.</exception>
    public static TransformNodeHandle<TIn, TOut> WithResilience<TIn, TOut>(
        this TransformNodeHandle<TIn, TOut> handle,
        PipelineBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.WithResilience(handle);
        return handle;
    }
}

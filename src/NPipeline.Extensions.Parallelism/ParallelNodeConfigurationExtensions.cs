using NPipeline.Graph;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Parallelism;

/// <summary>
///     Fluent configuration extension methods for parallel execution on transform nodes.
///     These extensions provide a convenient way to configure parallel execution strategies
///     immediately after adding nodes to the pipeline builder.
/// </summary>
/// <remarks>
///     Usage pattern:
///     <code>
/// var transform = builder
///     .AddTransform&lt;MyTransform, int, string&gt;()
///     .WithBlockingParallelism(builder, maxDegreeOfParallelism: 4)
///     .WithRetries(builder, maxRetries: 2);
/// </code>
///     This extension is part of the NPipeline.Extensions.Parallelism package
///     and requires a using directive to access.
/// </remarks>
public static class ParallelNodeConfigurationExtensions
{
    /// <summary>
    ///     Configures blocking parallel execution for a transform node.
    /// </summary>
    /// <remarks>
    ///     Blocking parallelism preserves ordering and applies end-to-end backpressure,
    ///     making it suitable for scenarios requiring flow control and ordered output.
    /// </remarks>
    /// <typeparam name="TIn">The input type of the transform node.</typeparam>
    /// <typeparam name="TOut">The output type of the transform node.</typeparam>
    /// <param name="handle">The transform node handle.</param>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="maxDegreeOfParallelism">Maximum degree of parallelism. If null, uses system processor count.</param>
    /// <param name="maxQueueLength">Optional bounded input queue length for backpressure control.</param>
    /// <param name="outputBufferCapacity">Optional maximum number of buffered results. Null means unbounded.</param>
    /// <returns>The same transform node handle for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when builder is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when maxDegreeOfParallelism is &lt;= 0.</exception>
    public static TransformNodeHandle<TIn, TOut> WithBlockingParallelism<TIn, TOut>(
        this TransformNodeHandle<TIn, TOut> handle,
        PipelineBuilder builder,
        int? maxDegreeOfParallelism = null,
        int? maxQueueLength = null,
        int? outputBufferCapacity = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        if (maxDegreeOfParallelism is not null and <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxDegreeOfParallelism), "Max degree of parallelism must be positive.");

        var options = new ParallelOptions(
            maxDegreeOfParallelism,
            maxQueueLength,
            BoundedQueuePolicy.Block,
            outputBufferCapacity);

        builder.SetNodeExecutionOption(handle.Id, options);
        builder.WithExecutionStrategy(handle, new BlockingParallelStrategy());
        return handle;
    }

    /// <summary>
    ///     Configures drop-oldest parallel execution for a transform node.
    /// </summary>
    /// <remarks>
    ///     Drop-oldest parallelism discards the oldest items when the input queue is full,
    ///     prioritizing processing of new data. This is suitable for high-throughput scenarios
    ///     where missing some items is acceptable.
    /// </remarks>
    /// <typeparam name="TIn">The input type of the transform node.</typeparam>
    /// <typeparam name="TOut">The output type of the transform node.</typeparam>
    /// <param name="handle">The transform node handle.</param>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="maxDegreeOfParallelism">Maximum degree of parallelism. If null, uses system processor count.</param>
    /// <param name="maxQueueLength">Bounded input queue length. If null, uses a default size.</param>
    /// <returns>The same transform node handle for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when builder is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when maxDegreeOfParallelism is &lt;= 0.</exception>
    public static TransformNodeHandle<TIn, TOut> WithDropOldestParallelism<TIn, TOut>(
        this TransformNodeHandle<TIn, TOut> handle,
        PipelineBuilder builder,
        int? maxDegreeOfParallelism = null,
        int? maxQueueLength = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        if (maxDegreeOfParallelism is not null and <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxDegreeOfParallelism), "Max degree of parallelism must be positive.");

        var options = new ParallelOptions(
            maxDegreeOfParallelism,
            maxQueueLength ?? 100, // Default queue length for bounded policy
            BoundedQueuePolicy.DropOldest,
            null,
            false);

        builder.SetNodeExecutionOption(handle.Id, options);
        builder.WithExecutionStrategy(handle, new DropOldestParallelStrategy());
        return handle;
    }

    /// <summary>
    ///     Configures drop-newest parallel execution for a transform node.
    /// </summary>
    /// <remarks>
    ///     Drop-newest parallelism discards the newest items when the input queue is full,
    ///     preserving older data. This is suitable for high-throughput scenarios where
    ///     maintaining historical data is more important than processing all new data.
    /// </remarks>
    /// <typeparam name="TIn">The input type of the transform node.</typeparam>
    /// <typeparam name="TOut">The output type of the transform node.</typeparam>
    /// <param name="handle">The transform node handle.</param>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="maxDegreeOfParallelism">Maximum degree of parallelism. If null, uses system processor count.</param>
    /// <param name="maxQueueLength">Bounded input queue length. If null, uses a default size.</param>
    /// <returns>The same transform node handle for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when builder is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when maxDegreeOfParallelism is &lt;= 0.</exception>
    public static TransformNodeHandle<TIn, TOut> WithDropNewestParallelism<TIn, TOut>(
        this TransformNodeHandle<TIn, TOut> handle,
        PipelineBuilder builder,
        int? maxDegreeOfParallelism = null,
        int? maxQueueLength = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        if (maxDegreeOfParallelism is not null and <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxDegreeOfParallelism), "Max degree of parallelism must be positive.");

        var options = new ParallelOptions(
            maxDegreeOfParallelism,
            maxQueueLength ?? 100, // Default queue length for bounded policy
            BoundedQueuePolicy.DropNewest,
            null,
            false);

        builder.SetNodeExecutionOption(handle.Id, options);
        builder.WithExecutionStrategy(handle, new DropNewestParallelStrategy());
        return handle;
    }

    /// <summary>
    ///     Configures parallel execution for a transform node with custom options.
    /// </summary>
    /// <remarks>
    ///     This overload allows full control over parallel execution options.
    ///     Use one of the specialized methods (WithBlockingParallelism, WithDropOldestParallelism, etc.)
    ///     for common scenarios.
    /// </remarks>
    /// <typeparam name="TIn">The input type of the transform node.</typeparam>
    /// <typeparam name="TOut">The output type of the transform node.</typeparam>
    /// <param name="handle">The transform node handle.</param>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="options">The parallel execution options.</param>
    /// <param name="strategy">The parallel execution strategy to apply.</param>
    /// <returns>The same transform node handle for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when builder, options, or strategy is null.</exception>
    public static TransformNodeHandle<TIn, TOut> WithParallelism<TIn, TOut>(
        this TransformNodeHandle<TIn, TOut> handle,
        PipelineBuilder builder,
        ParallelOptions options,
        ParallelExecutionStrategyBase strategy)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(strategy);

        builder.SetNodeExecutionOption(handle.Id, options);
        builder.WithExecutionStrategy(handle, strategy);
        return handle;
    }
}

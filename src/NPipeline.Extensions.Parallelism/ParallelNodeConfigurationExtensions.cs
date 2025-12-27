using NPipeline.Graph;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Parallelism;

/// <summary>
///     Presets for automatic ParallelOptions configuration based on workload characteristics.
/// </summary>
public static class ParallelOptionsPresets
{
    /// <summary>
    ///     Gets the recommended ParallelOptions for a given workload type.
    /// </summary>
    public static ParallelOptions GetForWorkloadType(ParallelWorkloadType workloadType)
    {
        var processorCount = Environment.ProcessorCount;

        return workloadType switch
        {
            ParallelWorkloadType.General =>
                new ParallelOptions(
                    processorCount * 2,
                    processorCount * 4,
                    BoundedQueuePolicy.Block,
                    processorCount * 8),

            ParallelWorkloadType.CpuBound =>
                new ParallelOptions(
                    processorCount,
                    processorCount * 2,
                    BoundedQueuePolicy.Block,
                    processorCount * 4),

            ParallelWorkloadType.IoBound =>
                new ParallelOptions(
                    processorCount * 4,
                    processorCount * 8,
                    BoundedQueuePolicy.Block,
                    processorCount * 16),

            ParallelWorkloadType.NetworkBound =>
                new ParallelOptions(
                    Math.Min(processorCount * 8, 100),
                    200,
                    BoundedQueuePolicy.Block,
                    400),

            _ => throw new ArgumentException($"Unknown workload type: {workloadType}", nameof(workloadType)),
        };
    }
}

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
    ///     Default queue length for bounded parallel execution strategies.
    /// </summary>
    public const int DefaultQueueLength = 100;

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
            maxQueueLength ?? DefaultQueueLength, // Default queue length for bounded policy
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
            maxQueueLength ?? DefaultQueueLength, // Default queue length for bounded policy
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

    /// <summary>
    ///     Configures parallel execution for a transform node with a simplified preset based on workload type.
    /// </summary>
    /// <remarks>
    ///     This is the simplified API for configuring parallelism. It automatically selects sensible
    ///     defaults for degree of parallelism, queue length, and buffering based on the workload type.
    ///     Workload type recommendations:
    ///     - <see cref="ParallelWorkloadType.General" />: Mixed CPU and I/O workloads (default, safe choice)
    ///     - <see cref="ParallelWorkloadType.CpuBound" />: CPU-intensive operations (avoid oversubscription)
    ///     - <see cref="ParallelWorkloadType.IoBound" />: I/O-intensive operations (higher parallelism hides latency)
    ///     - <see cref="ParallelWorkloadType.NetworkBound" />: Network operations (very high latency, up to 100 concurrent)
    /// </remarks>
    /// <typeparam name="TIn">The input type of the transform node.</typeparam>
    /// <typeparam name="TOut">The output type of the transform node.</typeparam>
    /// <param name="handle">The transform node handle.</param>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="workloadType">The workload type to optimize for. Defaults to General.</param>
    /// <returns>The same transform node handle for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when builder is null.</exception>
    /// <example>
    ///     Configure blocking parallel execution optimized for I/O-bound operations:
    ///     <code>
    /// var result = builder
    ///     .AddTransform&lt;FileProcessor, string, ProcessedData&gt;()
    ///     .RunParallel(builder, ParallelWorkloadType.IoBound);
    /// </code>
    /// </example>
    public static TransformNodeHandle<TIn, TOut> RunParallel<TIn, TOut>(
        this TransformNodeHandle<TIn, TOut> handle,
        PipelineBuilder builder,
        ParallelWorkloadType workloadType = ParallelWorkloadType.General)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var options = ParallelOptionsPresets.GetForWorkloadType(workloadType);
        builder.SetNodeExecutionOption(handle.Id, options);
        builder.WithExecutionStrategy(handle, new BlockingParallelStrategy());
        return handle;
    }

    /// <summary>
    ///     Configures parallel execution for a transform node using a fluent builder for custom configuration.
    /// </summary>
    /// <remarks>
    ///     This API provides a flexible way to configure parallelism by using the ParallelOptionsBuilder
    ///     fluent interface. This is useful when the preset workload types don't exactly match your requirements.
    ///     The builder provides sensible defaults that can be customized with method chaining.
    ///     The default queue policy is Block (applies backpressure), which is appropriate for most scenarios.
    /// </remarks>
    /// <typeparam name="TIn">The input type of the transform node.</typeparam>
    /// <typeparam name="TOut">The output type of the transform node.</typeparam>
    /// <param name="handle">The transform node handle.</param>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="configure">
    ///     An action that configures the ParallelOptionsBuilder.
    ///     The builder starts with sensible defaults (General workload type) that can be overridden.
    /// </param>
    /// <returns>The same transform node handle for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when builder or configure is null.</exception>
    /// <example>
    ///     Configure parallel execution with custom settings:
    ///     <code>
    /// var result = builder
    ///     .AddTransform&lt;DataProcessor, int, string&gt;()
    ///     .RunParallel(builder, opt => opt
    ///         .MaxDegreeOfParallelism(8)
    ///         .MaxQueueLength(50)
    ///         .DropOldestOnBackpressure());
    /// </code>
    /// </example>
    public static TransformNodeHandle<TIn, TOut> RunParallel<TIn, TOut>(
        this TransformNodeHandle<TIn, TOut> handle,
        PipelineBuilder builder,
        Action<ParallelOptionsBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        var parallelBuilder = new ParallelOptionsBuilder();
        configure(parallelBuilder);
        var options = parallelBuilder.Build();

        builder.SetNodeExecutionOption(handle.Id, options);
        builder.WithExecutionStrategy(handle, new BlockingParallelStrategy());
        return handle;
    }
}

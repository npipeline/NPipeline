using NPipeline.Graph;
using NPipeline.Nodes;

namespace NPipeline.Pipeline;

/// <summary>
///     Provides extension methods for the <see cref="PipelineBuilder" /> class.
/// </summary>
public static class PipelineBuilderExtensions
{
    /// <summary>
    ///     Adds a tap node to the pipeline, which taps into data flow to send copies to a sink for monitoring purposes.
    ///     This allows for side-channel processing (monitoring, logging, etc.) without affecting the main data flow.
    ///     For general-purpose branching to multiple pathways, use <see cref="AddBranch{T}(PipelineBuilder, Func{T, Task}, string?)" /> instead.
    /// </summary>
    /// <typeparam name="T">The type of data being processed.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="sink">The sink node to send data copies to.</param>
    /// <param name="name">An optional descriptive name for the node. If null, a default name is used.</param>
    /// <returns>A handle to the newly added tap node.</returns>
    public static TransformNodeHandle<T, T> AddTap<T>(
        this PipelineBuilder builder,
        ISinkNode<T> sink,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(sink);

        var handle = builder.AddTransform<TapNode<T>, T, T>(name ?? "Tap");
        var node = new TapNode<T>(sink);
        builder.RegisterBuilderDisposable(node);
        _ = builder.AddPreconfiguredNodeInstance(handle.Id, node);
        return handle;
    }

    /// <summary>
    ///     Adds a tap node to the pipeline, which taps into data flow to send copies to a sink for monitoring purposes.
    ///     This allows for side-channel processing (monitoring, logging, etc.) without affecting the main data flow.
    ///     For general-purpose branching to multiple pathways, use <see cref="AddBranch{T}(PipelineBuilder, Func{T, Task}, string?)" /> instead.
    /// </summary>
    /// <typeparam name="T">The type of data being processed.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="sinkFactory">A factory function that creates the sink node to send data copies to.</param>
    /// <param name="name">An optional descriptive name for the node. If null, a default name is used.</param>
    /// <returns>A handle to the newly added tap node.</returns>
    public static TransformNodeHandle<T, T> AddTap<T>(this PipelineBuilder builder, Func<ISinkNode<T>> sinkFactory, string? name = null)
    {
        ArgumentNullException.ThrowIfNull(sinkFactory);

        var handle = builder.AddTransform<TapNode<T>, T, T>(name ?? "Tap");
        var node = new TapNode<T>(sinkFactory());
        builder.RegisterBuilderDisposable(node);
        _ = builder.AddPreconfiguredNodeInstance(handle.Id, node);
        return handle;
    }

    /// <summary>
    ///     Adds a branch node to the pipeline, which fans out data to multiple downstream pathways for parallel processing.
    ///     This allows data to be sent to multiple destinations simultaneously without affecting the main data flow.
    ///     For sink-specific monitoring, use <see cref="AddTap{T}(PipelineBuilder, ISinkNode{T}, string?)" /> instead.
    /// </summary>
    /// <typeparam name="T">The type of data being processed.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="outputHandler">An async function that processes the data item.</param>
    /// <param name="name">An optional descriptive name for the node. If null, a default name is used.</param>
    /// <returns>A handle to the newly added branch node.</returns>
    public static TransformNodeHandle<T, T> AddBranch<T>(this PipelineBuilder builder, Func<T, Task> outputHandler, string? name = null)
    {
        ArgumentNullException.ThrowIfNull(outputHandler);

        var handle = builder.AddTransform<BranchNode<T>, T, T>(name ?? "Tee");
        var node = new BranchNode<T>();
        builder.RegisterBuilderDisposable(node);
        node.AddOutput(outputHandler);
        _ = builder.AddPreconfiguredNodeInstance(handle.Id, node);
        return handle;
    }

    /// <summary>
    ///     Adds a branch node to the pipeline, which fans out data to multiple downstream pathways for parallel processing.
    ///     This allows data to be sent to multiple destinations simultaneously without affecting the main data flow.
    ///     For sink-specific monitoring, use <see cref="AddTap{T}(PipelineBuilder, ISinkNode{T}, string?)" /> instead.
    /// </summary>
    /// <typeparam name="T">The type of data being processed.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="outputHandlers">A collection of async functions that process the data item.</param>
    /// <param name="name">An optional descriptive name for the node. If null, a default name is used.</param>
    /// <returns>A handle to the newly added branch node.</returns>
    public static TransformNodeHandle<T, T> AddBranch<T>(this PipelineBuilder builder, IEnumerable<Func<T, Task>> outputHandlers, string? name = null)
    {
        ArgumentNullException.ThrowIfNull(outputHandlers);

        var handle = builder.AddTransform<BranchNode<T>, T, T>(name ?? "Tee");
        var node = new BranchNode<T>();
        builder.RegisterBuilderDisposable(node);

        foreach (var handler in outputHandlers)
        {
            node.AddOutput(handler);
        }

        _ = builder.AddPreconfiguredNodeInstance(handle.Id, node);
        return handle;
    }

    /// <summary>
    ///     Adds a lambda-based transform node that supports both synchronous and asynchronous transformations.
    /// </summary>
    /// <typeparam name="TIn">The input item type.</typeparam>
    /// <typeparam name="TOut">The output item type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="transform">The synchronous transformation function. For async, this overload is automatically replaced.</param>
    /// <param name="name">An optional descriptive name for the node. If null, a default name is used.</param>
    /// <returns>A handle to the newly added transform node.</returns>
    /// <remarks>
    ///     <para>
    ///         This is a convenience method for creating transformations without defining a separate class.
    ///         The compiler automatically selects the appropriate overload based on your delegate type.
    ///     </para>
    /// </remarks>
    /// <example>
    ///     <code>
    /// // Synchronous
    /// var t = builder.AddTransform((string s) => s.ToUpper(), "upper");
    /// // Asynchronous (compiler selects async overload)  
    /// var t = builder.AddTransform(async (id, ct) => await GetAsync(id, ct), "get");
    /// </code>
    /// </example>
    public static TransformNodeHandle<TIn, TOut> AddTransform<TIn, TOut>(
        this PipelineBuilder builder,
        Func<TIn, TOut> transform,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(transform);

        var nodeName = name ?? $"Transform_{typeof(TIn).Name}_to_{typeof(TOut).Name}";
        var handle = builder.AddTransform<LambdaTransformNode<TIn, TOut>, TIn, TOut>(nodeName);
        var node = new LambdaTransformNode<TIn, TOut>(transform);
        _ = builder.AddPreconfiguredNodeInstance(handle.Id, node);
        return handle;
    }

    /// <summary>
    ///     Adds a lambda-based asynchronous transform node for transformations that require I/O or other async operations.
    /// </summary>
    /// <typeparam name="TIn">The input item type.</typeparam>
    /// <typeparam name="TOut">The output item type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="transform">The asynchronous transformation function. The compiler selects this overload automatically.</param>
    /// <param name="name">An optional descriptive name for the node. If null, a default name is used.</param>
    /// <returns>A handle to the newly added transform node.</returns>
    /// <remarks>
    ///     <para>
    ///         This overload is automatically selected when you pass an async function.
    ///         The transformation respects the cancellation token and can be cancelled when the pipeline stops.
    ///     </para>
    /// </remarks>
    public static TransformNodeHandle<TIn, TOut> AddTransform<TIn, TOut>(
        this PipelineBuilder builder,
        Func<TIn, CancellationToken, ValueTask<TOut>> transform,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(transform);

        var nodeName = name ?? $"Transform_{typeof(TIn).Name}_to_{typeof(TOut).Name}";
        var handle = builder.AddTransform<AsyncLambdaTransformNode<TIn, TOut>, TIn, TOut>(nodeName);
        var node = new AsyncLambdaTransformNode<TIn, TOut>(transform);
        _ = builder.AddPreconfiguredNodeInstance(handle.Id, node);
        return handle;
    }

    /// <summary>
    ///     Adds a preconfigured source node instance to the pipeline using the runtime type of the node.
    /// </summary>
    /// <typeparam name="TOut">The output item type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="node">The source node instance.</param>
    /// <param name="name">An optional descriptive name for the node. If null, a default name is used.</param>
    /// <returns>A handle to the newly added source node.</returns>
    public static SourceNodeHandle<TOut> AddSource<TOut>(
        this PipelineBuilder builder,
        ISourceNode<TOut> node,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(node);

        var handle = builder.AddSource<TOut>(node.GetType(), name);
        builder.RegisterBuilderDisposable(node);
        _ = builder.AddPreconfiguredNodeInstance(handle.Id, node);
        return handle;
    }

    /// <summary>
    ///     Adds a lambda-based source node that supports both synchronous and asynchronous factory functions.
    /// </summary>
    /// <typeparam name="TOut">The output item type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="factory">
    ///     A factory function that returns an enumerable of items. For async, this overload is automatically replaced.
    /// </param>
    /// <param name="name">An optional descriptive name for the node. If null, a default name is used.</param>
    /// <returns>A handle to the newly added source node.</returns>
    /// <remarks>
    ///     <para>
    ///         This is a convenience method for creating data sources without defining a separate class.
    ///         The compiler automatically selects the appropriate overload based on your delegate type.
    ///     </para>
    /// </remarks>
    /// <example>
    ///     <code>
    /// // Synchronous
    /// var source = builder.AddSource(() => new[] { 1, 2, 3 }, "numbers");
    /// // Asynchronous (compiler selects async overload)
    /// var source = builder.AddSource(async ct => await GetAsync(ct), "items");
    /// </code>
    /// </example>
    public static SourceNodeHandle<TOut> AddSource<TOut>(
        this PipelineBuilder builder,
        Func<IEnumerable<TOut>> factory,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(factory);

        var nodeName = name ?? $"Source_{typeof(TOut).Name}";
        var handle = builder.AddSource<LambdaSourceNode<TOut>, TOut>(nodeName);
        var node = new LambdaSourceNode<TOut>(factory);
        _ = builder.AddPreconfiguredNodeInstance(handle.Id, node);
        return handle;
    }

    /// <summary>
    ///     Adds a lambda-based asynchronous source node that produces items from an async factory function.
    /// </summary>
    /// <typeparam name="TOut">The output item type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="factory">
    ///     A factory function that receives a cancellation token and returns an async enumerable.
    ///     The compiler selects this overload automatically.
    /// </param>
    /// <param name="name">An optional descriptive name for the node. If null, a default name is used.</param>
    /// <returns>A handle to the newly added source node.</returns>
    /// <remarks>
    ///     <para>
    ///         This overload is automatically selected when you pass an async factory function.
    ///         The source factory receives a cancellation token and can respect it to support pipeline cancellation.
    ///     </para>
    /// </remarks>
    public static SourceNodeHandle<TOut> AddSource<TOut>(
        this PipelineBuilder builder,
        Func<CancellationToken, IAsyncEnumerable<TOut>> factory,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(factory);

        var nodeName = name ?? $"Source_{typeof(TOut).Name}";
        var handle = builder.AddSource<LambdaSourceNode<TOut>, TOut>(nodeName);
        var node = new LambdaSourceNode<TOut>(factory);
        _ = builder.AddPreconfiguredNodeInstance(handle.Id, node);
        return handle;
    }

    /// <summary>
    ///     Adds a preconfigured sink node instance to the pipeline using the runtime type of the node.
    /// </summary>
    /// <typeparam name="TIn">The input item type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="node">The sink node instance.</param>
    /// <param name="name">An optional descriptive name for the node. If null, a default name is used.</param>
    /// <returns>A handle to the newly added sink node.</returns>
    public static SinkNodeHandle<TIn> AddSink<TIn>(
        this PipelineBuilder builder,
        ISinkNode<TIn> node,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(node);

        var handle = builder.AddSink<TIn>(node.GetType(), name);
        builder.RegisterBuilderDisposable(node);
        _ = builder.AddPreconfiguredNodeInstance(handle.Id, node);
        return handle;
    }

    /// <summary>
    ///     Adds a lambda-based sink node that supports both synchronous and asynchronous consume functions.
    /// </summary>
    /// <typeparam name="TIn">The input item type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="consume">
    ///     The consume function. For async, this overload is automatically replaced.
    /// </param>
    /// <param name="name">An optional descriptive name for the node. If null, a default name is used.</param>
    /// <returns>A handle to the newly added sink node.</returns>
    /// <remarks>
    ///     <para>
    ///         This is a convenience method for creating sink nodes without defining a separate class.
    ///         The compiler automatically selects the appropriate overload based on your delegate type.
    ///     </para>
    /// </remarks>
    /// <example>
    ///     <code>
    /// // Synchronous
    /// var sink = builder.AddSink((item) => Console.WriteLine(item), "print");
    /// // Asynchronous (compiler selects async overload)
    /// var sink = builder.AddSink(async (item, ct) => await LogAsync(item, ct), "log");
    /// </code>
    /// </example>
    public static SinkNodeHandle<TIn> AddSink<TIn>(
        this PipelineBuilder builder,
        Action<TIn> consume,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(consume);

        var nodeName = name ?? $"Sink_{typeof(TIn).Name}";
        var handle = builder.AddSink<LambdaSinkNode<TIn>, TIn>(nodeName);
        var node = new LambdaSinkNode<TIn>(consume);
        _ = builder.AddPreconfiguredNodeInstance(handle.Id, node);
        return handle;
    }

    /// <summary>
    ///     Adds a lambda-based asynchronous sink node for terminal operations that require I/O or other async work.
    /// </summary>
    /// <typeparam name="TIn">The input item type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="consume">
    ///     The asynchronous function that processes each input item.
    ///     The compiler selects this overload automatically.
    /// </param>
    /// <param name="name">An optional descriptive name for the node. If null, a default name is used.</param>
    /// <returns>A handle to the newly added sink node.</returns>
    /// <remarks>
    ///     <para>
    ///         This overload is automatically selected when you pass an async function.
    ///         The consume function receives a cancellation token and can respect it for graceful cancellation.
    ///     </para>
    /// </remarks>
    public static SinkNodeHandle<TIn> AddSink<TIn>(
        this PipelineBuilder builder,
        Func<TIn, CancellationToken, ValueTask> consume,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(consume);

        var nodeName = name ?? $"Sink_{typeof(TIn).Name}";
        var handle = builder.AddSink<LambdaSinkNode<TIn>, TIn>(nodeName);
        var node = new LambdaSinkNode<TIn>(consume);
        _ = builder.AddPreconfiguredNodeInstance(handle.Id, node);
        return handle;
    }
}

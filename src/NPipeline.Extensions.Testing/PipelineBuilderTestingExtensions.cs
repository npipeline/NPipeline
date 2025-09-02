using NPipeline.Graph;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Testing;

/// <summary>
///     Convenience extensions for registering <see cref="InMemorySourceNode{T}" /> instances with data in a fluent, concise way
///     during test pipeline construction.
/// </summary>
public static class PipelineBuilderTestingExtensions
{
    /// <summary>
    ///     Adds an <see cref="InMemorySourceNode{T}" /> to the pipeline and returns its handle.
    ///     A concrete <see cref="InMemorySourceNode{T}" /> instance is preconfigured and registered on the builder.
    /// </summary>
    public static SourceNodeHandle<T> AddInMemorySource<T>(this PipelineBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var handle = builder.AddSource<InMemorySourceNode<T>, T>();
        builder.AddPreconfiguredNodeInstance(handle.Id, new InMemorySourceNode<T>());
        return handle;
    }

    /// <summary>
    ///     Adds a named <see cref="InMemorySourceNode{T}" /> to the pipeline and returns its handle.
    /// </summary>
    public static SourceNodeHandle<T> AddInMemorySource<T>(this PipelineBuilder builder, string name)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(name);
        var handle = builder.AddSource<InMemorySourceNode<T>, T>(name);
        builder.AddPreconfiguredNodeInstance(handle.Id, new InMemorySourceNode<T>());
        return handle;
    }

    /// <summary>
    ///     Adds an <see cref="InMemorySourceNode{T}" /> populated with the provided <paramref name="items" /> (instance-seeded) and returns its handle.
    ///     Items are embedded directly in the node instance (context is NOT used).
    /// </summary>
    public static SourceNodeHandle<T> AddInMemorySource<T>(this PipelineBuilder builder, IEnumerable<T> items)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(items);
        var handle = builder.AddSource<InMemorySourceNode<T>, T>();
        builder.AddPreconfiguredNodeInstance(handle.Id, new InMemorySourceNode<T>(items));
        return handle;
    }

    /// <summary>
    ///     Adds a named <see cref="InMemorySourceNode{T}" /> populated with the provided <paramref name="items" /> (instance-seeded) and returns its handle.
    ///     Items are embedded directly in the node instance (context is NOT used).
    /// </summary>
    public static SourceNodeHandle<T> AddInMemorySource<T>(this PipelineBuilder builder, string name, IEnumerable<T> items)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(items);
        var handle = builder.AddSource<InMemorySourceNode<T>, T>(name);
        builder.AddPreconfiguredNodeInstance(handle.Id, new InMemorySourceNode<T>(items));
        return handle;
    }

    /// <summary>
    ///     Adds an InMemorySourceNode that will resolve its data from the <see cref="PipelineContext" /> at execution time.
    ///     Data is written to both the node-scoped key and the type-scoped key via <see cref="TestingContextExtensions.SetSourceData" />.
    ///     This uses the parameterless constructor (context-backed) to keep lineage scenarios consistent with context-provided data.
    /// </summary>
    public static SourceNodeHandle<T> AddInMemorySourceWithDataFromContext<T>(this PipelineBuilder builder, PipelineContext context, IEnumerable<T> items)
    {
        // Create the node definition and also preconfigure a parameterless instance (consistent with other AddInMemorySource overloads)
        var handle = builder.AddSource<InMemorySourceNode<T>, T>();
        var nodeInstance = new InMemorySourceNode<T>();
        builder.AddPreconfiguredNodeInstance(handle.Id, nodeInstance);
        context.SetSourceData(items, handle.Id);
        return handle;
    }

    /// <summary>
    ///     Adds a named context-backed InMemorySourceNode that pulls data from the <see cref="PipelineContext" /> at runtime.
    /// </summary>
    public static SourceNodeHandle<T> AddInMemorySourceWithDataFromContext<T>(this PipelineBuilder builder, PipelineContext context, string name,
        IEnumerable<T> items)
    {
        var handle = builder.AddSource<InMemorySourceNode<T>, T>(name);
        var nodeInstance = new InMemorySourceNode<T>();
        builder.AddPreconfiguredNodeInstance(handle.Id, nodeInstance);
        context.SetSourceData(items, handle.Id);
        return handle;
    }

    /// <summary>
    ///     Adds a pass-through transform node that casts items from <typeparamref name="TIn" /> to <typeparamref name="TOut" />.
    ///     This is a convenience wrapper around <c>builder.AddTransform&lt;PassThroughTransformNode&lt;TIn, TOut&gt;, TIn, TOut&gt;(name)</c>.
    /// </summary>
    public static TransformNodeHandle<TIn, TOut> AddPassThroughTransform<TIn, TOut>(this PipelineBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.AddTransform<PassThroughTransformNode<TIn, TOut>, TIn, TOut>();
    }

    /// <summary>
    ///     Adds a named pass-through transform node that casts items from <typeparamref name="TIn" /> to <typeparamref name="TOut" />.
    /// </summary>
    public static TransformNodeHandle<TIn, TOut> AddPassThroughTransform<TIn, TOut>(this PipelineBuilder builder, string name)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(name);
        return builder.AddTransform<PassThroughTransformNode<TIn, TOut>, TIn, TOut>(name);
    }

    /// <summary>
    ///     Adds an <see cref="InMemorySinkNode{T}" /> instance to the pipeline and returns its handle.
    ///     A concrete <see cref="InMemorySinkNode{T}" /> instance is preconfigured and registered on the builder so
    ///     tests can retrieve the collected items via the sink instance from the pipeline context.
    /// </summary>
    public static SinkNodeHandle<T> AddInMemorySink<T>(this PipelineBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var handle = builder.AddSink<InMemorySinkNode<T>, T>();
        var sink = new InMemorySinkNode<T>();
        builder.AddPreconfiguredNodeInstance(handle.Id, sink);
        return handle;
    }

    /// <summary>
    ///     Adds an <see cref="InMemorySinkNode{T}" /> instance to the pipeline and returns its handle.
    ///     The sink is registered in the provided context so it can be retrieved before execution.
    /// </summary>
    public static SinkNodeHandle<T> AddInMemorySink<T>(this PipelineBuilder builder, PipelineContext context)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(context);

        var handle = builder.AddSink<InMemorySinkNode<T>, T>();
        var sink = new InMemorySinkNode<T>();
        builder.AddPreconfiguredNodeInstance(handle.Id, sink);

        // Register the sink in the context immediately
        sink.RegisterInContext(context);

        return handle;
    }

    /// <summary>
    ///     Adds a named <see cref="InMemorySinkNode{T}" /> instance to the pipeline and returns its handle.
    /// </summary>
    public static SinkNodeHandle<T> AddInMemorySink<T>(this PipelineBuilder builder, string name)
    {
        var handle = builder.AddSink<InMemorySinkNode<T>, T>(name);
        var sink = new InMemorySinkNode<T>();
        builder.AddPreconfiguredNodeInstance(handle.Id, sink);
        return handle;
    }

    /// <summary>
    ///     Adds a named <see cref="InMemorySinkNode{T}" /> instance to the pipeline and returns its handle.
    ///     The sink is registered in the provided context so it can be retrieved before execution.
    /// </summary>
    public static SinkNodeHandle<T> AddInMemorySink<T>(this PipelineBuilder builder, string name, PipelineContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        var handle = builder.AddSink<InMemorySinkNode<T>, T>(name);
        var sink = new InMemorySinkNode<T>();
        builder.AddPreconfiguredNodeInstance(handle.Id, sink);

        // Register the sink in the context immediately
        sink.RegisterInContext(context);

        return handle;
    }
}

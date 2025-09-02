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
        builder.AddPreconfiguredNodeInstance(handle.Id, node);
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
        builder.AddPreconfiguredNodeInstance(handle.Id, node);
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
        builder.AddPreconfiguredNodeInstance(handle.Id, node);
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

        builder.AddPreconfiguredNodeInstance(handle.Id, node);
        return handle;
    }
}

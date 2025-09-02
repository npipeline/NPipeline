using NPipeline.Graph;
using NPipeline.Nodes;

namespace NPipeline.Pipeline;

/// <summary>
///     Extensions for adding batching and unbatching nodes to the pipeline builder.
///     Restores the API surface used by NPipeline.Benchmarks (AddBatcher/AddUnbatcher).
/// </summary>
public static class BatchingPipelineBuilderExtensions
{
    /// <summary>
    ///     Adds a batching node that groups incoming items into IReadOnlyCollection batches,
    ///     configured by batch size and a maximum time window.
    /// </summary>
    public static TransformNodeHandle<T, IReadOnlyCollection<T>> AddBatcher<T>(
        this PipelineBuilder builder,
        string name,
        int batchSize,
        TimeSpan timespan)
    {
        var handle = builder.AddTransform<BatchingNode<T>, T, IReadOnlyCollection<T>>(name);
        var node = new BatchingNode<T>(batchSize, timespan);
        builder.RegisterBuilderDisposable(node);
        builder.AddPreconfiguredNodeInstance(handle.Id, node);
        return handle;
    }

    /// <summary>
    ///     Adds an unbatching node that flattens sequences (IEnumerable&lt;T&gt;) back into individual items (T).
    /// </summary>
    public static TransformNodeHandle<IEnumerable<T>, T> AddUnbatcher<T>(
        this PipelineBuilder builder,
        string name)
    {
        var handle = builder.AddTransform<UnbatchingNode<T>, IEnumerable<T>, T>(name);
        var node = new UnbatchingNode<T>();
        builder.RegisterBuilderDisposable(node);
        builder.AddPreconfiguredNodeInstance(handle.Id, node);
        return handle;
    }
}

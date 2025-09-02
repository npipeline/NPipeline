using NPipeline.Graph;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Parallelism;

/// <summary>
///     Policy applied when the parallel input queue reaches its capacity.
/// </summary>
public enum BoundedQueuePolicy
{
    Block = 0,
    DropNewest = 1,
    DropOldest = 2,
}

/// <summary>
///     Configuration options for <see cref="ParallelExecutionStrategy" /> per node.
/// </summary>
/// <param name="MaxDegreeOfParallelism">Overrides the maximum degree of parallelism (defaults to Environment.ProcessorCount when null).</param>
/// <param name="MaxQueueLength">
///     Optional bounded input queue length for backpressure control. When null, input enumeration can run ahead (bounded only by
///     upstream).
/// </param>
/// <param name="QueuePolicy">Behavior when the input queue is full (only applies when <see cref="MaxQueueLength" /> is set).</param>
/// <param name="OutputBufferCapacity">
///     Optional maximum number of processed results allowed to queue ahead of downstream consumption when using the Block policy.
///     When specified, the strategy uses an internal bounded channel to throttle workers, restoring end-to-end backpressure. Null means unbounded output buffering
///     (default).
/// </param>
/// <param name="PreserveOrdering">
///     When true (default), the Dataflow-based execution path preserves input ordering in its output. When false, ordering is not preserved,
///     which can increase throughput. Note: drop-policy paths are inherently unordered.
/// </param>
public sealed record ParallelOptions(
    int? MaxDegreeOfParallelism = null,
    int? MaxQueueLength = null,
    BoundedQueuePolicy QueuePolicy = BoundedQueuePolicy.Block,
    int? OutputBufferCapacity = null,
    bool PreserveOrdering = true);

public static class ParallelPipelineBuilderExtensions
{
    /// <summary>
    ///     Attaches parallel execution options to a transform node. These are consumed when the node's execution strategy is
    ///     <see cref="ParallelExecutionStrategy" />.
    /// </summary>
    public static PipelineBuilder WithParallelOptions(this PipelineBuilder builder, NodeHandle handle, ParallelOptions options)
    {
        builder.SetNodeExecutionOption(handle.Id, options);
        return builder;
    }
}

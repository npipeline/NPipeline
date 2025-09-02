using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Parallelism;

/// <summary>
///     Default parallel execution strategy with queue policy selection support.
///     This class provides backward compatibility by instantiating the appropriate concrete strategy
///     (BlockingParallelStrategy, DropOldestParallelStrategy, or DropNewestParallelStrategy) based on
///     the QueuePolicy specified in ParallelOptions during ExecuteAsync.
/// </summary>
public sealed class ParallelExecutionStrategy : BlockingParallelStrategy
{
    /// <summary>
    ///     Creates a new parallel execution strategy with queue policy selection.
    /// </summary>
    public ParallelExecutionStrategy()
    {
    }

    /// <summary>
    ///     Creates a new parallel execution strategy with specified degree of parallelism and queue policy selection.
    /// </summary>
    /// <param name="maxDegreeOfParallelism">The maximum degree of parallelism.</param>
    public ParallelExecutionStrategy(int maxDegreeOfParallelism) : base(maxDegreeOfParallelism)
    {
    }

    /// <summary>
    ///     Executes the input items through the transform node with parallelism, selecting the appropriate
    ///     queue policy implementation based on ParallelOptions in the context.
    /// </summary>
    public override async Task<IDataPipe<TOut>> ExecuteAsync<TIn, TOut>(
        IDataPipe<TIn> input,
        ITransformNode<TIn, TOut> node,
        PipelineContext context,
        CancellationToken cancellationToken = default)
    {
        // Determine which queue policy to use
        var nodeId = context.CurrentNodeId;
        var queuePolicy = BoundedQueuePolicy.Block;

        if (context.Items.TryGetValue(PipelineContextKeys.NodeExecutionOptions(nodeId), out var opt) && opt is ParallelOptions po)
            queuePolicy = po.QueuePolicy;

        // Delegate to the appropriate strategy
        return queuePolicy switch
        {
            BoundedQueuePolicy.Block => await base.ExecuteAsync(input, node, context, cancellationToken),
            BoundedQueuePolicy.DropOldest =>
                await new DropOldestParallelStrategy(ConfiguredMaxDop).ExecuteAsync(input, node, context, cancellationToken),
            BoundedQueuePolicy.DropNewest =>
                await new DropNewestParallelStrategy(ConfiguredMaxDop).ExecuteAsync(input, node, context, cancellationToken),
            _ => await base.ExecuteAsync(input, node, context, cancellationToken),
        };
    }

    /// <summary>
    ///     Creates a new parallel execution strategy based on the provided options.
    /// </summary>
    /// <param name="options">Configuration options for the strategy, or null to use defaults (blocking policy, processor count DOP).</param>
    /// <returns>An IExecutionStrategy implementation with the requested configuration.</returns>
    public static IExecutionStrategy Create(ParallelOptions? options = null)
    {
        return options?.QueuePolicy switch
        {
            BoundedQueuePolicy.DropOldest => new DropOldestParallelStrategy(options.MaxDegreeOfParallelism),
            BoundedQueuePolicy.DropNewest => new DropNewestParallelStrategy(options.MaxDegreeOfParallelism),
            _ => new BlockingParallelStrategy(options?.MaxDegreeOfParallelism),
        };
    }
}

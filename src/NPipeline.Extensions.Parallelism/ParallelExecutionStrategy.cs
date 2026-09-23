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
    private DropNewestParallelStrategy? _dropNewest;
    private DropOldestParallelStrategy? _dropOldest;

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
    public override async Task<IDataStream<TOut>> ExecuteAsync<TIn, TOut>(
        IDataStream<TIn> input,
        ITransformNode<TIn, TOut> node,
        PipelineContext context,
        string nodeId,
        CancellationToken cancellationToken = default)
    {
        // Determine which queue policy to use
        var queuePolicy = BoundedQueuePolicy.Block;

        if (context.NodeEnvironment.NodeExecutionScopeRegistry.TryGetNodeExecutionAnnotation(nodeId, out var opt) && opt is ParallelOptions po)
            queuePolicy = po.QueuePolicy;

        // Delegate to the appropriate strategy. Drop strategies are stateless and cached per instance
        // to avoid per-call allocations on repeated pipeline runs.
        return queuePolicy switch
        {
            BoundedQueuePolicy.Block => await base.ExecuteAsync(input, node, context, nodeId, cancellationToken).ConfigureAwait(false),
            BoundedQueuePolicy.DropOldest =>
                await (_dropOldest ??= new DropOldestParallelStrategy(ConfiguredMaxDop)).ExecuteAsync(input, node, context, nodeId, cancellationToken).ConfigureAwait(false),
            BoundedQueuePolicy.DropNewest =>
                await (_dropNewest ??= new DropNewestParallelStrategy(ConfiguredMaxDop)).ExecuteAsync(input, node, context, nodeId, cancellationToken).ConfigureAwait(false),
            _ => await base.ExecuteAsync(input, node, context, nodeId, cancellationToken).ConfigureAwait(false),
        };
    }

    /// <summary>
    ///     Resumes the node through the implementation for its queue policy, as <see cref="ExecuteAsync{TIn,TOut}" /> selects it.
    /// </summary>
    public override async Task<IDataStream<TOut>> ExecuteFromAsync<TIn, TOut>(
        IDataStream<TIn> input,
        long offset,
        RestartCheckpoint checkpoint,
        ITransformNode<TIn, TOut> node,
        PipelineContext context,
        string nodeId,
        CancellationToken cancellationToken)
    {
        var queuePolicy = BoundedQueuePolicy.Block;

        if (context.NodeEnvironment.NodeExecutionScopeRegistry.TryGetNodeExecutionAnnotation(nodeId, out var opt) && opt is ParallelOptions po)
            queuePolicy = po.QueuePolicy;

        return queuePolicy switch
        {
            BoundedQueuePolicy.DropOldest =>
                await (_dropOldest ??= new DropOldestParallelStrategy(ConfiguredMaxDop))
                    .ExecuteFromAsync(input, offset, checkpoint, node, context, nodeId, cancellationToken).ConfigureAwait(false),
            BoundedQueuePolicy.DropNewest =>
                await (_dropNewest ??= new DropNewestParallelStrategy(ConfiguredMaxDop))
                    .ExecuteFromAsync(input, offset, checkpoint, node, context, nodeId, cancellationToken).ConfigureAwait(false),
            _ => await base.ExecuteFromAsync(input, offset, checkpoint, node, context, nodeId, cancellationToken).ConfigureAwait(false),
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

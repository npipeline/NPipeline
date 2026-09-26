using System.Runtime.CompilerServices;
using System.Threading.Channels;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Execution.Lineage;
using NPipeline.Execution.Services;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Parallelism;

/// <summary>
///     Base class for parallel execution strategies. Items are transformed by the same item executor as the
///     sequential strategy, so retry, backoff, and failure handling behave identically.
/// </summary>
/// <remarks>
///     Parallel strategies are resumable, so node restart works with them. An ordered strategy delivers each output
///     exactly once across restarts. An unordered or dropping strategy delivers at least once: after a restart,
///     outputs that were delivered ahead of the checkpoint are delivered again, at most the number in flight at the
///     failure.
/// </remarks>
public abstract class ParallelExecutionStrategyBase(int? maxDegreeOfParallelism = null) : IResumableExecutionStrategy, ILineageProvenanceStrategy
{
    /// <summary>
    ///     Gets the configured maximum degree of parallelism for the strategy.
    /// </summary>
    protected int? ConfiguredMaxDop { get; } = maxDegreeOfParallelism;

    /// <inheritdoc />
    /// <remarks>
    ///     Only the strategies in this assembly report: a subclass that replaces how outputs are produced would not.
    /// </remarks>
    bool ILineageProvenanceStrategy.ReportsLineageProvenance(INode node)
    {
        var type = GetType();

        return type == typeof(BlockingParallelStrategy) || type == typeof(ParallelExecutionStrategy)
                                                        || type == typeof(DropOldestParallelStrategy)
                                                        || type == typeof(DropNewestParallelStrategy);
    }

    /// <summary>
    ///     Executes a transform node with parallel processing strategy.
    /// </summary>
    /// <typeparam name="TIn">The type of input data.</typeparam>
    /// <typeparam name="TOut">The type of output data.</typeparam>
    /// <param name="input">The input data pipe.</param>
    /// <param name="node">The transform node to execute.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="nodeId">The id of the node being executed, passed explicitly rather than read from the shared context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation with the output data pipe.</returns>
    public abstract Task<IDataStream<TOut>> ExecuteAsync<TIn, TOut>(IDataStream<TIn> input, ITransformNode<TIn, TOut> node, PipelineContext context,
        string nodeId, CancellationToken cancellationToken);

    /// <inheritdoc />
    public abstract Task<IDataStream<TOut>> ExecuteFromAsync<TIn, TOut>(IDataStream<TIn> input, long offset, RestartCheckpoint checkpoint,
        ITransformNode<TIn, TOut> node, PipelineContext context, string nodeId, CancellationToken cancellationToken);

    /// <summary>
    ///     Records that the input item at <paramref name="inputIndex" /> was dropped by the bounded queue, so it will
    ///     never produce an output.
    /// </summary>
    private protected static void ReportDropped(LineageNodeOutcomeWriter lineage, long inputIndex)
    {
        lineage.Record(inputIndex, LineageOutcomeReason.DroppedByBackpressure, 0);
        lineage.ReportDone(inputIndex, LineageOutcomeReason.DroppedByBackpressure);
    }

    /// <summary>
    ///     Transforms one work item through the core item executor, which applies the node's item retry, backoff,
    ///     resilience policy, dead-lettering, and lineage outcome.
    /// </summary>
    private protected static ValueTask<ItemExecutionResult<TOut>> ExecuteItemAsync<TIn, TOut>(
        IndexedWorkItem<TIn> work,
        ITransformNode<TIn, TOut> node,
        PipelineContext context,
        CachedNodeExecutionContext cached,
        Action<int>? onRetry) =>

        // Without tracing there is no activity to dispose, so return the executor's task and skip a state machine per item.
        cached.TracingEnabled
            ? ExecuteTracedAsync(work, node, context, cached, onRetry)
            : ExecuteCoreAsync(work, node, context, cached, null, onRetry);

    private static async ValueTask<ItemExecutionResult<TOut>> ExecuteTracedAsync<TIn, TOut>(
        IndexedWorkItem<TIn> work,
        ITransformNode<TIn, TOut> node,
        PipelineContext context,
        CachedNodeExecutionContext cached,
        Action<int>? onRetry)
    {
        using var itemActivity = context.Observability.Tracer.StartActivity("Item.Transform");
        return await ExecuteCoreAsync(work, node, context, cached, itemActivity, onRetry).ConfigureAwait(false);
    }

    private static ValueTask<ItemExecutionResult<TOut>> ExecuteCoreAsync<TIn, TOut>(
        IndexedWorkItem<TIn> work,
        ITransformNode<TIn, TOut> node,
        PipelineContext context,
        CachedNodeExecutionContext cached,
        IPipelineActivity? itemActivity,
        Action<int>? onRetry) =>
        PerItemRetryExecutor.Instance.ExecuteWithRetryAsync(
            work.Item,
            node,
            context,
            cached.NodeId,
            cached.Resilience,
            work.LineageInputIndex.HasValue,
            work.LineageInputIndex.GetValueOrDefault(),
            cached.LineageOutcomeWriter,
            itemActivity,
            cached.CancellationToken,
            onRetry,
            cached.CircuitBreaker);

    /// <summary>
    ///     Creates worker tasks that drain items from the queue and process them in parallel.
    /// </summary>
    /// <typeparam name="TIn">The type of input data.</typeparam>
    /// <typeparam name="TOut">The type of output data.</typeparam>
    /// <param name="reader">The channel reader to drain items from.</param>
    /// <param name="node">The transform node to execute.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cachedContext">The cached execution context with pre-resolved configuration.</param>
    /// <param name="metrics">The metrics tracker.</param>
    /// <param name="effectiveDop">The effective degree of parallelism.</param>
    /// <param name="checkpoint">
    ///     Where to report items that produce no output (skipped or dead-lettered), when the node is restartable;
    ///     otherwise <see langword="null" />. Item sequence numbers are indexes in the node's input.
    /// </param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <param name="onFault">
    ///     Called with a worker's failure before the worker's task faults, so the caller can stop the feeder and the
    ///     other workers instead of waiting for the input to drain.
    /// </param>
    /// <returns>A tuple containing the output channel and the list of worker tasks.</returns>
    protected static (Channel<IndexedResult<TOut>> OutChannel, List<Task> Workers) CreateWorkerTasks<TIn, TOut>(
        ChannelReader<IndexedWorkItem<TIn>> reader,
        ITransformNode<TIn, TOut> node,
        PipelineContext context,
        CachedNodeExecutionContext cachedContext,
        ParallelExecutionMetrics metrics,
        int effectiveDop,
        RestartCheckpoint? checkpoint,
        CancellationToken cancellationToken,
        Action<Exception>? onFault = null)
    {
        var outChannel = Channel.CreateUnbounded<IndexedResult<TOut>>();
        var workers = new List<Task>(effectiveDop);
        var onRetry = metrics.RecordRetry;

        for (var i = 0; i < effectiveDop; i++)
        {
            workers.Add(Task.Run(async () =>
            {
                try
                {
                    await foreach (var next in reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                    {
                        var result = await ExecuteItemAsync(next, node, context, cachedContext, onRetry).ConfigureAwait(false);

                        if (result.Produced)
                        {
                            _ = metrics.IncrementProcessed();
                            await outChannel.Writer.WriteAsync(new IndexedResult<TOut>(next.Sequence, result.Output!), cancellationToken).ConfigureAwait(false);
                        }
                        else
                            checkpoint?.Complete(next.Sequence);
                    }
                }
                catch (Exception ex)
                {
                    onFault?.Invoke(ex);
                    throw;
                }
            }, cancellationToken));
        }

        return (outChannel, workers);
    }

    /// <summary>
    ///     Creates an async enumerable that reads from the output channel and emits metrics on completion.
    /// </summary>
    /// <typeparam name="TOut">The type of output data.</typeparam>
    /// <param name="outChannel">The output channel to read from.</param>
    /// <param name="nodeId">The node identifier for metrics tagging.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="metrics">The metrics tracker.</param>
    /// <param name="currentActivity">The current tracing activity.</param>
    /// <param name="observabilityScope">Observability scope handle for recording item counts and scope disposal.</param>
    /// <param name="checkpoint">Where to report delivered outputs when the node is restartable; otherwise <see langword="null" />.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An async enumerable of output items.</returns>
    protected static IAsyncEnumerable<TOut> CreateOutputEnumerable<TOut>(
        Channel<IndexedResult<TOut>> outChannel,
        string nodeId,
        PipelineContext context,
        ParallelExecutionMetrics metrics,
        IPipelineActivity? currentActivity,
        IAutoObservabilityScope observabilityScope,
        RestartCheckpoint? checkpoint,
        CancellationToken cancellationToken) =>
        CreateOutputEnumerable(outChannel, nodeId, context, metrics, currentActivity, observabilityScope, checkpoint, default,
            cancellationToken);

    /// <summary>
    ///     Creates an async enumerable that reads from the output channel and emits metrics on completion.
    /// </summary>
    /// <typeparam name="TOut">The type of output data.</typeparam>
    /// <param name="outChannel">The output channel to read from.</param>
    /// <param name="nodeId">The node identifier for metrics tagging.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="metrics">The metrics tracker.</param>
    /// <param name="currentActivity">The current tracing activity.</param>
    /// <param name="observabilityScope">Observability scope handle for recording item counts and scope disposal.</param>
    /// <param name="checkpoint">Where to report delivered outputs when the node is restartable; otherwise <see langword="null" />.</param>
    /// <param name="lineage">
    ///     The node's lineage state, to report which input item each output came from. Item sequence numbers must
    ///     be indexes in the node's input.
    /// </param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An async enumerable of output items.</returns>
    private protected static async IAsyncEnumerable<TOut> CreateOutputEnumerable<TOut>(
        Channel<IndexedResult<TOut>> outChannel,
        string nodeId,
        PipelineContext context,
        ParallelExecutionMetrics metrics,
        IPipelineActivity? currentActivity,
        IAutoObservabilityScope observabilityScope,
        RestartCheckpoint? checkpoint,
        LineageNodeOutcomeWriter lineage,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var scope = observabilityScope;

        try
        {
            await foreach (var item in outChannel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                scope.IncrementEmitted();
                lineage.ReportOutput(item.Sequence);
                yield return item.Value;

                // Reached once the consumer asks for the next output, so this one has been delivered.
                checkpoint?.Complete(item.Sequence);
            }
        }
        finally
        {
            // Tag drop and queue metrics on the current activity for observability
            currentActivity?.SetTag("parallel.dropped.newest", metrics.DroppedNewest);
            currentActivity?.SetTag("parallel.dropped.oldest", metrics.DroppedOldest);
            currentActivity?.SetTag("parallel.enqueued", metrics.Enqueued);
            currentActivity?.SetTag("parallel.processed", metrics.Processed);

            // Store metrics in context runtime annotations for downstream monitoring.
            context.NodeEnvironment.NodeExecutionScopeRegistry.SetRuntimeAnnotation(PipelineContextKeys.ParallelMetricsDroppedNewest(nodeId),
                metrics.DroppedNewest);

            context.NodeEnvironment.NodeExecutionScopeRegistry.SetRuntimeAnnotation(PipelineContextKeys.ParallelMetricsDroppedOldest(nodeId),
                metrics.DroppedOldest);

            context.NodeEnvironment.NodeExecutionScopeRegistry.SetRuntimeAnnotation(PipelineContextKeys.ParallelMetricsEnqueued(nodeId), metrics.Enqueued);
            context.NodeEnvironment.NodeExecutionScopeRegistry.SetRuntimeAnnotation(PipelineContextKeys.ParallelMetricsProcessed(nodeId), metrics.Processed);
        }
    }

    /// <summary>
    ///     Attempts to retrieve the auto-observability scope for a node if one was configured.
    /// </summary>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="nodeId">The node identifier.</param>
    /// <returns>The configured scope handle, or a no-op scope when observability is not enabled.</returns>
    protected static IAutoObservabilityScope BeginNodeObservabilityScope(PipelineContext context, string nodeId) =>
        context.NodeEnvironment.NodeExecutionScopeRegistry.BeginNodeScope(nodeId);

    /// <summary>
    ///     Work item wrapper carrying optional lineage input index for per-item outcome correlation.
    /// </summary>
    /// <typeparam name="T">The input item type.</typeparam>
    /// <param name="Item">The actual item payload.</param>
    /// <param name="LineageInputIndex">
    ///     The item's index in the node's input when the node tracks lineage, which the item's lineage is keyed by;
    ///     otherwise <see langword="null" />.
    /// </param>
    /// <param name="Sequence">Monotonically increasing input sequence number used to restore ordering.</param>
    protected readonly record struct IndexedWorkItem<T>(T Item, long? LineageInputIndex, long Sequence = 0);

    /// <summary>
    ///     A worker's output, carrying the input sequence number of the item it came from.
    /// </summary>
    /// <typeparam name="T">The output item type.</typeparam>
    /// <param name="Sequence">The <see cref="IndexedWorkItem{T}.Sequence" /> of the item that produced the output.</param>
    /// <param name="Value">The output.</param>
    protected readonly record struct IndexedResult<T>(long Sequence, T Value);
}

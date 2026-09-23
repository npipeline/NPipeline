using System.Runtime.CompilerServices;
using System.Threading.Channels;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Execution.Services;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Parallelism
{

    /// <summary>
    ///     Base class for parallel execution strategies. Items are transformed by the same item executor as the
    ///     sequential strategy, so retry, backoff, and failure handling behave identically.
    /// </summary>
    public abstract class ParallelExecutionStrategyBase(int? maxDegreeOfParallelism = null) : IExecutionStrategy
    {
        /// <summary>
        /// Work item wrapper carrying optional lineage input index for per-item outcome correlation.
        /// </summary>
        /// <typeparam name="T">The input item type.</typeparam>
        /// <param name="Item">The actual item payload.</param>
        /// <param name="LineageInputIndex">Optional lineage input index associated with the item.</param>
        /// <param name="CorrelationId">Optional correlation identifier associated with the item.</param>
        /// <param name="AncestryInputIndices">Optional contributor indices associated with the item.</param>
        /// <param name="Sequence">Monotonically increasing input sequence number used to restore ordering.</param>
        protected readonly record struct IndexedWorkItem<T>(T Item, long? LineageInputIndex, Guid? CorrelationId = null,
            int[]? AncestryInputIndices = null, long Sequence = 0);

        /// <summary>
        ///     Gets the configured maximum degree of parallelism for the strategy.
        /// </summary>
        protected int? ConfiguredMaxDop { get; } = maxDegreeOfParallelism;

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

        /// <summary>
        ///     Transforms one work item through the core item executor, which applies the node's item retry, backoff,
        ///     resilience policy, dead-lettering, and lineage outcome.
        /// </summary>
        private protected static Task<ItemExecutionResult<TOut>> ExecuteItemAsync<TIn, TOut>(
            IndexedWorkItem<TIn> work,
            ITransformNode<TIn, TOut> node,
            PipelineContext context,
            CachedNodeExecutionContext cached,
            Action<int>? onRetry)
        {
            // Without tracing there is no activity to dispose, so return the executor's task and skip a state machine per item.
            return cached.TracingEnabled
                ? ExecuteTracedAsync(work, node, context, cached, onRetry)
                : ExecuteCoreAsync(work, node, context, cached, null, onRetry);
        }

        private static async Task<ItemExecutionResult<TOut>> ExecuteTracedAsync<TIn, TOut>(
            IndexedWorkItem<TIn> work,
            ITransformNode<TIn, TOut> node,
            PipelineContext context,
            CachedNodeExecutionContext cached,
            Action<int>? onRetry)
        {
            using var itemActivity = context.Observability.Tracer.StartActivity("Item.Transform");
            return await ExecuteCoreAsync(work, node, context, cached, itemActivity, onRetry).ConfigureAwait(false);
        }

        private static Task<ItemExecutionResult<TOut>> ExecuteCoreAsync<TIn, TOut>(
            IndexedWorkItem<TIn> work,
            ITransformNode<TIn, TOut> node,
            PipelineContext context,
            CachedNodeExecutionContext cached,
            IPipelineActivity? itemActivity,
            Action<int>? onRetry)
        {
            return PerItemRetryExecutor.Instance.ExecuteWithRetryAsync(
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
                work.CorrelationId,
                work.AncestryInputIndices,
                onRetry);
        }

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
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A tuple containing the output channel and the list of worker tasks.</returns>
        protected static (Channel<TOut> OutChannel, List<Task> Workers) CreateWorkerTasks<TIn, TOut>(
            ChannelReader<IndexedWorkItem<TIn>> reader,
            ITransformNode<TIn, TOut> node,
            PipelineContext context,
            CachedNodeExecutionContext cachedContext,
            ParallelExecutionMetrics metrics,
            int effectiveDop,
            CancellationToken cancellationToken)
        {
            var outChannel = Channel.CreateUnbounded<TOut>();
            var workers = new List<Task>(effectiveDop);
            Action<int> onRetry = metrics.RecordRetry;

            for (var i = 0; i < effectiveDop; i++)
            {
                workers.Add(Task.Run(async () =>
                {
                    await foreach (var next in reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                    {
                        var result = await ExecuteItemAsync(next, node, context, cachedContext, onRetry).ConfigureAwait(false);

                        if (result.Produced)
                        {
                            _ = metrics.IncrementProcessed();
                            await outChannel.Writer.WriteAsync(result.Output!, cancellationToken).ConfigureAwait(false);
                        }
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
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>An async enumerable of output items.</returns>
        protected static async IAsyncEnumerable<TOut> CreateOutputEnumerable<TOut>(
            Channel<TOut> outChannel,
            string nodeId,
            PipelineContext context,
            ParallelExecutionMetrics metrics,
            IPipelineActivity? currentActivity,
            IAutoObservabilityScope observabilityScope,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            using var scope = observabilityScope;

            try
            {
                await foreach (var item in outChannel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                {
                    scope.IncrementEmitted();
                    yield return item;
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
        protected static IAutoObservabilityScope BeginNodeObservabilityScope(PipelineContext context, string nodeId)
        {
            return context.NodeEnvironment.NodeExecutionScopeRegistry.BeginNodeScope(nodeId);
        }
    }
}

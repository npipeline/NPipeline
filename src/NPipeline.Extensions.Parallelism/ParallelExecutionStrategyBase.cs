using System.Runtime.CompilerServices;
using System.Threading.Channels;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Annotations;
using NPipeline.Execution.Lineage;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;
using NPipeline.Resilience;
using NPipeline.Sampling;

namespace NPipeline.Extensions.Parallelism
{

    /// <summary>
    ///     Base class for parallel execution strategies with common retry and handler logic.
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
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the output data pipe.</returns>
        public abstract Task<IDataStream<TOut>> ExecuteAsync<TIn, TOut>(IDataStream<TIn> input, ITransformNode<TIn, TOut> node, PipelineContext context,
            CancellationToken cancellationToken);

        /// <summary>
        ///     Gets effective retry options for a node, checking per-node, global, and context fallback.
        /// </summary>
        /// <param name="nodeId">The identifier of the node.</param>
        /// <param name="context">The pipeline execution context.</param>
        /// <returns>The effective retry options to use for the node.</returns>
        protected static PipelineRetryOptions GetRetryOptions(string nodeId, PipelineContext context)
        {
            var logger = context.LoggerFactory.CreateLogger(nameof(ParallelExecutionStrategyBase));

            // Check for per-node retry options first
            if (context.NodeRetryOverrides.TryGetValue(nodeId, out var nodeOptions))
            {
                ParallelExecutionStrategyLogMessages.PerNodeRetryOptionsFound(logger, nodeId, nodeOptions.MaxItemRetries);
                return nodeOptions;
            }

            // Check for global retry options stored by PipelineRunner
            var globalRetryOptions = context.GlobalRetryOptions;
            ParallelExecutionStrategyLogMessages.GlobalRetryOptionsUsed(logger, nodeId, globalRetryOptions.MaxItemRetries);
            return globalRetryOptions;
        }

        /// <summary>
        ///     Executes a transform node on an item with retry logic and error handling using a cached execution context.
        /// </summary>
        /// <typeparam name="TIn">The type of input data.</typeparam>
        /// <typeparam name="TOut">The type of output data.</typeparam>
        /// <param name="item">The item to process.</param>
        /// <param name="node">The transform node to execute.</param>
        /// <param name="context">The pipeline execution context.</param>
        /// <param name="cached">The cached execution context with pre-resolved configuration.</param>
        /// <param name="metrics">Optional metrics for tracking execution.</param>
        /// <param name="observer">Optional observer for execution events.</param>
        /// <param name="lineageInputIndex">Optional lineage input index used to correlate per-item outcomes.</param>
        /// <param name="correlationId">Optional correlation identifier used to correlate per-item errors.</param>
        /// <param name="ancestryInputIndices">Optional contributor indices associated with the current item.</param>
        /// <returns>A task representing the asynchronous operation with the processed item, or null if skipped.</returns>
        /// <remarks>
        ///     This overload accepts a pre-created <see cref="CachedNodeExecutionContext" /> to avoid
        ///     per-item dictionary lookups and allocations, improving performance for high-throughput scenarios.
        /// </remarks>
        protected static async Task<TOut?> ExecuteWithRetryAsync<TIn, TOut>(
            TIn item,
            ITransformNode<TIn, TOut> node,
            PipelineContext context,
            CachedNodeExecutionContext cached,
            ParallelExecutionMetrics? metrics = null,
            IExecutionObserver? observer = null,
            long? lineageInputIndex = null,
            Guid? correlationId = null,
            int[]? ancestryInputIndices = null)
        {
            var logger = cached.LoggingEnabled
                ? context.LoggerFactory.CreateLogger(nameof(ParallelExecutionStrategyBase))
                : null;

            using var itemActivity = cached.TracingEnabled
                ? context.Tracer.StartActivity("Item.Transform")
                : null;

            var attempt = 0;

            while (true)
            {
                cached.CancellationToken.ThrowIfCancellationRequested();

                try
                {
                    var output = await ExecuteNodeAsync(node, item, context, cached.CancellationToken).ConfigureAwait(false);
                    RecordLineageOutcome(lineageInputIndex, context, in cached, LineageOutcomeReason.Emitted, attempt);
                    return output;
                }
                catch (Exception ex)
                {
                    itemActivity?.RecordException(ex);

                    if (logger is not null)
                    {
                        ParallelExecutionStrategyLogMessages.NodeFailure(logger, ex, cached.NodeId, attempt + 1);
                    }

                    var policy = ResolveResiliencePolicy(context, cached.NodeId);

                    var decision = await policy
                        .DecideItemFailureAsync(node, item, ex, context, cached.NodeId, attempt, cached.CancellationToken)
                        .ConfigureAwait(false);

                    switch (decision)
                    {
                        case ResilienceDecision.Skip:
                            RecordLineageOutcome(lineageInputIndex, context, in cached, LineageOutcomeReason.FilteredOut, attempt);
                            return default;
                        case ResilienceDecision.DeadLetter:
                            await TryDispatchDeadLetterAsync(item, ex, context, cached.NodeId, attempt, cached.CancellationToken)
                                .ConfigureAwait(false);
                            RecordLineageOutcome(lineageInputIndex, context, in cached, LineageOutcomeReason.DeadLettered,
                                attempt);
                            return default;
                        case ResilienceDecision.Retry:
                            if (attempt >= cached.RetryOptions.MaxItemRetries)
                            {
                                PipelineSampleErrorReporter.TryRecordError(context, cached.NodeId, item, ex, cached.RetryOptions.MaxItemRetries,
                                    correlationId, ancestryInputIndices);
                                RecordLineageOutcome(lineageInputIndex, context, in cached, LineageOutcomeReason.Error,
                                    cached.RetryOptions.MaxItemRetries);
                                throw;
                            }

                            attempt++;
                            itemActivity?.SetTag("retry.attempt", attempt.ToString());
                            PublishRetryInstrumentation(metrics, observer, context, cached.NodeId, attempt, ex);
                            continue;
                        case ResilienceDecision.Fail:
                            PipelineSampleErrorReporter.TryRecordError(context, cached.NodeId, item, ex, attempt, correlationId, ancestryInputIndices);
                            RecordLineageOutcome(lineageInputIndex, context, in cached, LineageOutcomeReason.Error, attempt);
                            throw;
                        default:
                            throw new InvalidOperationException($"Error handling failed for node {cached.NodeId} with decision {decision}.", ex);
                    }
                }
            }
        }

        private static ValueTask<TOut> ExecuteNodeAsync<TIn, TOut>(ITransformNode<TIn, TOut> node, TIn item, PipelineContext context,
            CancellationToken cancellationToken)
        {
            return node is IValueTaskTransform<TIn, TOut> fastPath
                ? fastPath.ExecuteValueTaskAsync(item, context, cancellationToken)
                : new ValueTask<TOut>(node.TransformAsync(item, context, cancellationToken));
        }

        private static void PublishRetryInstrumentation(ParallelExecutionMetrics? metrics, IExecutionObserver? observer, PipelineContext context,
            string nodeId, int attempt, Exception exception)
        {
            metrics?.RecordRetry(attempt);
            observer?.OnRetry(new NodeRetryEvent(nodeId, RetryKind.ItemRetry, attempt, exception, context.PipelineId, context.PipelineName));
        }

        private static IResiliencePolicy ResolveResiliencePolicy(PipelineContext context, string nodeId)
        {
            var key = ExecutionAnnotationKeys.NodeResiliencePolicyForNode(nodeId);

            if (context.NodeExecutionScopeRegistry.TryGetRuntimeAnnotation(key, out var annotation) && annotation is IResiliencePolicy nodePolicy)
                return nodePolicy;

            return context.ResiliencePolicy;
        }

        private static void RecordLineageOutcome(long? lineageInputIndex, PipelineContext context, in CachedNodeExecutionContext cached,
            LineageOutcomeReason outcomeReason, int retryCount)
        {
            if (lineageInputIndex is null)
            {
                return;
            }

            var writer = cached.LineageOutcomeWriter;

            if (writer.IsActive)
            {
                // Fast path: the node's outcome store was resolved once when the cached context was created,
                // so this avoids the per-item (pipeline, node) lookup that is otherwise multiplied by the
                // degree of parallelism on the worker threads.
                writer.Record(lineageInputIndex.Value, outcomeReason, retryCount);
                return;
            }

            // Fallback preserves behavior when the outcome store was not pre-resolved.
            LineageNodeOutcomeRegistry.Record(context.PipelineId, cached.NodeId, lineageInputIndex.Value, outcomeReason, retryCount);
        }

        private static async Task TryDispatchDeadLetterAsync<TIn>(
            TIn failedItem,
            Exception exception,
            PipelineContext context,
            string nodeId,
            int retryAttempt,
            CancellationToken cancellationToken)
        {
            if (context.DeadLetterSink is null)
            {
                return;
            }

            var attribution = FailureAttributionResolver.Resolve(exception, context, nodeId, retryAttempt);
            var envelope = new DeadLetterEnvelope(failedItem!, exception, attribution);
            await context.DeadLetterSink.HandleAsync(envelope, context, cancellationToken).ConfigureAwait(false);
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
        /// <param name="observer">The execution observer.</param>
        /// <param name="effectiveDop">The effective degree of parallelism.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A tuple containing the output channel and the list of worker tasks.</returns>
        protected static (Channel<TOut> OutChannel, List<Task> Workers) CreateWorkerTasks<TIn, TOut>(
            ChannelReader<IndexedWorkItem<TIn>> reader,
            ITransformNode<TIn, TOut> node,
            PipelineContext context,
            CachedNodeExecutionContext cachedContext,
            ParallelExecutionMetrics metrics,
            IExecutionObserver? observer,
            int effectiveDop,
            CancellationToken cancellationToken)
        {
            var outChannel = Channel.CreateUnbounded<TOut>();
            var workers = new List<Task>(effectiveDop);

            for (var i = 0; i < effectiveDop; i++)
            {
                workers.Add(Task.Run(async () =>
                {
                    await foreach (var next in reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                    {
                        var result = await ExecuteWithRetryAsync(next.Item, node, context, cachedContext, metrics, observer, next.LineageInputIndex,
                            next.CorrelationId, next.AncestryInputIndices);

                        if (result is not null)
                        {
                            _ = metrics.IncrementProcessed();
                            await outChannel.Writer.WriteAsync(result, cancellationToken);
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
                context.NodeExecutionScopeRegistry.SetRuntimeAnnotation(PipelineContextKeys.ParallelMetricsDroppedNewest(nodeId),
                    metrics.DroppedNewest);
                context.NodeExecutionScopeRegistry.SetRuntimeAnnotation(PipelineContextKeys.ParallelMetricsDroppedOldest(nodeId),
                    metrics.DroppedOldest);
                context.NodeExecutionScopeRegistry.SetRuntimeAnnotation(PipelineContextKeys.ParallelMetricsEnqueued(nodeId), metrics.Enqueued);
                context.NodeExecutionScopeRegistry.SetRuntimeAnnotation(PipelineContextKeys.ParallelMetricsProcessed(nodeId), metrics.Processed);
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
            return context.NodeExecutionScopeRegistry.BeginNodeScope(nodeId);
        }
    }
}

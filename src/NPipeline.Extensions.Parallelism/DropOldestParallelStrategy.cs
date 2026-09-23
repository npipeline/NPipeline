using System.Threading.Channels;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Execution.Lineage;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Parallelism;

/// <summary>
///     Parallel execution strategy that drops oldest items when the input queue is full.
///     This strategy prioritizes keeping newer items in the queue, making it suitable for
///     scenarios where latency/freshness is critical (e.g., real-time alerts with bounded memory).
/// </summary>
public sealed class DropOldestParallelStrategy : ParallelExecutionStrategyBase
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="DropOldestParallelStrategy" /> class.
    /// </summary>
    /// <param name="maxDegreeOfParallelism">The maximum degree of parallelism. If not specified, it defaults to the processor count.</param>
    public DropOldestParallelStrategy(int? maxDegreeOfParallelism = null) : base(maxDegreeOfParallelism)
    {
    }

    /// <inheritdoc />
    public override Task<IDataStream<TOut>> ExecuteAsync<TIn, TOut>(
        IDataStream<TIn> input,
        ITransformNode<TIn, TOut> node,
        PipelineContext context,
        string nodeId,
        CancellationToken cancellationToken)
    {
        return Execute(input, 0, null, node, context, nodeId, cancellationToken);
    }

    /// <inheritdoc />
    /// <remarks>
    ///     Output is in completion order, so the checkpoint is the oldest item not yet delivered, and outputs delivered
    ///     ahead of it are delivered again after a restart. A dropped item counts as delivered.
    /// </remarks>
    public override Task<IDataStream<TOut>> ExecuteFromAsync<TIn, TOut>(
        IDataStream<TIn> input,
        long offset,
        RestartCheckpoint checkpoint,
        ITransformNode<TIn, TOut> node,
        PipelineContext context,
        string nodeId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(checkpoint);
        return Execute(input, offset, checkpoint, node, context, nodeId, cancellationToken);
    }

    /// <summary>
    ///     Executes the node over an input whose first item has index <c>offset</c>, reporting delivered items to
    ///     <c>checkpoint</c> when the node is restartable (otherwise it is <see langword="null" />).
    /// </summary>
    private Task<IDataStream<TOut>> Execute<TIn, TOut>(
        IDataStream<TIn> input,
        long offset,
        RestartCheckpoint? checkpoint,
        ITransformNode<TIn, TOut> node,
        PipelineContext context,
        string nodeId,
        CancellationToken cancellationToken)
    {
        // Set the parallel execution flag to help ErrorHandlingService preserve original exception types
        context.ExecutionConfiguration.IsParallelExecution = true;

        var observabilityScope = BeginNodeObservabilityScope(context, nodeId);
        var currentActivity = context.Observability.Tracer.CurrentActivity;
        var cachedContext = CachedNodeExecutionContext.Create(context, nodeId);
        var logger = context.Observability.LoggerFactory.CreateLogger(nameof(DropOldestParallelStrategy));
        ParallelExecutionStrategyLogMessages.FinalMaxRetries(logger, nodeId, cachedContext.Resilience.ItemRetry.MaxRetries);

        ParallelOptions? parallelOptions = null;

        if (context.NodeEnvironment.NodeExecutionScopeRegistry.TryGetNodeExecutionAnnotation(nodeId, out var opt) && opt is ParallelOptions po)
            parallelOptions = po;

        // Input-wait timing is opt-in for parallel execution (channel backpressure dominates the measurement).
        var timedInput = parallelOptions?.EnableInputWaitTiming == true
            ? NodeTimingDataStreamWrapper.WrapInputWait(input, observabilityScope)
            : input;

        var effectiveDop = parallelOptions?.MaxDegreeOfParallelism ?? ConfiguredMaxDop ?? Environment.ProcessorCount;
        var boundedCapacity = parallelOptions?.MaxQueueLength ?? 1;
        var metricsInterval = parallelOptions?.EffectiveMetricsInterval ?? TimeSpan.FromSeconds(1);
        var observer = context.Observability.ExecutionObserver;

        // Custom bounded queue with drop-oldest policy
        var fullMode = BoundedChannelFullMode.Wait; // Wait then allow explicit read-and-drop

        var queue = Channel.CreateBounded<IndexedWorkItem<TIn>>(new BoundedChannelOptions(boundedCapacity)
        {
            FullMode = fullMode,
            SingleReader = false,
            SingleWriter = true,
        });

        var writer = queue.Writer;
        var reader = queue.Reader;

        // Check if metrics already exist before creating new ones
        ParallelExecutionMetrics metrics;

        if (!context.NodeEnvironment.NodeExecutionScopeRegistry.TryGetRuntimeAnnotation(PipelineContextKeys.ParallelMetrics(nodeId), out var existingMetrics) ||
            existingMetrics is not ParallelExecutionMetrics cachedMetrics)
        {
            metrics = new ParallelExecutionMetrics();
            context.NodeEnvironment.NodeExecutionScopeRegistry.SetRuntimeAnnotation(PipelineContextKeys.ParallelMetrics(nodeId), metrics);
        }
        else
            metrics = cachedMetrics;

        // Cooperative fault propagation: the first failure, of the feeder or of a worker, stops the others and becomes
        // the stream's failure. Without it a failed worker went unnoticed until the input drained, which for a
        // never-ending input (or a restart's replay window, held open by the failed item) is never.
        var faultCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        Exception? firstFault = null;

        void RecordFault(Exception ex)
        {
            _ = Interlocked.CompareExchange(ref firstFault, ex, null);

            try
            {
                faultCts.Cancel();
            }
            catch (ObjectDisposedException)
            {
                // The stream has already completed.
            }
        }

        _ = Task.Run(async () =>
        {
            var lastMetricsEmit = DateTimeOffset.UtcNow;
            var sequence = offset;

            try
            {
                await foreach (var item in timedInput.WithCancellation(faultCts.Token).ConfigureAwait(false))
                {
                    var lineageInputIndex = LineageExecutionItemContext.TryGetCurrentInputIndex(out var currentInputIndex)
                        ? currentInputIndex
                        : (long?)null;

                    var hasMetadata = LineageExecutionItemContext.TryGetCurrentItemMetadata(out var currentMetadata);
                    var correlationId = hasMetadata
                        ? currentMetadata.CorrelationId
                        : (Guid?)null;
                    var ancestryInputIndices = hasMetadata
                        ? currentMetadata.AncestryInputIndices
                        : null;

                    var indexedItem = new IndexedWorkItem<TIn>(item, lineageInputIndex, correlationId, ancestryInputIndices, sequence++);

                    if (queue.Writer.TryWrite(indexedItem))
                    {
                        observabilityScope?.IncrementProcessed();
                        metrics.IncrementEnqueued();
                    }
                    else
                    {
                        // Drop oldest: read and discard one item, then try to write the new one

                        // Keep trying to drop oldest and write the new item until successful
                        var dropAttempts = 0;
                        var maxDropAttempts = 3; // Prevent infinite loop

                        while (dropAttempts < maxDropAttempts)
                        {
                            if (queue.Reader.TryRead(out var dropped))
                            {
                                // A dropped item will never produce output, so its outcome is delivered.
                                metrics.IncrementDroppedOldest();
                                checkpoint?.Complete(dropped.Sequence);
                                dropAttempts++;

                                observer?.OnDrop(new QueueDropEvent(nodeId, nameof(BoundedQueuePolicy.DropOldest),
                                    QueueDropKind.Oldest, boundedCapacity,
                                    queue.Reader.Count, (int)metrics.DroppedNewest, (int)metrics.DroppedOldest, (int)metrics.Enqueued));
                            }
                            else
                            {
                                // No items to drop, break out of loop
                                break;
                            }

                            if (queue.Writer.TryWrite(indexedItem))
                            {
                                observabilityScope?.IncrementProcessed();
                                metrics.IncrementEnqueued();

                                break; // Success, exit the loop
                            }
                        }

                        if (dropAttempts >= maxDropAttempts)
                        {
                            ParallelExecutionStrategyLogMessages.EnqueueFailed(logger, nodeId, item?.ToString(), maxDropAttempts);
                            checkpoint?.Complete(indexedItem.Sequence);
                        }
                    }

                    if (DateTimeOffset.UtcNow - lastMetricsEmit >= metricsInterval)
                    {
                        lastMetricsEmit = DateTimeOffset.UtcNow;

                        observer?.OnQueueMetrics(new QueueMetricsEvent(nodeId, nameof(BoundedQueuePolicy.DropOldest), boundedCapacity,
                            queue.Reader.Count, (int)metrics.DroppedNewest, (int)metrics.DroppedOldest, (int)metrics.Enqueued, lastMetricsEmit));
                    }
                }

                _ = writer.TryComplete();
            }
            catch (Exception ex)
            {
                // A failed input fails the stream; completing normally would end it as if the input had drained.
                RecordFault(ex);
                _ = writer.TryComplete(ex);
            }
        }, CancellationToken.None);

        // Worker tasks: drain queue until completion and empty.
        var (outChannel, workers) = CreateWorkerTasks(
            reader,
            node,
            context,
            cachedContext,
            metrics,
            effectiveDop,
            checkpoint,
            faultCts.Token,
            RecordFault);

        _ = Task.WhenAll(workers).ContinueWith(completed =>
        {
            _ = outChannel.Writer.TryComplete(Volatile.Read(ref firstFault));
            faultCts.Dispose();
        }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);

        return Task.FromResult<IDataStream<TOut>>(
            new DataStream<TOut>(CreateOutputEnumerable(outChannel, nodeId, context, metrics, currentActivity, observabilityScope, checkpoint,
                cancellationToken)));
    }
}

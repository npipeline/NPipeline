using System.Threading.Channels;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution;
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
    public override Task<IDataPipe<TOut>> ExecuteAsync<TIn, TOut>(
        IDataPipe<TIn> input,
        ITransformNode<TIn, TOut> node,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Set the parallel execution flag to help ErrorHandlingService preserve original exception types
        context.Items[PipelineContextKeys.ParallelExecution] = true;

        var nodeId = context.CurrentNodeId;
        var observabilityScope = TryGetNodeObservabilityScope(context, nodeId);
        var currentActivity = context.Tracer.CurrentActivity;
        var effectiveRetries = GetRetryOptions(nodeId, context);
        var cachedContext = CachedNodeExecutionContext.CreateWithRetryOptions(context, nodeId, effectiveRetries);
        var logger = context.LoggerFactory.CreateLogger(nameof(DropOldestParallelStrategy));
        ParallelExecutionStrategyLogMessages.FinalMaxRetries(logger, nodeId, effectiveRetries.MaxItemRetries);

        ParallelOptions? parallelOptions = null;

        if (context.Items.TryGetValue(PipelineContextKeys.NodeExecutionOptions(nodeId), out var opt) && opt is ParallelOptions po)
            parallelOptions = po;

        var effectiveDop = parallelOptions?.MaxDegreeOfParallelism ?? ConfiguredMaxDop ?? Environment.ProcessorCount;
        var boundedCapacity = parallelOptions?.MaxQueueLength ?? 1;
        var metricsInterval = parallelOptions?.EffectiveMetricsInterval ?? TimeSpan.FromSeconds(1);
        var observer = context.ExecutionObserver;

        // Custom bounded queue with drop-oldest policy
        var fullMode = BoundedChannelFullMode.Wait; // Wait then allow explicit read-and-drop

        var queue = Channel.CreateBounded<TIn>(new BoundedChannelOptions(boundedCapacity)
        {
            FullMode = fullMode,
            SingleReader = false,
            SingleWriter = true,
        });

        var writer = queue.Writer;
        var reader = queue.Reader;

        // Check if metrics already exist before creating new ones
        ParallelExecutionMetrics metrics;

        if (!context.Items.TryGetValue(PipelineContextKeys.ParallelMetrics(nodeId), out _))
        {
            metrics = new ParallelExecutionMetrics();
            context.Items[PipelineContextKeys.ParallelMetrics(nodeId)] = metrics;
        }
        else
            metrics = (ParallelExecutionMetrics)context.Items[PipelineContextKeys.ParallelMetrics(nodeId)];

        _ = Task.Run(async () =>
        {
            var lastMetricsEmit = DateTimeOffset.UtcNow;

            try
            {
                await foreach (var item in input.WithCancellation(cancellationToken))
                {
                    if (queue.Writer.TryWrite(item))
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
                            if (queue.Reader.TryRead(out _))
                            {
                                metrics.IncrementDroppedOldest();
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

                            if (queue.Writer.TryWrite(item))
                            {
                                observabilityScope?.IncrementProcessed();
                                metrics.IncrementEnqueued();

                                break; // Success, exit the loop
                            }
                        }

                        if (dropAttempts >= maxDropAttempts)
                            ParallelExecutionStrategyLogMessages.EnqueueFailed(logger, nodeId, item?.ToString(), maxDropAttempts);
                    }

                    if (DateTimeOffset.UtcNow - lastMetricsEmit >= metricsInterval)
                    {
                        lastMetricsEmit = DateTimeOffset.UtcNow;

                        observer?.OnQueueMetrics(new QueueMetricsEvent(nodeId, nameof(BoundedQueuePolicy.DropOldest), boundedCapacity,
                            queue.Reader.Count, (int)metrics.DroppedNewest, (int)metrics.DroppedOldest, (int)metrics.Enqueued, lastMetricsEmit));
                    }
                }
            }
            finally
            {
                writer.Complete();
            }
        }, cancellationToken);

        // Worker tasks: drain queue until completion and empty.
        var (outChannel, workers) = CreateWorkerTasks(
            reader,
            node,
            context,
            cachedContext,
            metrics,
            observer,
            effectiveDop,
            cancellationToken);

        _ = Task.WhenAll(workers).ContinueWith(t => { outChannel.Writer.TryComplete(t.Exception); }, cancellationToken);

        return Task.FromResult<IDataPipe<TOut>>(
            new StreamingDataPipe<TOut>(CreateOutputEnumerable(outChannel, nodeId, context, metrics, currentActivity, cancellationToken, observabilityScope)));
    }
}

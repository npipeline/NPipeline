using System.Threading.Channels;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Parallelism;

/// <summary>
///     Parallel execution strategy that drops newest items when the input queue is full.
///     This strategy prioritizes keeping older items in the queue, making it suitable for
///     scenarios where recency bias is acceptable (e.g., real-time analytics with bounded memory).
/// </summary>
public sealed class DropNewestParallelStrategy : ParallelExecutionStrategyBase
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="DropNewestParallelStrategy" /> class.
    /// </summary>
    /// <param name="maxDegreeOfParallelism">The maximum degree of parallelism. If not specified, it defaults to the processor count.</param>
    public DropNewestParallelStrategy(int? maxDegreeOfParallelism = null) : base(maxDegreeOfParallelism)
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
        var logger = context.LoggerFactory.CreateLogger(nameof(DropNewestParallelStrategy));
        logger.Log(LogLevel.Debug, "Node {NodeId}, Final MaxRetries: {MaxRetries}", nodeId, effectiveRetries.MaxItemRetries);

        ParallelOptions? parallelOptions = null;

        if (context.Items.TryGetValue(PipelineContextKeys.NodeExecutionOptions(nodeId), out var opt) && opt is ParallelOptions po)
            parallelOptions = po;

        var effectiveDop = parallelOptions?.MaxDegreeOfParallelism ?? ConfiguredMaxDop ?? Environment.ProcessorCount;
        var boundedCapacity = parallelOptions?.MaxQueueLength ?? 1000;
        var metricsInterval = parallelOptions?.EffectiveMetricsInterval ?? TimeSpan.FromSeconds(1);
        var observer = context.ExecutionObserver;

        // Custom bounded queue with drop-newest policy
        var fullMode = BoundedChannelFullMode.DropWrite;

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
            var itemsSeen = 0;

            try
            {
                await foreach (var item in input.WithCancellation(cancellationToken))
                {
                    itemsSeen++;

                    if (queue.Writer.TryWrite(item))
                    {
                        observabilityScope?.IncrementProcessed();
                        metrics.IncrementEnqueued();
                    }
                    else
                    {
                        // Drop the incoming item (newest)
                        metrics.IncrementDroppedNewest();

                        observer?.OnDrop(new QueueDropEvent(nodeId, BoundedQueuePolicy.DropNewest.ToString(),
                            QueueDropKind.Newest, boundedCapacity, queue.Reader.Count,
                            (int)metrics.DroppedNewest, (int)metrics.DroppedOldest, (int)metrics.Enqueued));
                    }

                    if (DateTimeOffset.UtcNow - lastMetricsEmit >= metricsInterval)
                    {
                        lastMetricsEmit = DateTimeOffset.UtcNow;

                        observer?.OnQueueMetrics(new QueueMetricsEvent(nodeId, BoundedQueuePolicy.DropNewest.ToString(), boundedCapacity,
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

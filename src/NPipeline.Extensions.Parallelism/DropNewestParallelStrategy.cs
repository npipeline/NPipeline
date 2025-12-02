using System.Runtime.CompilerServices;
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
        var currentActivity = context.Tracer.CurrentActivity;
        var effectiveRetries = GetRetryOptions(nodeId, context);
        var logger = context.LoggerFactory.CreateLogger(nameof(DropNewestParallelStrategy));
        logger.Log(LogLevel.Debug, "Node {NodeId}, Final MaxRetries: {MaxRetries}", nodeId, effectiveRetries.MaxItemRetries);

        ParallelOptions? parallelOptions = null;

        if (context.Items.TryGetValue(PipelineContextKeys.NodeExecutionOptions(nodeId), out var opt) && opt is ParallelOptions po)
            parallelOptions = po;

        var effectiveDop = parallelOptions?.MaxDegreeOfParallelism ?? ConfiguredMaxDop ?? Environment.ProcessorCount;
        var boundedCapacity = parallelOptions?.MaxQueueLength ?? 1;
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
                        metrics.IncrementEnqueued();
                    else
                    {
                        // Drop the incoming item (newest)
                        metrics.IncrementDroppedNewest();

                        observer?.OnDrop(new QueueDropEvent(nodeId, BoundedQueuePolicy.DropNewest.ToString(),
                            QueueDropKind.Newest, boundedCapacity, queue.Reader.Count,
                            (int)metrics.DroppedNewest, (int)metrics.DroppedOldest, (int)metrics.Enqueued));
                    }

                    if (DateTimeOffset.UtcNow - lastMetricsEmit >= TimeSpan.FromSeconds(1))
                    {
                        lastMetricsEmit = DateTimeOffset.UtcNow;

                        observer?.OnQueueMetrics(new QueueMetricsEvent(nodeId, BoundedQueuePolicy.DropNewest.ToString(), boundedCapacity,
                            queue.Reader.Count, (int)metrics.DroppedNewest, (int)metrics.DroppedOldest, (int)metrics.Enqueued, lastMetricsEmit));
                    }
                }

                // Calculate how many items were dropped by the channel itself (due to DropWrite mode)
                var itemsActuallyProcessed = (int)metrics.Enqueued;
                var expectedDropped = Math.Max(0, itemsSeen - itemsActuallyProcessed - boundedCapacity);

                for (var i = 0; i < expectedDropped; i++)
                {
                    metrics.IncrementDroppedNewest();
                }
            }
            finally
            {
                writer.Complete();
            }
        }, cancellationToken);

        // Worker tasks: drain queue until completion and empty.
        var outChannel = Channel.CreateUnbounded<TOut>();
        var dop = effectiveDop;
        var workers = new List<Task>(dop);

        for (var i = 0; i < dop; i++)
        {
            workers.Add(Task.Run(async () =>
            {
                await foreach (var next in reader.ReadAllAsync(cancellationToken))
                {
                    var result = await ExecuteWithRetryAsync(next, node, context, nodeId, effectiveRetries, cancellationToken, metrics, observer);

                    if (result is not null)
                    {
                        metrics.IncrementProcessed();
                        await outChannel.Writer.WriteAsync(result, cancellationToken);
                    }
                }
            }, cancellationToken));
        }

        _ = Task.WhenAll(workers).ContinueWith(t => { outChannel.Writer.TryComplete(t.Exception?.InnerException); }, cancellationToken);

        return Task.FromResult<IDataPipe<TOut>>(new StreamingDataPipe<TOut>(ReadOut(cancellationToken)));

        async IAsyncEnumerable<TOut> ReadOut([EnumeratorCancellation] CancellationToken ct)
        {
            try
            {
                await foreach (var item in outChannel.Reader.ReadAllAsync(ct))
                {
                    yield return item;
                }
            }
            finally
            {
                // Tag drop and queue metrics on the current activity for observability
                if (metrics is not null)
                {
                    currentActivity?.SetTag("parallel.dropped.newest", metrics.DroppedNewest);
                    currentActivity?.SetTag("parallel.dropped.oldest", metrics.DroppedOldest);
                    currentActivity?.SetTag("parallel.enqueued", metrics.Enqueued);
                    currentActivity?.SetTag("parallel.processed", metrics.Processed);

                    // Store metrics in context.Items for downstream monitoring
                    context.Items[PipelineContextKeys.ParallelMetricsDroppedNewest(nodeId)] = metrics.DroppedNewest;
                    context.Items[PipelineContextKeys.ParallelMetricsDroppedOldest(nodeId)] = metrics.DroppedOldest;
                    context.Items[PipelineContextKeys.ParallelMetricsEnqueued(nodeId)] = metrics.Enqueued;
                    context.Items[PipelineContextKeys.ParallelMetricsProcessed(nodeId)] = metrics.Processed;
                }
            }
        }
    }
}

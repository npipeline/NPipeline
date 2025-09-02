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
        var logger = context.LoggerFactory.CreateLogger(nameof(DropOldestParallelStrategy));
        logger.Log(LogLevel.Debug, "Node {NodeId}, Final MaxRetries: {MaxRetries}", nodeId, effectiveRetries.MaxItemRetries);

        ParallelOptions? parallelOptions = null;

        if (context.Items.TryGetValue(PipelineContextKeys.NodeExecutionOptions(nodeId), out var opt) && opt is ParallelOptions po)
            parallelOptions = po;

        var effectiveDop = parallelOptions?.MaxDegreeOfParallelism ?? ConfiguredMaxDop ?? Environment.ProcessorCount;
        var boundedCapacity = parallelOptions?.MaxQueueLength ?? 1;
        var observer = context.ExecutionObserver;

        // Custom bounded queue with drop-oldest policy
        var fullMode = BoundedChannelFullMode.Wait; // Wait then allow explicit read-and-drop

        // DIAGNOSTIC LOG: Log queue configuration
        logger.Log(LogLevel.Debug, "Node {NodeId}, Creating DropOldest queue with capacity {Capacity} and FullMode {FullMode}", nodeId, boundedCapacity,
            fullMode);

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
                        metrics.IncrementEnqueued();

                        logger.Log(LogLevel.Debug, "Node {NodeId}, Successfully enqueued item {Item}, queue count: {Count}", nodeId, item?.ToString() ?? "null",
                            queue.Reader.Count);
                    }
                    else
                    {
                        // Drop oldest: read and discard one item, then try to write the new one
                        logger.Log(LogLevel.Debug, "Node {NodeId}, Queue full, attempting to drop oldest item", nodeId);

                        if (queue.Reader.TryRead(out _))
                        {
                            metrics.IncrementDroppedOldest();
                            logger.Log(LogLevel.Debug, "Node {NodeId}, Dropped oldest item, queue count after drop: {Count}", nodeId, queue.Reader.Count);

                            observer?.OnDrop(new QueueDropEvent(nodeId, BoundedQueuePolicy.DropOldest.ToString(),
                                QueueDropKind.Oldest, boundedCapacity,
                                queue.Reader.Count, (int)metrics.DroppedNewest, (int)metrics.DroppedOldest, (int)metrics.Enqueued));
                        }

                        if (queue.Writer.TryWrite(item))
                        {
                            metrics.IncrementEnqueued();

                            logger.Log(LogLevel.Debug, "Node {NodeId}, Successfully enqueued new item after dropping oldest: {Item}, queue count: {Count}",
                                nodeId, item?.ToString() ?? "null", queue.Reader.Count);
                        }
                        else
                            logger.Log(LogLevel.Warning, "Node {NodeId}, Failed to enqueue item even after dropping oldest: {Item}", nodeId,
                                item?.ToString() ?? "null");
                    }

                    if (DateTimeOffset.UtcNow - lastMetricsEmit >= TimeSpan.FromSeconds(1))
                    {
                        lastMetricsEmit = DateTimeOffset.UtcNow;

                        observer?.OnQueueMetrics(new QueueMetricsEvent(nodeId, BoundedQueuePolicy.DropOldest.ToString(), boundedCapacity,
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

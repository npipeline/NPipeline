using System.Runtime.CompilerServices;
using System.Threading.Channels;
using System.Threading.Tasks.Dataflow;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Parallelism;

/// <summary>
///     Parallel execution strategy using TPL Dataflow with blocking/backpressure semantics.
///     This strategy preserves ordering and applies backpressure, making it suitable for
///     scenarios requiring end-to-end flow control.
/// </summary>
public class BlockingParallelStrategy : ParallelExecutionStrategyBase
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="BlockingParallelStrategy" /> class.
    /// </summary>
    /// <param name="maxDegreeOfParallelism">The maximum degree of parallelism. If not specified, it defaults to the processor count.</param>
    public BlockingParallelStrategy(int? maxDegreeOfParallelism = null) : base(maxDegreeOfParallelism)
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

        // Capture a stable node id (PipelineRunner sets this prior to invoking the strategy). In parallel execution
        // relying on context.CurrentNodeId inside the delegate would be racy if other nodes change it.
        var nodeId = context.CurrentNodeId;
        var observabilityScope = TryGetNodeObservabilityScope(context, nodeId);

        // Capture the current activity for tagging observability metrics
        var currentActivity = context.Tracer.CurrentActivity;

        // Resolve effective retry options using our helper method
        var effectiveRetries = GetRetryOptions(nodeId, context);
        var logger = context.LoggerFactory.CreateLogger(nameof(BlockingParallelStrategy));
        logger.Log(LogLevel.Debug, "Node {NodeId}, Final MaxRetries: {MaxRetries}", nodeId, effectiveRetries.MaxItemRetries);

        // Resolve per-node parallel options if provided
        ParallelOptions? parallelOptions = null;

        if (context.Items.TryGetValue(PipelineContextKeys.NodeExecutionOptions(nodeId), out var opt) && opt is ParallelOptions po)
            parallelOptions = po;

        var effectiveDop = parallelOptions?.MaxDegreeOfParallelism ?? ConfiguredMaxDop ?? Environment.ProcessorCount;
        var boundedCapacity = parallelOptions?.MaxQueueLength;
        var observer = context.ExecutionObserver;

        // Metrics only created for drop policies previously; extend to Block path for retry visibility.
        ParallelExecutionMetrics? blockMetrics = null;

        if (context.Items.TryGetValue($"parallel.metrics::{nodeId}", out var existingMetrics) && existingMetrics is ParallelExecutionMetrics cached)
            blockMetrics = cached;
        else
        {
            blockMetrics = new ParallelExecutionMetrics();
            context.Items[PipelineContextKeys.ParallelMetrics(nodeId)] = blockMetrics;
        }

        // Create cached execution context once for all items (performance optimization)
        var cachedContext = CachedNodeExecutionContext.CreateWithRetryOptions(context, nodeId, effectiveRetries);

        var transformBlock = new TransformBlock<TIn, (bool hasValue, TOut value)>(async item =>
            {
                var result = await ExecuteWithRetryAsync(item, node, context, cachedContext, blockMetrics, observer);

                return result is null
                    ? (false, default!)
                    : (true, result);
            },
            new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = effectiveDop,
                CancellationToken = cancellationToken,
                BoundedCapacity = boundedCapacity ?? DataflowBlockOptions.Unbounded,
                EnsureOrdered = parallelOptions?.PreserveOrdering ?? true,
            });

        // Track high-water marks
        var inputHighWater = 0;
        var outputHighWater = 0;
        var outputCap = parallelOptions?.OutputBufferCapacity; // capture once (may be null)

        // Output channel (bounded when OutputBufferCapacity specified)
        Channel<TOut> channel;

        if (outputCap is null)
            channel = Channel.CreateUnbounded<TOut>(new UnboundedChannelOptions { SingleReader = false, SingleWriter = true });
        else
        {
            channel = Channel.CreateBounded<TOut>(new BoundedChannelOptions(outputCap.Value)
                { SingleReader = false, SingleWriter = true, FullMode = BoundedChannelFullMode.Wait });
        }

        // Producer: feed block
        _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var item in input.WithCancellation(cancellationToken))
                {
                    observabilityScope?.IncrementProcessed();

                    // SendAsync applies backpressure based on transformBlock input capacity
                    await transformBlock.SendAsync(item, cancellationToken);
                    var count = transformBlock.InputCount;

                    if (count > inputHighWater)
                        inputHighWater = count;
                }
            }
            finally
            {
                transformBlock.Complete();
            }
        }, cancellationToken);

        // Drainer: move results to output channel honoring OutputBufferCapacity
        _ = Task.Run(async () =>
        {
            try
            {
                while (await transformBlock.OutputAvailableAsync(cancellationToken))
                {
                    while (transformBlock.TryReceive(out var tuple))
                    {
                        if (tuple.hasValue)
                        {
                            observabilityScope?.IncrementEmitted();
                            await channel.Writer.WriteAsync(tuple.value, cancellationToken);
                        }
                    }

                    // We cannot directly read Channel count; approximate output backlog using block's OutputCount + InputCount
                    var approxOut = transformBlock.OutputCount;

                    if (approxOut > outputHighWater)
                        outputHighWater = approxOut;
                }

                // Await completion explicitly to surface any fault (OutputAvailableAsync may return false without throwing when faulted & drained)
                await transformBlock.Completion.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                channel.Writer.TryComplete(ex);
                return;
            }

            channel.Writer.TryComplete();
        }, cancellationToken);

        return Task.FromResult<IDataPipe<TOut>>(new StreamingDataPipe<TOut>(ReadOut(cancellationToken)));

        async IAsyncEnumerable<TOut> ReadOut([EnumeratorCancellation] CancellationToken ct)
        {
            try
            {
                await foreach (var item in channel.Reader.ReadAllAsync(ct))
                {
                    yield return item;
                }
            }
            finally
            {
                // Tag high-water metrics on the current activity for observability
                currentActivity?.SetTag("parallel.input.highwater", inputHighWater);
                currentActivity?.SetTag("parallel.output.highwater", outputHighWater);

                if (outputCap is not null)
                {
                    currentActivity?.SetTag("parallel.output.capacity", outputCap.Value);
                    context.Items[PipelineContextKeys.ParallelMetricsOutputCapacity(nodeId)] = outputCap.Value;
                }

                // Store metrics in context.Items for downstream monitoring
                context.Items[PipelineContextKeys.ParallelMetricsInputHighWater(nodeId)] = inputHighWater;
                context.Items[PipelineContextKeys.ParallelMetricsOutputHighWater(nodeId)] = outputHighWater;

                if (blockMetrics is not null)
                {
                    currentActivity?.SetTag("parallel.retry.events", blockMetrics.RetryEvents);
                    currentActivity?.SetTag("parallel.retry.items", blockMetrics.ItemsWithRetry);
                    currentActivity?.SetTag("parallel.retry.maxItemAttempts", blockMetrics.MaxItemRetryAttempts);

                    context.Items[PipelineContextKeys.ParallelMetricsRetryEvents(nodeId)] = blockMetrics.RetryEvents;
                    context.Items[PipelineContextKeys.ParallelMetricsRetryItems(nodeId)] = blockMetrics.ItemsWithRetry;
                    context.Items[PipelineContextKeys.ParallelMetricsMaxItemRetryAttempts(nodeId)] = blockMetrics.MaxItemRetryAttempts;
                }

                observabilityScope?.Dispose();
            }
        }
    }
}

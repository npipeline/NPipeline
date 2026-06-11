using System.Runtime.CompilerServices;
using System.Threading.Channels;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Execution.Lineage;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Parallelism;

/// <summary>
///     Parallel execution strategy using lightweight channels with blocking/backpressure semantics.
///     Items are fanned out to a fixed set of worker tasks and fanned back in through a single output channel.
///     When <see cref="ParallelOptions.PreserveOrdering" /> is true (default), output is restored to input order
///     by a reorder buffer; when false, results are emitted in completion order for maximum throughput.
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

    /// <summary>
    ///     Result envelope written by workers. Placeholder entries (<see cref="HasValue" /> = false) keep the
    ///     sequence contiguous so the reorder buffer and the in-flight window can advance past skipped items.
    /// </summary>
    private readonly record struct SequencedResult<T>(long Sequence, bool HasValue, T Value);

    /// <inheritdoc />
    public override Task<IDataStream<TOut>> ExecuteAsync<TIn, TOut>(
        IDataStream<TIn> input,
        ITransformNode<TIn, TOut> node,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Set the parallel execution flag to help ErrorHandlingService preserve original exception types
        context.IsParallelExecution = true;

        // Capture a stable node id (PipelineRunner sets this prior to invoking the strategy). In parallel execution
        // relying on context.CurrentNodeId inside worker tasks would be racy if other nodes change it.
        var nodeId = context.CurrentNodeId;
        var observabilityScope = BeginNodeObservabilityScope(context, nodeId);

        // Resolve per-node parallel options if provided
        ParallelOptions? parallelOptions = null;

        if (context.NodeExecutionScopeRegistry.TryGetNodeExecutionAnnotation(nodeId, out var opt) && opt is ParallelOptions po)
            parallelOptions = po;

        // Input-wait timing is opt-in for parallel execution: the wait measured here is dominated by
        // channel backpressure rather than upstream latency, and the per-item timestamps cost throughput.
        var timedInput = parallelOptions?.EnableInputWaitTiming == true
            ? NodeTimingDataStreamWrapper.WrapInputWait(input, observabilityScope)
            : input;

        // Capture the current activity for tagging observability metrics
        var currentActivity = context.Tracer.CurrentActivity;

        // Resolve effective retry options using our helper method
        var effectiveRetries = GetRetryOptions(nodeId, context);
        var logger = context.LoggerFactory.CreateLogger(nameof(BlockingParallelStrategy));
        ParallelExecutionStrategyLogMessages.FinalMaxRetries(logger, nodeId, effectiveRetries.MaxItemRetries);

        var effectiveDop = parallelOptions?.MaxDegreeOfParallelism ?? ConfiguredMaxDop ?? Environment.ProcessorCount;
        var windowSize = parallelOptions?.MaxQueueLength;
        var outputCap = parallelOptions?.OutputBufferCapacity;
        var preserveOrdering = parallelOptions?.PreserveOrdering ?? true;
        var observer = context.ExecutionObserver;

        // Metrics for retry visibility.
        ParallelExecutionMetrics blockMetrics;

        if (context.NodeExecutionScopeRegistry.TryGetRuntimeAnnotation(PipelineContextKeys.ParallelMetrics(nodeId), out var existingMetrics) &&
            existingMetrics is ParallelExecutionMetrics cached)
            blockMetrics = cached;
        else
        {
            blockMetrics = new ParallelExecutionMetrics();
            context.NodeExecutionScopeRegistry.SetRuntimeAnnotation(PipelineContextKeys.ParallelMetrics(nodeId), blockMetrics);
        }

        // Create cached execution context once for all items (performance optimization)
        var cachedContext = CachedNodeExecutionContext.CreateWithRetryOptions(context, nodeId, effectiveRetries);

        // Input channel: single writer (feeder), many readers (workers).
        // MaxQueueLength is enforced by the in-flight window semaphore below rather than by channel capacity.
        // A window slot is taken when the feeder admits an item and released only when the consumer reads it
        // (or when a worker skips it), so one bound covers every stage end to end: queued in the input channel,
        // in-flight in a worker, buffered in the output channel, and parked in the reorder buffer.
        var inputChannel = Channel.CreateUnbounded<IndexedWorkItem<TIn>>(new UnboundedChannelOptions
        {
            SingleWriter = true,
            SingleReader = effectiveDop == 1,
        });

        // Bounds the number of items admitted but not yet handed to the consumer.
        var window = windowSize is null
            ? null
            : new SemaphoreSlim(windowSize.Value, windowSize.Value);

        // Output channel: many writers (workers), single reader (consumer).
        var outputChannel = outputCap is null
            ? Channel.CreateUnbounded<SequencedResult<TOut>>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = effectiveDop == 1 })
            : Channel.CreateBounded<SequencedResult<TOut>>(new BoundedChannelOptions(outputCap.Value)
            {
                SingleReader = true,
                SingleWriter = effectiveDop == 1,
                FullMode = BoundedChannelFullMode.Wait,
            });

        // Cooperative fault propagation: the first failure cancels the feeder and sibling workers.
        var faultCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        Exception? firstFault = null;

        // Track high-water marks. These are approximate diagnostics sampled (without synchronization) from the
        // producing/consuming threads and may understate the true peak backlog. Single-consumer unbounded
        // channels do not support Count (CanCount is false), so sampling is gated on CanCount.
        var canCountInput = inputChannel.Reader.CanCount;
        var canCountOutput = outputChannel.Reader.CanCount;
        var inputHighWater = 0;
        var outputHighWater = 0;

        // Feeder: enumerate upstream and admit items into the input channel.
        var feeder = Task.Run(async () =>
        {
            try
            {
                long sequence = 0;

                await foreach (var item in timedInput.WithCancellation(faultCts.Token).ConfigureAwait(false))
                {
                    if (window is not null)
                        await window.WaitAsync(faultCts.Token).ConfigureAwait(false);

                    observabilityScope.IncrementProcessed();

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

                    var work = new IndexedWorkItem<TIn>(item, lineageInputIndex, correlationId, ancestryInputIndices, sequence++);

                    // The input channel is always unbounded and the feeder is its only writer, so TryWrite
                    // never fails here; there is no bounded-capacity path that would require an async wait.
                    _ = inputChannel.Writer.TryWrite(work);

                    if (canCountInput)
                    {
                        var count = inputChannel.Reader.Count;

                        if (count > inputHighWater)
                            inputHighWater = count;
                    }
                }

                _ = inputChannel.Writer.TryComplete();
            }
            catch (Exception ex)
            {
                // Record the source fault as the first fault before cancelling, so a sibling worker that
                // observes the cancellation cannot overwrite it with an OperationCanceledException. Then stop
                // the workers cooperatively and complete the input channel.
                _ = Interlocked.CompareExchange(ref firstFault, ex, null);
                faultCts.Cancel();
                _ = inputChannel.Writer.TryComplete(ex);
            }
        }, CancellationToken.None);

        // Workers: drain the input channel, transform items, write results to the output channel.
        var workers = new Task[effectiveDop];

        for (var i = 0; i < effectiveDop; i++)
        {
            workers[i] = Task.Run(async () =>
            {
                try
                {
                    await foreach (var work in inputChannel.Reader.ReadAllAsync(faultCts.Token).ConfigureAwait(false))
                    {
                        var result = await ExecuteWithRetryAsync(work.Item, node, context, cachedContext, blockMetrics, observer,
                            work.LineageInputIndex, work.CorrelationId, work.AncestryInputIndices).ConfigureAwait(false);

                        if (result is not null)
                        {
                            var envelope = new SequencedResult<TOut>(work.Sequence, true, result);

                            if (!outputChannel.Writer.TryWrite(envelope))
                                await outputChannel.Writer.WriteAsync(envelope, faultCts.Token).ConfigureAwait(false);
                        }
                        else if (preserveOrdering)
                        {
                            // Skipped item: emit a placeholder so the reorder buffer can advance past this sequence.
                            var placeholder = new SequencedResult<TOut>(work.Sequence, false, default!);

                            if (!outputChannel.Writer.TryWrite(placeholder))
                                await outputChannel.Writer.WriteAsync(placeholder, faultCts.Token).ConfigureAwait(false);
                        }
                        else
                        {
                            // Unordered mode never sees this sequence again; release its window slot directly.
                            _ = window?.Release();
                        }
                    }
                }
                catch (Exception ex)
                {
                    _ = Interlocked.CompareExchange(ref firstFault, ex, null);
                    faultCts.Cancel();
                    throw;
                }
            }, CancellationToken.None);
        }

        // Completion: once the workers and the feeder have settled, complete the output channel. Faults from
        // either side are recorded in firstFault, so the consumer always observes the first failure and the
        // output channel is never completed successfully when a fault occurred.
        _ = Task.Run(async () =>
        {
            try
            {
                await Task.WhenAll(workers).ConfigureAwait(false);
            }
            catch
            {
                // Worker faults are captured in firstFault by the worker catch block.
            }

            // The feeder always completes normally (its own catch records the fault); awaiting it guarantees
            // firstFault reflects a source fault before the output channel is finalized.
            await feeder.ConfigureAwait(false);

            var fault = Volatile.Read(ref firstFault);

            _ = fault is not null
                ? outputChannel.Writer.TryComplete(fault)
                : outputChannel.Writer.TryComplete();

            faultCts.Dispose();
        }, CancellationToken.None);

        var output = preserveOrdering
            ? ReadOrdered(cancellationToken)
            : ReadUnordered(cancellationToken);

        return Task.FromResult<IDataStream<TOut>>(new DataStream<TOut>(output));

        async IAsyncEnumerable<TOut> ReadOrdered([EnumeratorCancellation] CancellationToken ct)
        {
            using var scopeHandle = observabilityScope;

            // Out-of-order completions parked until their sequence becomes current. Bounded by the in-flight
            // window when MaxQueueLength is set; unbounded otherwise (same as unbounded queue semantics).
            var pending = new Dictionary<long, SequencedResult<TOut>>();
            long nextSequence = 0;

            try
            {
                await foreach (var result in outputChannel.Reader.ReadAllAsync(ct).ConfigureAwait(false))
                {
                    SampleOutputHighWater();

                    if (result.Sequence != nextSequence)
                    {
                        pending[result.Sequence] = result;
                        continue;
                    }

                    var current = result;

                    while (true)
                    {
                        nextSequence++;
                        _ = window?.Release();

                        if (current.HasValue)
                        {
                            observabilityScope.IncrementEmitted();
                            yield return current.Value;
                        }

                        if (!pending.Remove(nextSequence, out current))
                            break;
                    }
                }
            }
            finally
            {
                PublishCompletionMetrics();
            }
        }

        async IAsyncEnumerable<TOut> ReadUnordered([EnumeratorCancellation] CancellationToken ct)
        {
            using var scopeHandle = observabilityScope;

            try
            {
                await foreach (var result in outputChannel.Reader.ReadAllAsync(ct).ConfigureAwait(false))
                {
                    SampleOutputHighWater();

                    _ = window?.Release();
                    observabilityScope.IncrementEmitted();
                    yield return result.Value;
                }
            }
            finally
            {
                PublishCompletionMetrics();
            }
        }

        void SampleOutputHighWater()
        {
            // Approximate, read-time sample of the output backlog (see the high-water comment above).
            if (!canCountOutput)
                return;

            var backlog = outputChannel.Reader.Count;

            if (backlog > outputHighWater)
                outputHighWater = backlog;
        }

        void PublishCompletionMetrics()
        {
            // Tag high-water metrics on the current activity for observability
            currentActivity?.SetTag("parallel.input.highwater", inputHighWater);
            currentActivity?.SetTag("parallel.output.highwater", outputHighWater);

            if (outputCap is not null)
            {
                currentActivity?.SetTag("parallel.output.capacity", outputCap.Value);
                context.NodeExecutionScopeRegistry.SetRuntimeAnnotation(PipelineContextKeys.ParallelMetricsOutputCapacity(nodeId),
                    outputCap.Value);
            }

            // Store metrics in runtime annotations for downstream monitoring
            context.NodeExecutionScopeRegistry.SetRuntimeAnnotation(PipelineContextKeys.ParallelMetricsInputHighWater(nodeId), inputHighWater);
            context.NodeExecutionScopeRegistry.SetRuntimeAnnotation(PipelineContextKeys.ParallelMetricsOutputHighWater(nodeId), outputHighWater);

            currentActivity?.SetTag("parallel.retry.events", blockMetrics.RetryEvents);
            currentActivity?.SetTag("parallel.retry.items", blockMetrics.ItemsWithRetry);
            currentActivity?.SetTag("parallel.retry.maxItemAttempts", blockMetrics.MaxItemRetryAttempts);

            context.NodeExecutionScopeRegistry.SetRuntimeAnnotation(PipelineContextKeys.ParallelMetricsRetryEvents(nodeId),
                blockMetrics.RetryEvents);
            context.NodeExecutionScopeRegistry.SetRuntimeAnnotation(PipelineContextKeys.ParallelMetricsRetryItems(nodeId),
                blockMetrics.ItemsWithRetry);
            context.NodeExecutionScopeRegistry.SetRuntimeAnnotation(PipelineContextKeys.ParallelMetricsMaxItemRetryAttempts(nodeId),
                blockMetrics.MaxItemRetryAttempts);
        }
    }
}

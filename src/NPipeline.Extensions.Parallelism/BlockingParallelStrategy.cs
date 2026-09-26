using System.Runtime.CompilerServices;
using System.Threading.Channels;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
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

    /// <inheritdoc />
    public override Task<IDataStream<TOut>> ExecuteAsync<TIn, TOut>(
        IDataStream<TIn> input,
        ITransformNode<TIn, TOut> node,
        PipelineContext context,
        string nodeId,
        CancellationToken cancellationToken) =>
        Execute(input, 0, null, node, context, nodeId, cancellationToken);

    /// <inheritdoc />
    /// <remarks>
    ///     With <see cref="ParallelOptions.PreserveOrdering" /> (the default) the checkpoint advances in input order, so
    ///     each output is delivered exactly once across restarts. Without it, the checkpoint is the oldest item still
    ///     in flight, and outputs delivered ahead of it are delivered again after a restart.
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
        // Capture a stable node id (PipelineRunner sets this prior to invoking the strategy). In parallel execution
        var observabilityScope = BeginNodeObservabilityScope(context, nodeId);

        // Resolve per-node parallel options if provided
        ParallelOptions? parallelOptions = null;

        if (context.NodeEnvironment.NodeExecutionScopeRegistry.TryGetNodeExecutionAnnotation(nodeId, out var opt) && opt is ParallelOptions po)
            parallelOptions = po;

        // Input-wait timing is opt-in for parallel execution: the wait measured here is dominated by
        // channel backpressure rather than upstream latency, and the per-item timestamps cost throughput.
        var timedInput = parallelOptions?.EnableInputWaitTiming == true
            ? NodeTimingDataStreamWrapper.WrapInputWait(input, observabilityScope)
            : input;

        // Capture the current activity for tagging observability metrics
        var currentActivity = context.Observability.Tracer.CurrentActivity;

        var cachedContext = CachedNodeExecutionContext.Create(context, nodeId);
        var lineage = cachedContext.LineageOutcomeWriter;
        var trackLineage = lineage.IsActive;
        var logger = context.Observability.LoggerFactory.CreateLogger(nameof(BlockingParallelStrategy));
        ParallelExecutionStrategyLogMessages.FinalMaxRetries(logger, nodeId, cachedContext.Resilience.ItemRetry.MaxRetries);

        var effectiveDop = parallelOptions?.MaxDegreeOfParallelism ?? ConfiguredMaxDop ?? Environment.ProcessorCount;
        var windowSize = parallelOptions?.MaxQueueLength;
        var outputCap = parallelOptions?.OutputBufferCapacity;
        var preserveOrdering = parallelOptions?.PreserveOrdering ?? true;

        // Metrics for retry visibility.
        ParallelExecutionMetrics blockMetrics;

        if (context.NodeEnvironment.NodeExecutionScopeRegistry.TryGetRuntimeAnnotation(PipelineContextKeys.ParallelMetrics(nodeId), out var existingMetrics) &&
            existingMetrics is ParallelExecutionMetrics cached)
            blockMetrics = cached;
        else
        {
            blockMetrics = new ParallelExecutionMetrics();
            context.NodeEnvironment.NodeExecutionScopeRegistry.SetRuntimeAnnotation(PipelineContextKeys.ParallelMetrics(nodeId), blockMetrics);
        }

        var onRetry = blockMetrics.RecordRetry;

        // Input channels: one dedicated channel per worker (single writer = feeder, single reader = the owning
        // worker). The feeder round-robins items across partitions. Giving each worker its own channel avoids the
        // dequeue-lock and cache-line contention that a single shared multi-reader channel suffers when several
        // workers race to read it; this is the primary fix for the parallel scaling regression. With effectiveDop
        // of 1 this degenerates to a single single-reader channel, matching the previous fast path.
        // MaxQueueLength is enforced by the in-flight window semaphore below rather than by channel capacity.
        // A window slot is taken when the feeder admits an item and released only when the consumer reads it
        // (or when a worker skips it), so one bound covers every stage end to end: queued in an input channel,
        // in-flight in a worker, buffered in the output channel, and parked in the reorder buffer.
        var inputChannels = new Channel<IndexedWorkItem<TIn>>[effectiveDop];

        for (var i = 0; i < effectiveDop; i++)
        {
            inputChannels[i] = Channel.CreateUnbounded<IndexedWorkItem<TIn>>(new UnboundedChannelOptions
            {
                SingleWriter = true,
                SingleReader = true,
            });
        }

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
        // producing/consuming threads and may understate the true peak backlog. Unbounded single-reader channels
        // do not support Count (CanCount is false), so both samples are gated on CanCount. All partitions share
        // the same options, so the first partition's CanCount is representative.
        var canCountInput = inputChannels[0].Reader.CanCount;
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

                    // Counted by input index, so an item a node restart replays is reported as replayed, not processed.
                    observabilityScope.IncrementProcessed(offset + sequence);

                    // Lineage is keyed by the item's index in the node's input, which a restart preserves.
                    var lineageInputIndex = trackLineage
                        ? offset + sequence
                        : (long?)null;

                    var work = new IndexedWorkItem<TIn>(item, lineageInputIndex, sequence);

                    // Round-robin the item to a worker partition. The input channels are always unbounded and the
                    // feeder is the only writer, so TryWrite never fails here; there is no bounded-capacity path
                    // that would require an async wait.
                    var partition = (int)(sequence % effectiveDop);
                    sequence++;
                    var partitionChannel = inputChannels[partition];
                    _ = partitionChannel.Writer.TryWrite(work);

                    // Cheap, feeder-thread-only sample of the partition backlog. Gated on CanCount because
                    // unbounded single-reader channels do not support Count.
                    if (canCountInput)
                    {
                        var count = partitionChannel.Reader.Count;

                        if (count > inputHighWater)
                            inputHighWater = count;
                    }
                }

                for (var i = 0; i < effectiveDop; i++)
                {
                    _ = inputChannels[i].Writer.TryComplete();
                }
            }
            catch (Exception ex)
            {
                // Record the source fault as the first fault before cancelling, so a sibling worker that
                // observes the cancellation cannot overwrite it with an OperationCanceledException. Then stop
                // the workers cooperatively and complete every input partition.
                _ = Interlocked.CompareExchange(ref firstFault, ex, null);
                faultCts.Cancel();

                for (var i = 0; i < effectiveDop; i++)
                {
                    _ = inputChannels[i].Writer.TryComplete(ex);
                }
            }
        }, CancellationToken.None);

        // Workers: each drains its own input partition, transforms items, writes results to the output channel.
        var workers = new Task[effectiveDop];

        for (var i = 0; i < effectiveDop; i++)
        {
            // Capture the partition reader for this worker; closing over the loop variable directly would be racy.
            var reader = inputChannels[i].Reader;

            workers[i] = Task.Run(async () =>
            {
                try
                {
                    await foreach (var work in reader.ReadAllAsync(faultCts.Token).ConfigureAwait(false))
                    {
                        var result = await ExecuteItemAsync(work, node, context, cachedContext, onRetry).ConfigureAwait(false);

                        if (result.Produced)
                        {
                            var envelope = new SequencedResult<TOut>(work.Sequence, true, result.Output!);

                            if (!outputChannel.Writer.TryWrite(envelope))
                                await outputChannel.Writer.WriteAsync(envelope, faultCts.Token).ConfigureAwait(false);
                        }
                        else if (preserveOrdering)
                        {
                            // Skipped or dead-lettered item: emit a placeholder so the reorder buffer can advance past this sequence.
                            var placeholder = new SequencedResult<TOut>(work.Sequence, false, default!);

                            if (!outputChannel.Writer.TryWrite(placeholder))
                                await outputChannel.Writer.WriteAsync(placeholder, faultCts.Token).ConfigureAwait(false);
                        }
                        else
                        {
                            // Unordered mode never sees this sequence again; release its window slot directly. The item
                            // has no output, so its outcome is delivered now.
                            _ = window?.Release();
                            checkpoint?.Complete(offset + work.Sequence);
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
                            lineage.ReportOutput(offset + current.Sequence);
                            yield return current.Value;
                        }

                        // Reached once the consumer asks for the next output (or at once for a placeholder), so every
                        // item below nextSequence has been delivered.
                        checkpoint?.Advance(offset + nextSequence);

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
                    lineage.ReportOutput(offset + result.Sequence);
                    yield return result.Value;

                    // Reached once the consumer asks for the next output, so this one has been delivered.
                    checkpoint?.Complete(offset + result.Sequence);
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

                context.NodeEnvironment.NodeExecutionScopeRegistry.SetRuntimeAnnotation(PipelineContextKeys.ParallelMetricsOutputCapacity(nodeId),
                    outputCap.Value);
            }

            // Store metrics in runtime annotations for downstream monitoring
            context.NodeEnvironment.NodeExecutionScopeRegistry.SetRuntimeAnnotation(PipelineContextKeys.ParallelMetricsInputHighWater(nodeId), inputHighWater);

            context.NodeEnvironment.NodeExecutionScopeRegistry.SetRuntimeAnnotation(PipelineContextKeys.ParallelMetricsOutputHighWater(nodeId),
                outputHighWater);

            currentActivity?.SetTag("parallel.retry.events", blockMetrics.RetryEvents);
            currentActivity?.SetTag("parallel.retry.items", blockMetrics.ItemsWithRetry);
            currentActivity?.SetTag("parallel.retry.maxItemAttempts", blockMetrics.MaxItemRetryAttempts);

            context.NodeEnvironment.NodeExecutionScopeRegistry.SetRuntimeAnnotation(PipelineContextKeys.ParallelMetricsRetryEvents(nodeId),
                blockMetrics.RetryEvents);

            context.NodeEnvironment.NodeExecutionScopeRegistry.SetRuntimeAnnotation(PipelineContextKeys.ParallelMetricsRetryItems(nodeId),
                blockMetrics.ItemsWithRetry);

            context.NodeEnvironment.NodeExecutionScopeRegistry.SetRuntimeAnnotation(PipelineContextKeys.ParallelMetricsMaxItemRetryAttempts(nodeId),
                blockMetrics.MaxItemRetryAttempts);
        }
    }

    /// <summary>
    ///     Result envelope written by workers. Placeholder entries (<see cref="HasValue" /> = false) keep the
    ///     sequence contiguous so the reorder buffer and the in-flight window can advance past skipped items.
    /// </summary>
    private readonly record struct SequencedResult<T>(long Sequence, bool HasValue, T Value);
}

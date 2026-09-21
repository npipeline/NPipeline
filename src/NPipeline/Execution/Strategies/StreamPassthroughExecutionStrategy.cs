using System.Diagnostics;
using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Strategies;

/// <summary>
///     Hands the whole input stream to the node and passes its output stream on unchanged.
/// </summary>
/// <remarks>
///     <para>
///         This is the default strategy for a stream transform node, and the only one that preserves what such a node
///         is for: the node sees the stream itself, so it can drop items, emit several for one, buffer, or batch.
///         Per-item strategies cannot express any of that, which is why one is rejected for a stream node.
///     </para>
///     <para>
///         Observability is tracked at stream level — items in, items out, and the time the node spent working as
///         distinct from the time it spent waiting for its input. Per-item retry is deliberately not applied: the
///         strategy cannot re-run one item through a node that consumes a stream.
///     </para>
/// </remarks>
public sealed class StreamPassthroughExecutionStrategy : IExecutionStrategy, IStreamExecutionStrategy
{
    private StreamPassthroughExecutionStrategy()
    {
    }

    /// <summary>
    ///     The shared instance. The strategy holds no state, so one serves every node and every run.
    /// </summary>
    public static StreamPassthroughExecutionStrategy Instance { get; } = new();

    /// <summary>
    ///     Runs a per-item transform node one item at a time, without retry orchestration.
    /// </summary>
    /// <remarks>
    ///     Present so a node that implements both shapes can be run either way; a stream node reaches the overload
    ///     below.
    /// </remarks>
    public Task<IDataStream<TOut>> ExecuteAsync<TIn, TOut>(
        IDataStream<TIn> input,
        ITransformNode<TIn, TOut> node,
        PipelineContext context,
        string nodeId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(context);

        return Task.FromResult<IDataStream<TOut>>(new DataStream<TOut>(Iterate(cancellationToken), input.StreamName));

        async IAsyncEnumerable<TOut> Iterate([EnumeratorCancellation] CancellationToken ct)
        {
            using var observabilityScope = context.NodeEnvironment.NodeExecutionScopeRegistry.BeginNodeScope(nodeId);
            using var activity = context.Observability.Tracer.StartActivity("Node.StreamTransform");
            var timedInput = NodeTimingDataStreamWrapper.WrapInputWait(input, observabilityScope);

            await using var inputEnumerator = timedInput.WithCancellation(ct).GetAsyncEnumerator();

            while (true)
            {
                TIn item;

                try
                {
                    if (!await inputEnumerator.MoveNextAsync())
                        break;

                    item = inputEnumerator.Current;
                }
                catch (Exception ex)
                {
                    observabilityScope.RecordFailure(ex);
                    throw;
                }

                observabilityScope.IncrementProcessed();

                TOut output;

                try
                {
                    var workStart = Stopwatch.GetTimestamp();
                    output = await node.TransformAsync(item, context, ct).ConfigureAwait(false);
                    observabilityScope.AddWork(Stopwatch.GetElapsedTime(workStart));
                }
                catch (Exception ex)
                {
                    observabilityScope.RecordFailure(ex);
                    throw;
                }

                observabilityScope.IncrementEmitted();
                yield return output;
            }
        }
    }

    /// <inheritdoc />
    public Task<IDataStream<TOut>> ExecuteAsync<TIn, TOut>(
        IDataStream<TIn> input,
        IStreamTransformNode<TIn, TOut> node,
        PipelineContext context,
        string nodeId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(context);

        return Task.FromResult<IDataStream<TOut>>(new DataStream<TOut>(Iterate(cancellationToken), input.StreamName));

        async IAsyncEnumerable<TOut> Iterate([EnumeratorCancellation] CancellationToken ct)
        {
            using var observabilityScope = context.NodeEnvironment.NodeExecutionScopeRegistry.BeginNodeScope(nodeId);
            using var activity = context.Observability.Tracer.StartActivity("Node.StreamTransform");
            var timedInput = NodeTimingDataStreamWrapper.WrapInputWait(input, observabilityScope);

            var outputs = node.TransformAsync(TrackInput(ct), context, ct).WithCancellation(ct);
            await using var outputEnumerator = outputs.GetAsyncEnumerator();

            while (true)
            {
                TOut output;

                try
                {
                    if (!await outputEnumerator.MoveNextAsync())
                        break;

                    output = outputEnumerator.Current;
                }
                catch (Exception ex)
                {
                    observabilityScope.RecordFailure(ex);
                    throw;
                }

                observabilityScope.IncrementEmitted();
                yield return output;
            }

            // A stream node owns the work across the whole delegate, less the time spent waiting on its input.
            var breakdown = observabilityScope.GetTimingBreakdown();
            var work = breakdown.WallDuration - breakdown.InputWaitDuration;

            if (work > TimeSpan.Zero)
                observabilityScope.AddWork(work);

            async IAsyncEnumerable<TIn> TrackInput([EnumeratorCancellation] CancellationToken innerCt)
            {
                await foreach (var item in timedInput.WithCancellation(innerCt).ConfigureAwait(false))
                {
                    observabilityScope.IncrementProcessed();
                    yield return item;
                }
            }
        }
    }
}

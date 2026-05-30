using System.Runtime.CompilerServices;
using System.Diagnostics;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Execution;

/// <summary>
///     Stream passthrough strategy that delegates directly to <see cref="IStreamTransformNode{TIn,TOut}.TransformAsync" />.
/// </summary>
/// <remarks>
///     This strategy preserves native stream semantics for stream nodes (including internal buffering/batching behavior).
///     It tracks observability and tracing at stream level, but it intentionally does not apply per-item retry orchestration.
///     Item-level lineage wrapping remains handled by the core node execution pipeline.
/// </remarks>
internal sealed class AIStreamPassthroughExecutionStrategy : IExecutionStrategy, IStreamExecutionStrategy
{
    private AIStreamPassthroughExecutionStrategy()
    {
    }

    public static AIStreamPassthroughExecutionStrategy Instance { get; } = new();

    /// <inheritdoc />
    public Task<IDataStream<TOut>> ExecuteAsync<TIn, TOut>(
        IDataStream<TIn> input,
        ITransformNode<TIn, TOut> node,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var nodeId = context.CurrentNodeId;

        async IAsyncEnumerable<TOut> Iterate([EnumeratorCancellation] CancellationToken ct)
        {
            using var observabilityScope = context.NodeExecutionScopeRegistry.BeginNodeScope(nodeId);
            using var activity = context.Tracer.StartActivity("Node.StreamTransform");
            var timedInput = NPipeline.Execution.NodeTimingDataStreamWrapper.WrapInputWait(input, observabilityScope);

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

                using var _ = context.ScopedNode(nodeId);
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

        return Task.FromResult<IDataStream<TOut>>(new DataStream<TOut>(Iterate(cancellationToken), input.StreamName));
    }

    /// <inheritdoc />
    public Task<IDataStream<TOut>> ExecuteAsync<TIn, TOut>(
        IDataStream<TIn> input,
        IStreamTransformNode<TIn, TOut> node,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var nodeId = context.CurrentNodeId;

        async IAsyncEnumerable<TOut> Iterate([EnumeratorCancellation] CancellationToken ct)
        {
            using var observabilityScope = context.NodeExecutionScopeRegistry.BeginNodeScope(nodeId);
            using var activity = context.Tracer.StartActivity("Node.StreamTransform");
            var timedInput = NPipeline.Execution.NodeTimingDataStreamWrapper.WrapInputWait(input, observabilityScope);

            async IAsyncEnumerable<TIn> TrackInput([EnumeratorCancellation] CancellationToken innerCt)
            {
                await foreach (var item in timedInput.WithCancellation(innerCt).ConfigureAwait(false))
                {
                    using var _ = context.ScopedNode(nodeId);
                    observabilityScope.IncrementProcessed();
                    yield return item;
                }
            }

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

                using var _ = context.ScopedNode(nodeId);
                observabilityScope.IncrementEmitted();
                yield return output;
            }

            // Stream nodes own work over the whole stream delegate, excluding input waits captured by timedInput.
            var breakdown = observabilityScope.GetTimingBreakdown();
            var work = breakdown.WallDuration - breakdown.InputWaitDuration;
            if (work > TimeSpan.Zero)
                observabilityScope.AddWork(work);
        }

        return Task.FromResult<IDataStream<TOut>>(new DataStream<TOut>(Iterate(cancellationToken), input.StreamName));
    }
}

using System.Runtime.CompilerServices;
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

            await foreach (var item in input.WithCancellation(ct).ConfigureAwait(false))
            {
                using var _ = context.ScopedNode(nodeId);
                observabilityScope.IncrementProcessed();

                var output = await node.TransformAsync(item, context, ct).ConfigureAwait(false);
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

            async IAsyncEnumerable<TIn> TrackInput([EnumeratorCancellation] CancellationToken innerCt)
            {
                await foreach (var item in input.WithCancellation(innerCt).ConfigureAwait(false))
                {
                    using var _ = context.ScopedNode(nodeId);
                    observabilityScope.IncrementProcessed();
                    yield return item;
                }
            }

            await foreach (var output in node.TransformAsync(TrackInput(ct), context, ct).WithCancellation(ct).ConfigureAwait(false))
            {
                using var _ = context.ScopedNode(nodeId);
                observabilityScope.IncrementEmitted();
                yield return output;
            }
        }

        return Task.FromResult<IDataStream<TOut>>(new DataStream<TOut>(Iterate(cancellationToken), input.StreamName));
    }
}

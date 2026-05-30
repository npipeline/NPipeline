using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Strategies;

/// <summary>
///     An execution strategy that flattens a stream of batches into a stream of individual items.
/// </summary>
public sealed class UnbatchingExecutionStrategy : IExecutionStrategy, IStreamExecutionStrategy
{
    /// <inheritdoc />
    public Task<IDataStream<TOut>> ExecuteAsync<TIn, TOut>(
        IDataStream<TIn> input,
        ITransformNode<TIn, TOut> node,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var nodeId = context.CurrentNodeId;
        var observabilityScope = context.NodeExecutionScopeRegistry.BeginNodeScope(nodeId);
        var timedInput = NPipeline.Execution.NodeTimingDataStreamWrapper.WrapInputWait(input, observabilityScope);

        // This strategy is designed to work with UnbatchingNode<T>, where TIn is IEnumerable<TOut>.
        // Therefore, input IDataStream<TIn> can be treated as an IAsyncEnumerable<IEnumerable<TOut>>.
        if (timedInput is not IAsyncEnumerable<IEnumerable<TOut>> batchedSource)

        // This should not happen if the pipeline is configured correctly.
        {
            throw new InvalidOperationException(
                $"The input for {nameof(UnbatchingExecutionStrategy)} must be an IAsyncEnumerable of IEnumerable<{typeof(TOut).Name}>.");
        }

        var flattenedSource = FlattenWithObservabilityAsync(batchedSource, observabilityScope, cancellationToken);

        var outputPipe = new DataStream<TOut>(flattenedSource, input.StreamName);

        // Use Task.FromResult for already-completed synchronous result
        return Task.FromResult<IDataStream<TOut>>(outputPipe);
    }

    /// <summary>
    ///     Executes unbatching strategy for stream transform nodes.
    /// </summary>
    public Task<IDataStream<TOut>> ExecuteAsync<TIn, TOut>(
        IDataStream<TIn> input,
        IStreamTransformNode<TIn, TOut> node,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var nodeId = context.CurrentNodeId;
        var observabilityScope = context.NodeExecutionScopeRegistry.BeginNodeScope(nodeId);
        var timedInput = NPipeline.Execution.NodeTimingDataStreamWrapper.WrapInputWait(input, observabilityScope);

        // This strategy is designed to work with UnbatchingNode<T>, where TIn is IEnumerable<TOut>.
        // Therefore, input IDataStream<TIn> can be treated as an IAsyncEnumerable<IEnumerable<TOut>>.
        if (timedInput is not IAsyncEnumerable<IEnumerable<TOut>> batchedSource)

        // This should not happen if the pipeline is configured correctly.
        {
            throw new InvalidOperationException(
                $"The input for {nameof(UnbatchingExecutionStrategy)} must be an IAsyncEnumerable of IEnumerable<{typeof(TOut).Name}>.");
        }

        var flattenedSource = FlattenWithObservabilityAsync(batchedSource, observabilityScope, cancellationToken);

        var outputPipe = new DataStream<TOut>(flattenedSource, input.StreamName);

        // Use Task.FromResult for already-completed synchronous result
        return Task.FromResult<IDataStream<TOut>>(outputPipe);
    }

    /// <summary>
    ///     Flattens batches with observability support.
    /// </summary>
    private static async IAsyncEnumerable<T> FlattenWithObservabilityAsync<T>(
        IAsyncEnumerable<IEnumerable<T>> batchedSource,
        IAutoObservabilityScope observabilityScope,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var scope = observabilityScope;

        await using var batchEnumerator = batchedSource.WithCancellation(cancellationToken).GetAsyncEnumerator();

        while (true)
        {
            IEnumerable<T> batch;

            try
            {
                if (!await batchEnumerator.MoveNextAsync())
                    break;

                batch = batchEnumerator.Current;
            }
            catch (Exception ex)
            {
                scope.RecordFailure(ex);
                throw;
            }

            // Track item processed (one batch)
            scope.IncrementProcessed();

            using var itemEnumerator = batch.GetEnumerator();

            while (true)
            {
                T item;

                try
                {
                    if (!itemEnumerator.MoveNext())
                        break;

                    item = itemEnumerator.Current;
                }
                catch (Exception ex)
                {
                    scope.RecordFailure(ex);
                    throw;
                }

                // Track item emitted (each item in the batch)
                scope.IncrementEmitted();
                yield return item;
            }
        }
    }
}

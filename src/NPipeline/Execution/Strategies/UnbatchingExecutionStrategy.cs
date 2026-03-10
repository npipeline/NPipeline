using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
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
        // This strategy is designed to work with UnbatchingNode<T>, where TIn is IEnumerable<TOut>.
        // Therefore, input IDataStream<TIn> can be treated as an IAsyncEnumerable<IEnumerable<TOut>>.
        if (input is not IAsyncEnumerable<IEnumerable<TOut>> batchedSource)

            // This should not happen if the pipeline is configured correctly.
        {
            throw new InvalidOperationException(
                $"The input for {nameof(UnbatchingExecutionStrategy)} must be an IAsyncEnumerable of IEnumerable<{typeof(TOut).Name}>.");
        }

        var nodeId = context.CurrentNodeId;

        // Get observability scope if available
        IAutoObservabilityScope? observabilityScope = null;

        if (context.NodeObservabilityScopes.TryGetValue(nodeId, out var scope))
            observabilityScope = scope;

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
        // This strategy is designed to work with UnbatchingNode<T>, where TIn is IEnumerable<TOut>.
        // Therefore, input IDataStream<TIn> can be treated as an IAsyncEnumerable<IEnumerable<TOut>>.
        if (input is not IAsyncEnumerable<IEnumerable<TOut>> batchedSource)

            // This should not happen if the pipeline is configured correctly.
        {
            throw new InvalidOperationException(
                $"The input for {nameof(UnbatchingExecutionStrategy)} must be an IAsyncEnumerable of IEnumerable<{typeof(TOut).Name}>.");
        }

        var nodeId = context.CurrentNodeId;

        // Get observability scope if available
        IAutoObservabilityScope? observabilityScope = null;

        if (context.NodeObservabilityScopes.TryGetValue(nodeId, out var scope))
            observabilityScope = scope;

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
        IAutoObservabilityScope? observabilityScope,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var batch in batchedSource.WithCancellation(cancellationToken))
            {
                // Track item processed (one batch)
                observabilityScope?.IncrementProcessed();

                foreach (var item in batch)
                {
                    // Track item emitted (each item in the batch)
                    observabilityScope?.IncrementEmitted();
                    yield return item;
                }
            }
        }
        finally
        {
            // Dispose AutoObservabilityScope after all items are processed, even on failure or cancellation
            observabilityScope?.Dispose();
        }
    }
}

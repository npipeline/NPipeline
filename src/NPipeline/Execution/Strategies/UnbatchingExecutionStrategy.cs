using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Strategies;

/// <summary>
///     An execution strategy that flattens a stream of batches into a stream of individual items.
/// </summary>
public sealed class UnbatchingExecutionStrategy : IExecutionStrategy
{
    /// <inheritdoc />
    public Task<IDataPipe<TOut>> ExecuteAsync<TIn, TOut>(
        IDataPipe<TIn> input,
        ITransformNode<TIn, TOut> node,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // This strategy is designed to work with UnbatchingNode<T>, where TIn is IEnumerable<TOut>.
        // Therefore, the input IDataPipe<TIn> can be treated as an IAsyncEnumerable<IEnumerable<TOut>>.
        if (input is not IAsyncEnumerable<IEnumerable<TOut>> batchedSource)

            // This should not happen if the pipeline is configured correctly.
        {
            throw new InvalidOperationException(
                $"The input for {nameof(UnbatchingExecutionStrategy)} must be an IAsyncEnumerable of IEnumerable<{typeof(TOut).Name}>.");
        }

        var flattenedSource = batchedSource.FlattenAsync(cancellationToken);

        var outputPipe = new StreamingDataPipe<TOut>(flattenedSource, input.StreamName);

        // Use Task.FromResult for already-completed synchronous result
        return Task.FromResult<IDataPipe<TOut>>(outputPipe);
    }
}

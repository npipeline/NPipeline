using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Execution.Strategies;
using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     A transform node that flattens a stream of read-only collection batches into a stream of individual items.
///     This node works in conjunction with the <see cref="UnbatchingExecutionStrategy" />.
/// </summary>
/// <typeparam name="T">The type of item in the batches.</typeparam>
public sealed class ReadOnlyCollectionUnbatchingNode<T> : IStreamTransformNode<IReadOnlyCollection<T>, T>
{
    /// <inheritdoc />
    public IExecutionStrategy ExecutionStrategy { get; set; } = new UnbatchingExecutionStrategy();

    /// <summary>
    ///     Transforms an input stream of read-only collection batches into a stream of individual items asynchronously.
    /// </summary>
    /// <param name="items">The input stream of read-only collection batches to flatten.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The output stream of individual items.</returns>
    public async IAsyncEnumerable<T> TransformAsync(
        IAsyncEnumerable<IReadOnlyCollection<T>> items,
        PipelineContext context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var item in items.FlattenAsync(cancellationToken))
        {
            yield return item;
        }
    }

    /// <summary>
    ///     Asynchronously disposes of the node.
    /// </summary>
    /// <returns>A <see cref="ValueTask" /> that represents the asynchronous dispose operation.</returns>
    public ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }
}
using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Execution.Strategies;
using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     A transform node that batches incoming items into collections based on size or time.
///     This node works in conjunction with the <see cref="BatchingExecutionStrategy" />.
/// </summary>
/// <typeparam name="T">The type of items to batch.</typeparam>
public sealed class BatchingNode<T>(int batchSize, TimeSpan timespan)
    : IStreamTransformNode<T, IReadOnlyCollection<T>>, IExecutionStrategyProvider
{
    /// <inheritdoc />
    public IExecutionStrategy DefaultExecutionStrategy { get; } = new BatchingExecutionStrategy(batchSize, timespan);

    /// <summary>
    ///     Transforms an input stream of items into batches of items asynchronously.
    /// </summary>
    /// <param name="items">The input stream of items to batch.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The output stream of batched items.</returns>
    public async IAsyncEnumerable<IReadOnlyCollection<T>> TransformAsync(
        IAsyncEnumerable<T> items,
        PipelineContext context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Delegate to the BatchAsync extension method for the actual batching logic
        await foreach (var batch in items.BatchAsync(batchSize, timespan, cancellationToken))
        {
            yield return batch;
        }
    }
}

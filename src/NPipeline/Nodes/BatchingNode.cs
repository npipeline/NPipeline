using NPipeline.Execution.Strategies;
using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     A transform node that batches incoming items into collections based on size or time.
///     This node works in conjunction with the <see cref="BatchingExecutionStrategy" />.
/// </summary>
/// <typeparam name="T">The type of items to batch.</typeparam>
public sealed class BatchingNode<T> : TransformNode<T, IReadOnlyCollection<T>>
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="BatchingNode{T}" /> class.
    /// </summary>
    /// <param name="batchSize">The maximum number of items in a batch.</param>
    /// <param name="timespan">The maximum time to wait before emitting a batch.</param>
    public BatchingNode(int batchSize, TimeSpan timespan)
        : base(new BatchingExecutionStrategy(batchSize, timespan))
    {
    }

    /// <summary>
    ///     This method is not supported for the <see cref="BatchingNode{T}" />, as batching is handled by the execution strategy.
    /// </summary>
    public override Task<IReadOnlyCollection<T>> ExecuteAsync(T item, PipelineContext context, CancellationToken cancellationToken)
    {
        // This method will not be called by the BatchingExecutionStrategy, which operates on the entire stream.
        throw new NotSupportedException(
            $"The {nameof(BatchingNode<T>)} does not support item-by-item transformation. It relies on the {nameof(BatchingExecutionStrategy)} to process the stream.");
    }
}

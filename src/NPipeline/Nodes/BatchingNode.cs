using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Strategies;
using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     A transform node that batches incoming items into collections based on size or time.
///     This node works in conjunction with the <see cref="BatchingExecutionStrategy" />.
/// </summary>
/// <typeparam name="T">The type of items to batch.</typeparam>
public sealed class BatchingNode<T>(int batchSize, TimeSpan timespan) : IStreamTransformNode<T, IReadOnlyCollection<T>>
{
    private INodeErrorHandler? _errorHandler;

    /// <inheritdoc />
    public IExecutionStrategy ExecutionStrategy { get; set; } = new BatchingExecutionStrategy(batchSize, timespan);

    /// <inheritdoc />
    public INodeErrorHandler? ErrorHandler
    {
        get => _errorHandler;
        set
        {
            if (value is not null and not INodeErrorHandler<IStreamTransformNode<T, IReadOnlyCollection<T>>, T>)
            {
                throw new ArgumentException(
                    $"The provided error handler is not of the expected type INodeErrorHandler<IStreamTransformNode<T, IReadOnlyCollection<T>>, T> for node {GetType().Name}.",
                    nameof(value));
            }

            _errorHandler = value;
        }
    }

    /// <summary>
    ///     Transforms an input stream of items into batches of items asynchronously.
    /// </summary>
    /// <param name="items">The input stream of items to batch.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The output stream of batched items.</returns>
    public async IAsyncEnumerable<IReadOnlyCollection<T>> ExecuteAsync(
        IAsyncEnumerable<T> items,
        PipelineContext context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Get the batching parameters from the execution strategy
        if (ExecutionStrategy is not BatchingExecutionStrategy batchingStrategy)
            throw new InvalidOperationException($"The {nameof(BatchingNode<T>)} requires a {nameof(BatchingExecutionStrategy)} to be configured.");

        // Delegate to the BatchAsync extension method for the actual batching logic
        await foreach (var batch in items.BatchAsync(batchingStrategy.BatchSize, batchingStrategy.Timespan, cancellationToken))
        {
            yield return batch;
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

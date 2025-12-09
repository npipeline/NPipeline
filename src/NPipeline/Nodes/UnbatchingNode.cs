using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Strategies;
using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     A transform node that flattens a stream of batches into a stream of individual items.
///     This node works in conjunction with the <see cref="UnbatchingExecutionStrategy" />.
/// </summary>
/// <typeparam name="T">The type of item in the batches.</typeparam>
public sealed class UnbatchingNode<T> : IStreamTransformNode<IEnumerable<T>, T>
{
    private INodeErrorHandler? _errorHandler;

    /// <inheritdoc />
    public IExecutionStrategy ExecutionStrategy { get; set; } = new UnbatchingExecutionStrategy();

    /// <inheritdoc />
    public INodeErrorHandler? ErrorHandler
    {
        get => _errorHandler;
        set
        {
            if (value is not null and not INodeErrorHandler<IStreamTransformNode<IEnumerable<T>, T>, IEnumerable<T>>)
            {
                throw new ArgumentException(
                    $"The provided error handler is not of the expected type INodeErrorHandler<IStreamTransformNode<IEnumerable<T>, T>, IEnumerable<T>> for node {GetType().Name}.",
                    nameof(value));
            }

            _errorHandler = value;
        }
    }

    /// <summary>
    ///     Transforms an input stream of batches into a stream of individual items asynchronously.
    /// </summary>
    /// <param name="items">The input stream of batches to flatten.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The output stream of individual items.</returns>
    public async IAsyncEnumerable<T> ExecuteAsync(
        IAsyncEnumerable<IEnumerable<T>> items,
        PipelineContext context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Delegate to the FlattenAsync extension method for the actual unbatching logic
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

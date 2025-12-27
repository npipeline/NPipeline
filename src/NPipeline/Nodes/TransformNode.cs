using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Strategies;
using NPipeline.Pipeline;
using NPipeline.Utils;

namespace NPipeline.Nodes;

/// <summary>
///     A base class for transform nodes.
/// </summary>
/// <typeparam name="TIn">The input item type.</typeparam>
/// <typeparam name="TOut">The output item type.</typeparam>
public abstract class TransformNode<TIn, TOut>
    : ITransformNode<TIn, TOut>, INodeTypeMetadata, IValueTaskTransform<TIn, TOut>
{
    private INodeErrorHandler? _errorHandler;

    /// <summary>
    ///     Gets the strongly-typed error handler for this node.
    ///     <para>
    ///         This property is optimized for performance by caching the typed reference during setter validation.
    ///         Since type validation occurs at assignment time, the cached reference is guaranteed to be of the correct type,
    ///         allowing this property to be a simple field read rather than a repeated cast operation.
    ///     </para>
    /// </summary>
    protected INodeErrorHandler<ITransformNode<TIn, TOut>, TIn>? TypedErrorHandler { get; private set; }

    /// <summary>
    ///     Gets the input type of the transform node.
    /// </summary>
    public Type InputType => typeof(TIn);

    /// <summary>
    ///     Gets the output type of the transform node.
    /// </summary>
    public Type OutputType => typeof(TOut);

    /// <summary>
    ///     Gets or sets the execution strategy for this transform node.
    ///     Defaults to <see cref="SequentialExecutionStrategy" />.
    ///     <para>
    ///         Use custom strategies for parallel processing, batching, or other advanced execution patterns.
    ///         Set this property directly or via the fluent API using <c>WithExecutionStrategy</c> extension method.
    ///     </para>
    /// </summary>
    public IExecutionStrategy ExecutionStrategy { get; set; } = new SequentialExecutionStrategy();

    /// <inheritdoc />
    public INodeErrorHandler? ErrorHandler
    {
        get => _errorHandler;
        set
        {
            if (value is not null and not INodeErrorHandler<ITransformNode<TIn, TOut>, TIn>)
            {
                throw new ArgumentException(
                    $"The provided error handler is not of the expected type INodeErrorHandler<ITransformNode<TIn, TOut>, TIn> for node {GetType().Name}.",
                    nameof(value));
            }

            _errorHandler = value;

            // Cache the typed reference for zero-cost property access (avoids repeated cast operations)
            TypedErrorHandler = value as INodeErrorHandler<ITransformNode<TIn, TOut>, TIn>;
        }
    }

    /// <inheritdoc />
    public abstract Task<TOut> ExecuteAsync(TIn item, PipelineContext context, CancellationToken cancellationToken);

    /// <summary>
    ///     Asynchronously disposes of the node. This can be overridden by derived classes to release resources.
    /// </summary>
    /// <returns>A <see cref="ValueTask" /> that represents the asynchronous dispose operation.</returns>
    public virtual ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask; // base holds no resources
    }

    ValueTask<TOut> IValueTaskTransform<TIn, TOut>.ExecuteValueTaskAsync(TIn item, PipelineContext context, CancellationToken cancellationToken)
    {
        return ExecuteValueTaskAsync(item, context, cancellationToken);
    }

    /// <summary>
    ///     Provides a ValueTask-based execution hook for execution strategies that can take advantage of synchronous completions.
    ///     <para>
    ///         By default this wraps <see cref="ExecuteAsync" /> so existing Task-based implementations continue working.
    ///         Derived nodes can override to return a naturally produced <see cref="ValueTask{TOut}" /> to avoid per-item allocations.
    ///     </para>
    /// </summary>
    /// <param name="item">The input item.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="ValueTask{TOut}" /> representing the transformation.</returns>
    protected internal virtual ValueTask<TOut> ExecuteValueTaskAsync(TIn item, PipelineContext context, CancellationToken cancellationToken)
    {
        return new ValueTask<TOut>(ExecuteAsync(item, context, cancellationToken));
    }

    /// <summary>
    ///     Helper for converting a <see cref="ValueTask{TOut}" /> to a <see cref="Task{TOut}" /> for the <see cref="ExecuteAsync" /> method.
    /// </summary>
    /// <param name="work">The ValueTask to convert.</param>
    /// <returns>A <see cref="Task{TOut}" /> representing the same asynchronous operation.</returns>
    protected Task<TOut> FromValueTask(ValueTask<TOut> work)
    {
        return ValueTaskHelpers.ToTask(work);
    }
}

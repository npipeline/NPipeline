using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Strategies;
using NPipeline.Pipeline;
using NPipeline.Utils;

namespace NPipeline.Nodes;

/// <summary>
///     Wraps a <see cref="ValueTask" />-producing delegate so it can be consumed through the existing Task-based transform interface.
///     Primarily intended for tests and adapters that generate transforms dynamically without creating a dedicated node type.
/// </summary>
/// <typeparam name="TIn">Input type.</typeparam>
/// <typeparam name="TOut">Output type.</typeparam>
internal sealed class ValueTaskTransformAdapter<TIn, TOut> : ITransformNode<TIn, TOut>, IValueTaskTransform<TIn, TOut>
{
    private readonly Func<TIn, PipelineContext, CancellationToken, ValueTask<TOut>> _producer;

    public ValueTaskTransformAdapter(
        Func<TIn, PipelineContext, CancellationToken, ValueTask<TOut>> producer,
        IExecutionStrategy? executionStrategy = null,
        INodeErrorHandler? errorHandler = null)
    {
        ArgumentNullException.ThrowIfNull(producer);
        _producer = producer;
        ExecutionStrategy = executionStrategy ?? new SequentialExecutionStrategy();
        ErrorHandler = errorHandler;
    }

    public IExecutionStrategy ExecutionStrategy { get; set; }

    public INodeErrorHandler? ErrorHandler { get; set; }

    public Task<TOut> ExecuteAsync(TIn item, PipelineContext context, CancellationToken cancellationToken)
    {
        return ValueTaskHelpers.ToTask(_producer(item, context, cancellationToken));
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }

    public ValueTask<TOut> ExecuteValueTaskAsync(TIn item, PipelineContext context, CancellationToken cancellationToken)
    {
        return _producer(item, context, cancellationToken);
    }
}

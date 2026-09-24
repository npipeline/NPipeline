using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     Wraps a <see cref="ValueTask" />-producing delegate so it can be consumed through the existing Task-based transform interface.
///     Primarily intended for tests and adapters that generate transforms dynamically without creating a dedicated node type.
/// </summary>
/// <typeparam name="TIn">Input type.</typeparam>
/// <typeparam name="TOut">Output type.</typeparam>
internal sealed class ValueTaskTransformAdapter<TIn, TOut> : ITransformNode<TIn, TOut>
{
    private readonly Func<TIn, PipelineContext, CancellationToken, ValueTask<TOut>> _producer;

    /// <summary>
    ///     Initializes a new instance of the <see cref="ValueTaskTransformAdapter{TIn, TOut}" /> class.
    /// </summary>
    /// <param name="producer">The delegate that produces a ValueTask representing the transformation.</param>
    /// <exception cref="ArgumentNullException">Thrown when producer is null.</exception>
    public ValueTaskTransformAdapter(
        Func<TIn, PipelineContext, CancellationToken, ValueTask<TOut>> producer)
    {
        ArgumentNullException.ThrowIfNull(producer);
        _producer = producer;
    }

    public ValueTask<TOut> TransformAsync(TIn item, PipelineContext context, CancellationToken cancellationToken) =>
        _producer(item, context, cancellationToken);
}

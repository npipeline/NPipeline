using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     Internal contract exposing a ValueTask-based execution path for transform nodes.
///     This enables execution strategies to avoid creating intermediary <see cref="Task" /> instances
///     when transforms can complete synchronously.
/// </summary>
/// <typeparam name="TIn">The input item type.</typeparam>
/// <typeparam name="TOut">The output item type.</typeparam>
internal interface IValueTaskTransform<in TIn, TOut>
{
    /// <summary>
    ///     Executes the transform using a <see cref="ValueTask{TResult}" /> to allow allocation-friendly fast paths.
    /// </summary>
    /// <param name="item">The input item.</param>
    /// <param name="context">The current pipeline context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="ValueTask{TResult}" /> representing the transformation.</returns>
    ValueTask<TOut> ExecuteValueTaskAsync(TIn item, PipelineContext context, CancellationToken cancellationToken);
}

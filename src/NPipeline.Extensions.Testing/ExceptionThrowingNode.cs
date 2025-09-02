using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Testing;

/// <summary>
///     A transform node that throws a specified exception when executed.
/// </summary>
/// <typeparam name="TIn">The input item type.</typeparam>
public sealed class ExceptionThrowingNode<TIn> : TransformNode<TIn, TIn>
{
    private readonly Exception _exceptionToThrow;

    /// <summary>
    ///     Initializes a new instance of the <see cref="ExceptionThrowingNode{TIn}" /> class.
    /// </summary>
    /// <param name="exceptionToThrow">The exception to throw when the node is executed.</param>
    public ExceptionThrowingNode(Exception exceptionToThrow)
    {
        _exceptionToThrow = exceptionToThrow ?? throw new ArgumentNullException(nameof(exceptionToThrow));
    }

    /// <inheritdoc />
    public override Task<TIn> ExecuteAsync(TIn item, PipelineContext context, CancellationToken cancellationToken)
    {
        throw _exceptionToThrow;
    }
}

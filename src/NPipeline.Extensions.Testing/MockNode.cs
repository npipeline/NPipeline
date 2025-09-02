using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Testing;

/// <summary>
///     A mock transform node that can be configured with a delegate to provide its transformation logic.
/// </summary>
/// <typeparam name="TIn">The input item type.</typeparam>
/// <typeparam name="TOut">The output item type.</typeparam>
public sealed class MockNode<TIn, TOut> : TransformNode<TIn, TOut>
{
    private readonly Func<TIn, PipelineContext, CancellationToken, Task<TOut>> _transformLogic;

    /// <summary>
    ///     Initializes a new instance of the <see cref="MockNode{TIn, TOut}" /> class.
    /// </summary>
    /// <param name="transformLogic">The delegate that defines the transformation logic.</param>
    public MockNode(Func<TIn, PipelineContext, CancellationToken, Task<TOut>> transformLogic)
    {
        _transformLogic = transformLogic ?? throw new ArgumentNullException(nameof(transformLogic));
    }

    /// <inheritdoc />
    public override Task<TOut> ExecuteAsync(TIn item, PipelineContext context, CancellationToken cancellationToken)
    {
        return _transformLogic(item, context, cancellationToken);
    }
}

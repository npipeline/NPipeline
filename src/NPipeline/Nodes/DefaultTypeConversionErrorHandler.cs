using NPipeline.ErrorHandling;
using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     Default node-level handler for type conversion failures.
///     - When the error is a FormatException, InvalidCastException, or OverflowException returns the configured <see cref="NodeErrorDecision" />.
///     - For any other exception type, returns <see cref="NodeErrorDecision.Fail" />.
///     This enables a DX-friendly onFailure option without requiring users to implement custom handlers for routine cases.
/// </summary>
public sealed class DefaultTypeConversionErrorHandler<TIn, TOut>(NodeErrorDecision decision) : INodeErrorHandler<ITransformNode<TIn, TOut>, TIn>
{
    public Task<NodeErrorDecision> HandleAsync(
        ITransformNode<TIn, TOut> node,
        TIn failedItem,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        if (error is FormatException or InvalidCastException or OverflowException)
            return Task.FromResult(decision);

        return Task.FromResult(NodeErrorDecision.Fail);
    }
}

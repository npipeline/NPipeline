using NPipeline.ErrorHandling;
using NPipeline.Nodes;

namespace NPipeline.Tests.Common;

public sealed class RetryHandler : INodeErrorHandler<ITransformNode<int, int>, int>
{
    public Task<NodeErrorDecision> HandleAsync(ITransformNode<int, int> node, int failedItem, NodeFailureContext failure,
        CancellationToken cancellationToken)
    {
        return Task.FromResult(NodeErrorDecision.Retry);
    }
}

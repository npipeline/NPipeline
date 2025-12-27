using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Common;

public sealed class RetryHandler : INodeErrorHandler<ITransformNode<int, int>, int>
{
    public Task<NodeErrorDecision> HandleAsync(ITransformNode<int, int> node, int failedItem, Exception error, PipelineContext context,
        CancellationToken cancellationToken)
    {
        return Task.FromResult(NodeErrorDecision.Retry);
    }
}

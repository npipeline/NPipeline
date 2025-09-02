using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Execution;

public interface IErrorHandlingService
{
    Task ExecuteWithRetriesAsync(
        NodeDefinition nodeDef,
        INode nodeInstance,
        PipelineGraph graph,
        PipelineContext context,
        Func<Task> executeBody,
        CancellationToken cancellationToken);
}

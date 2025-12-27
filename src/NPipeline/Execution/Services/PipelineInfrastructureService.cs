using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Services;

/// <summary>
///     Implementation of IPipelineInfrastructureService that provides infrastructure services
///     for pipeline execution including error handling and persistence.
/// </summary>
public sealed class PipelineInfrastructureService(
    IErrorHandlingService errorHandlingService,
    IPersistenceService persistenceService) : IPipelineInfrastructureService
{
    /// <inheritdoc />
    public Task ExecuteWithRetriesAsync(
        NodeDefinition nodeDef,
        INode nodeInstance,
        PipelineGraph graph,
        PipelineContext context,
        Func<Task> executeBody,
        CancellationToken cancellationToken)
    {
        return errorHandlingService.ExecuteWithRetriesAsync(
            nodeDef,
            nodeInstance,
            graph,
            context,
            executeBody,
            cancellationToken);
    }

    /// <inheritdoc />
    public void TryPersistAfterNode(PipelineContext context, NodeExecutionCompleted completedEvent)
    {
        persistenceService.TryPersistAfterNode(context, completedEvent);
    }
}

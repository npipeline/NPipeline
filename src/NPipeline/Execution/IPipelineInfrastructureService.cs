using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Execution;

/// <summary>
///     Facade interface that provides infrastructure services for pipeline execution including
///     error handling and persistence. See <see href="~/docs/reference/api/pipeline-infrastructure-service.md" />
///     for detailed documentation.
/// </summary>
public interface IPipelineInfrastructureService
{
    /// <summary>
    ///     Executes a node with retry logic and error handling.
    /// </summary>
    /// <param name="nodeDef">The definition of the node to execute.</param>
    /// <param name="nodeInstance">The instantiated node object.</param>
    /// <param name="graph">The pipeline graph containing node definitions and configuration.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="executeBody">The execution body to run with retry logic.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>A task that represents the asynchronous execution.</returns>
    Task ExecuteWithRetriesAsync(
        NodeDefinition nodeDef,
        INode nodeInstance,
        PipelineGraph graph,
        PipelineContext context,
        Func<Task> executeBody,
        CancellationToken cancellationToken);

    /// <summary>
    ///     Attempts to persist state after node completion.
    /// </summary>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="completedEvent">The node execution completed event containing state information.</param>
    void TryPersistAfterNode(PipelineContext context, NodeExecutionCompleted completedEvent);
}

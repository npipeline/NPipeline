using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Execution;

/// <summary>
///     Provides error handling services for pipeline execution, including retry logic and circuit breaker functionality.
/// </summary>
public interface IErrorHandlingService
{
    /// <summary>
    ///     Executes a node with retry logic and circuit breaker protection.
    /// </summary>
    /// <param name="nodeDef">The definition of the node to execute.</param>
    /// <param name="nodeInstance">The instance of the node to execute.</param>
    /// <param name="graph">The pipeline graph containing the node.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="executeBody">The execution body to run with retry logic.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task representing the asynchronous execution.</returns>
    Task ExecuteWithRetriesAsync(
        NodeDefinition nodeDef,
        INode nodeInstance,
        PipelineGraph graph,
        PipelineContext context,
        Func<Task> executeBody,
        CancellationToken cancellationToken);
}

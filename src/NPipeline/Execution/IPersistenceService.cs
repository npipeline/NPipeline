using NPipeline.Pipeline;

namespace NPipeline.Execution;

/// <summary>
///     Provides snapshot persistence after node completion.
/// </summary>
public interface IPersistenceService
{
    /// <summary>
    ///     Attempts to persist the pipeline state after a node completes execution.
    /// </summary>
    /// <param name="context">The pipeline context containing execution state.</param>
    /// <param name="completedEvent">The event containing information about the completed node execution.</param>
    void TryPersistAfterNode(PipelineContext context, NodeExecutionCompleted completedEvent);
}

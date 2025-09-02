using NPipeline.Pipeline;

namespace NPipeline.Execution;

/// <summary>
///     Provides snapshot persistence after node completion.
/// </summary>
public interface IPersistenceService
{
    void TryPersistAfterNode(PipelineContext context, NodeExecutionCompleted completedEvent);
}

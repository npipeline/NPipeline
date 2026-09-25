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
    /// <returns>A <see cref="ValueTask" /> that completes when the snapshot attempt has finished.</returns>
    /// <remarks>
    ///     Await this so a snapshot cannot still be running after the run returns and the context is disposed.
    ///     A snapshot for a lazy node is taken when its stream is created, not when data flows through it.
    /// </remarks>
    ValueTask TryPersistAfterNode(PipelineContext context, NodeExecutionCompleted completedEvent);
}

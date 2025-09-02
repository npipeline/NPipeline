using NPipeline.Pipeline;

namespace NPipeline.State;

/// <summary>
///     Manages pipeline state persistence and snapshot operations.
///     This interface provides a strongly-typed abstraction for state management.
/// </summary>
public interface IPipelineStateManager
{
    /// <summary>
    ///     Creates a snapshot of the current pipeline state asynchronously.
    /// </summary>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="forceFullSnapshot">Whether to force a full snapshot even if incremental is possible.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask CreateSnapshotAsync(PipelineContext context, CancellationToken cancellationToken, bool forceFullSnapshot = false);

    /// <summary>
    ///     Attempts to restore pipeline state from a snapshot asynchronously.
    /// </summary>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if state was successfully restored, false otherwise.</returns>
    ValueTask<bool> TryRestoreAsync(PipelineContext context, CancellationToken cancellationToken);

    /// <summary>
    ///     Marks a node as completed for state tracking purposes.
    /// </summary>
    /// <param name="nodeId">The ID of the completed node.</param>
    /// <param name="context">The pipeline execution context.</param>
    void MarkNodeCompleted(string nodeId, PipelineContext context);

    /// <summary>
    ///     Marks a node as having encountered an error for state tracking purposes.
    /// </summary>
    /// <param name="nodeId">The ID of the node that encountered an error.</param>
    /// <param name="context">The pipeline execution context.</param>
    void MarkNodeError(string nodeId, PipelineContext context);
}

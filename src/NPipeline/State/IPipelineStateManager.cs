using NPipeline.Pipeline;

namespace NPipeline.State;

/// <summary>
///     Manages pipeline state persistence and snapshot operations.
///     This interface provides a strongly-typed abstraction for state management.
/// </summary>
/// <remarks>
///     <para>
///         <strong>Purpose:</strong>
///         When used with <see cref="PipelineContext" />, this interface allows for thread-safe state management
///         in scenarios requiring shared state across parallel execution or resilient restart capabilities.
///     </para>
///     <para>
///         <strong>Thread Safety:</strong>
///         Implementations of <see cref="IPipelineStateManager" /> should be thread-safe, as they may be accessed
///         from multiple worker threads during parallel node execution. The implementation is responsible for
///         synchronizing access to shared state.
///     </para>
///     <para>
///         <strong>Usage Pattern:</strong>
///         Register an implementation in <see cref="PipelineContext.Properties" /> under the key
///         <c>"NPipeline.StateManager"</c> or use <see cref="PipelineContext.StateManager" /> property accessor
///         for convenient access.
///     </para>
///     <para>
///         <strong>Alternative to Context Dictionaries:</strong>
///         Rather than directly accessing <see cref="PipelineContext.Items" /> or <see cref="PipelineContext.Parameters" />
///         from multiple threads, use this interface to ensure thread-safe state management.
///     </para>
/// </remarks>
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

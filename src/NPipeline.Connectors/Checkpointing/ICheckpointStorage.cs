namespace NPipeline.Connectors.Checkpointing;

/// <summary>
///     Interface for checkpoint storage backends.
///     Implementations can store checkpoints in files, databases, cloud storage, etc.
/// </summary>
public interface ICheckpointStorage
{
    /// <summary>
    ///     Loads a checkpoint for the specified pipeline and node.
    /// </summary>
    /// <param name="pipelineId">The unique identifier of the pipeline.</param>
    /// <param name="nodeId">The unique identifier of the node.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The checkpoint if found; otherwise, null.</returns>
    Task<Checkpoint?> LoadAsync(string pipelineId, string nodeId, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Saves a checkpoint for the specified pipeline and node.
    /// </summary>
    /// <param name="pipelineId">The unique identifier of the pipeline.</param>
    /// <param name="nodeId">The unique identifier of the node.</param>
    /// <param name="checkpoint">The checkpoint to save.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task SaveAsync(string pipelineId, string nodeId, Checkpoint checkpoint, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Deletes a checkpoint for the specified pipeline and node.
    /// </summary>
    /// <param name="pipelineId">The unique identifier of the pipeline.</param>
    /// <param name="nodeId">The unique identifier of the node.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task DeleteAsync(string pipelineId, string nodeId, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Checks if a checkpoint exists for the specified pipeline and node.
    /// </summary>
    /// <param name="pipelineId">The unique identifier of the pipeline.</param>
    /// <param name="nodeId">The unique identifier of the node.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>True if a checkpoint exists; otherwise, false.</returns>
    Task<bool> ExistsAsync(string pipelineId, string nodeId, CancellationToken cancellationToken = default);
}

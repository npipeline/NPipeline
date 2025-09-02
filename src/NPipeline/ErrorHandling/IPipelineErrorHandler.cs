using NPipeline.Pipeline;

namespace NPipeline.ErrorHandling;

/// <summary>
///     Defines the contract for handling catastrophic errors that affect the entire pipeline or a specific node.
/// </summary>
public interface IPipelineErrorHandler
{
    /// <summary>
    ///     Handles a failure that occurred at the node level, which could impact the entire pipeline.
    /// </summary>
    /// <param name="nodeId">The ID of the node that failed.</param>
    /// <param name="error">The exception that caused the failure.</param>
    /// <param name="context">The current pipeline context.</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests.</param>
    /// <returns>A <see cref="PipelineErrorDecision" /> indicating how to proceed.</returns>
    Task<PipelineErrorDecision> HandleNodeFailureAsync(
        string nodeId,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken);
}

using NPipeline.Pipeline;

namespace NPipeline.ErrorHandling;

/// <summary>
///     Provides structured failure context to node error handlers, combining the exception,
///     pipeline context, failure attribution, and retry state into a single parameter.
/// </summary>
public sealed class NodeFailureContext
{
    /// <summary>
    ///     Initializes a new instance of <see cref="NodeFailureContext" />.
    /// </summary>
    /// <param name="exception">The exception that caused the failure.</param>
    /// <param name="pipelineContext">The active pipeline context.</param>
    /// <param name="attribution">The failure attribution identifying origin and decision nodes.</param>
    /// <param name="retryAttempt">The current retry attempt number (0 on first failure).</param>
    public NodeFailureContext(
        Exception exception,
        PipelineContext pipelineContext,
        NodeFailureAttribution attribution,
        int retryAttempt)
    {
        Exception = exception;
        PipelineContext = pipelineContext;
        Attribution = attribution;
        RetryAttempt = retryAttempt;
    }

    /// <summary>
    ///     Gets the exception that caused the node failure.
    /// </summary>
    public Exception Exception { get; }

    /// <summary>
    ///     Gets the active pipeline execution context.
    /// </summary>
    public PipelineContext PipelineContext { get; }

    /// <summary>
    ///     Gets the failure attribution identifying origin and decision nodes.
    /// </summary>
    public NodeFailureAttribution Attribution { get; }

    /// <summary>
    ///     Gets the current retry attempt number (0 on first failure).
    /// </summary>
    public int RetryAttempt { get; }
}

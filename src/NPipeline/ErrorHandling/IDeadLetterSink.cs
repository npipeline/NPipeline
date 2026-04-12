using NPipeline.Pipeline;

namespace NPipeline.ErrorHandling;

/// <summary>
///     Defines the contract for a dead-letter sink, which handles items that fail processing permanently.
/// </summary>
/// <remarks>
///     <para>
///         A dead-letter sink receives a <see cref="DeadLetterEnvelope" /> containing the failed item,
///         the exception, and full failure attribution (origin node, decision node, pipeline and run identity).
///     </para>
///     <para>
///         Common implementations:
///         - File-based storage (JSON, CSV)
///         - Database tables for failed items
///         - Message queues for reprocessing attempts
///         - Logging/alerting systems
///     </para>
///     <para>
///         When an error handler returns <see cref="NodeErrorDecision.DeadLetter" />,
///         the framework constructs a <see cref="DeadLetterEnvelope" /> and passes it to this sink.
///     </para>
/// </remarks>
public interface IDeadLetterSink
{
    /// <summary>
    ///     Handles a dead-lettered item by persisting it for later analysis.
    /// </summary>
    /// <param name="envelope">The envelope containing the failed item, exception, and failure attribution.</param>
    /// <param name="context">The current pipeline context.</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests.</param>
    Task HandleAsync(DeadLetterEnvelope envelope, PipelineContext context, CancellationToken cancellationToken);
}

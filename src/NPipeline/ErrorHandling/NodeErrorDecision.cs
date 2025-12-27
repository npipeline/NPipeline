namespace NPipeline.ErrorHandling;

/// <summary>
///     Specifies the decision to be made after a node-level error has been handled.
/// </summary>
/// <remarks>
///     <para>
///         When a node fails to process an item, the error handler is called to decide how to proceed.
///         This enum represents the four possible recovery strategies:
///     </para>
///     <para>
///         - <see cref="Fail" />: Stop the entire pipeline (appropriate for critical data quality issues)
///         - <see cref="Skip" />: Skip the failing item and continue (for non-critical items)
///         - <see cref="Retry" />: Attempt the same item again (for transient failures)
///         - <see cref="DeadLetter" />: Send to dead-letter sink for analysis and potential reprocessing
///     </para>
///     <para>
///         The decision should be based on:
///         - Error type (is it transient or permanent?)
///         - Item criticality (how important is this specific item?)
///         - Error context (how many retries have already occurred?)
///         - Business requirements (can we skip this item without data loss?)
///     </para>
/// </remarks>
public enum NodeErrorDecision
{
    /// <summary>
    ///     Indicates that the entire pipeline should fail. This is the default behavior if no error handler is present or if the handler chooses to fail.
    /// </summary>
    /// <remarks>
    ///     Use this when the error represents a critical issue that should halt processing.
    ///     Examples: Invalid configuration, security violations, data corruption.
    /// </remarks>
    Fail,

    /// <summary>
    ///     Indicates that the item that caused the error should be skipped, and processing should continue with the next item.
    /// </summary>
    /// <remarks>
    ///     Use this when the failing item is non-critical or when the error is expected.
    ///     Examples: Filtering based on validation rules, expected data format mismatches.
    ///     Note: Skipped items are lost unless also sent to a dead-letter sink.
    /// </remarks>
    Skip,

    /// <summary>
    ///     Indicates that the operation for the failed item should be retried.
    /// </summary>
    /// <remarks>
    ///     Use this for transient errors that may succeed on retry.
    ///     Examples: Temporary network failures, connection timeouts, resource contention.
    ///     The pipeline respects retry limits configured in <see cref="Configuration.PipelineRetryOptions" />.
    /// </remarks>
    Retry,

    /// <summary>
    ///     Indicates that the failed item should be sent to a dead-letter sink for analysis or later reprocessing.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Use this when you want to preserve the failing item for analysis or later reprocessing.
    ///         The item and error context are sent to the <see cref="IDeadLetterSink" /> configured in
    ///         the pipeline context.
    ///     </para>
    ///     <para>
    ///         This is often combined with continuing pipeline execution. Some pipelines use this to
    ///         separate problematic items while allowing good items to proceed.
    ///     </para>
    /// </remarks>
    DeadLetter,
}

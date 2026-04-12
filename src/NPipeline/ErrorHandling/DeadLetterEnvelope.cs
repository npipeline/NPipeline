namespace NPipeline.ErrorHandling;

/// <summary>
///     Wraps a dead-lettered item with its error and full failure attribution.
///     Passed to <see cref="IDeadLetterSink.HandleAsync" /> as the single envelope parameter.
/// </summary>
/// <param name="Item">The item that failed processing.</param>
/// <param name="Error">The exception that caused the failure.</param>
/// <param name="Attribution">The failure attribution identifying origin and decision nodes.</param>
public sealed record DeadLetterEnvelope(
    object Item,
    Exception Error,
    NodeFailureAttribution Attribution);

namespace NPipeline.Connectors.Messaging;

/// <summary>What a message-queue sink does with a message it could not publish, after any retries.</summary>
public enum FailedMessageAction
{
    /// <summary>
    ///     The write fails. Messages of the batch that were published are settled; the failed one is not, so it is delivered
    ///     again. (For Kafka, whose offsets are positional, commits stop at the failed message.)
    /// </summary>
    Fail,

    /// <summary>
    ///     The source message is rejected with requeue, so it is delivered again later, and the write continues. A plain body,
    ///     with no source message to requeue, fails the write instead.
    /// </summary>
    Requeue,

    /// <summary>The body goes to the pipeline's dead-letter sink, the source message is acknowledged, and the write continues.</summary>
    DeadLetter,
}

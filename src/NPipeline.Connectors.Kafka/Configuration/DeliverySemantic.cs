namespace NPipeline.Connectors.Kafka.Configuration;

/// <summary>
///     Defines the delivery semantic guarantee for Kafka operations.
/// </summary>
public enum DeliverySemantic
{
    /// <summary>
    ///     At-least-once delivery: messages may be delivered more than once,
    ///     but no messages are lost. This is the default.
    /// </summary>
    AtLeastOnce,

    /// <summary>
    ///     Exactly-once delivery: messages are delivered exactly once.
    ///     Requires transactions and idempotent producers.
    /// </summary>
    ExactlyOnce,
}

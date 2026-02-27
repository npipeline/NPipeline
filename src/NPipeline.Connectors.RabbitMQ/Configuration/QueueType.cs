namespace NPipeline.Connectors.RabbitMQ.Configuration;

/// <summary>
///     Defines the type of RabbitMQ queue.
/// </summary>
public enum QueueType
{
    /// <summary>
    ///     Classic (non-replicated) queue. Legacy — prefer Quorum for new deployments.
    /// </summary>
    Classic,

    /// <summary>
    ///     Quorum queue — replicated, durable, no message loss on broker restart.
    ///     This is the recommended default.
    /// </summary>
    Quorum,

    /// <summary>
    ///     Stream queue — append-only log with offset-based consumption.
    ///     Not fully supported in v1 (classic AMQP only).
    /// </summary>
    Stream,
}

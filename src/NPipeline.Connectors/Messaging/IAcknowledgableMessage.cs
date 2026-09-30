namespace NPipeline.Connectors.Messaging;

/// <summary>
///     A message received from a broker, which stays with the broker until it is settled: acknowledged once it has been
///     handled, or rejected. A message that is never settled is delivered again (RabbitMQ, SQS, Service Bus) or read again
///     after a restart (Kafka).
/// </summary>
/// <remarks>
///     Settle a message once. The first settlement wins; later calls, and calls on a copy made with
///     <see cref="IAcknowledgableMessage{T}.WithBody{TNew}" />, return the first one's task.
/// </remarks>
public interface IAcknowledgableMessage
{
    /// <summary>The deserialized body.</summary>
    object? Body { get; }

    /// <summary>The broker's message id, or one the connector derived from the message's position.</summary>
    string MessageId { get; }

    /// <summary>Whether the message has been acknowledged or rejected.</summary>
    bool IsSettled { get; }

    /// <summary>The broker's properties and headers for the message.</summary>
    IReadOnlyDictionary<string, object> Metadata { get; }

    /// <summary>Tells the broker the message was handled, so it is not delivered again.</summary>
    Task AcknowledgeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    ///     Tells the broker the message was not handled. With <paramref name="requeue" />, it is delivered again; without, it
    ///     goes to the broker's dead-letter queue if it has one (RabbitMQ's dead-letter exchange, Service Bus's dead-letter
    ///     sub-queue), or is deleted (SQS). Kafka keeps no per-message state: a requeued message is read again after a
    ///     restart, and a rejected one is skipped.
    /// </summary>
    Task RejectAsync(bool requeue, CancellationToken cancellationToken = default);
}

/// <summary>A received message with a body of type <typeparamref name="T" />.</summary>
/// <typeparam name="T">The body type.</typeparam>
public interface IAcknowledgableMessage<T> : IAcknowledgableMessage
{
    /// <summary>The deserialized body.</summary>
    new T Body { get; }

    /// <summary>
    ///     The same message with another body, for a transform that maps the body and keeps the message: settling either
    ///     settles both, and the metadata is shared.
    /// </summary>
    IAcknowledgableMessage<TNew> WithBody<TNew>(TNew body);
}

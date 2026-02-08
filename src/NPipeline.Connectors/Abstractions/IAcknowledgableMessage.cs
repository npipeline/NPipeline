namespace NPipeline.Connectors.Abstractions
{
    /// <summary>
    /// Interface for messages that can be acknowledged after processing.
    /// This allows any sink to acknowledge messages regardless of the underlying
    /// messaging system (SQS, Service Bus, RabbitMQ, etc.).
    /// </summary>
    public interface IAcknowledgableMessage
    {
        /// <summary>
        /// Gets the deserialized message body.
        /// </summary>
        object Body { get; }

        /// <summary>
        /// Gets a unique identifier for the message.
        /// </summary>
        string MessageId { get; }

        /// <summary>
        /// Gets a value indicating whether this message has been acknowledged.
        /// </summary>
        bool IsAcknowledged { get; }

        /// <summary>
        /// Acknowledges the message, indicating successful processing.
        /// This method should be idempotent - calling it multiple times should have no adverse effects.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token for the operation.</param>
        /// <returns>A task representing the asynchronous acknowledgment operation.</returns>
        Task AcknowledgeAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets metadata associated with the message.
        /// The structure and content of metadata depends on the underlying messaging system.
        /// </summary>
        IReadOnlyDictionary<string, object> Metadata { get; }
    }

    /// <summary>
    /// Generic interface for messages that can be acknowledged after processing.
    /// </summary>
    /// <typeparam name="T">The type of the message body.</typeparam>
    public interface IAcknowledgableMessage<T> : IAcknowledgableMessage
    {
        /// <summary>
        /// Gets the deserialized message body.
        /// </summary>
        new T Body { get; }

        /// <summary>
        /// Creates a new acknowledgable message with the provided body while preserving acknowledgment semantics.
        /// </summary>
        /// <typeparam name="TNew">The new body type.</typeparam>
        /// <param name="body">The new message body.</param>
        /// <returns>A new acknowledgable message wrapping the provided body.</returns>
        IAcknowledgableMessage<TNew> WithBody<TNew>(TNew body);
    }
}

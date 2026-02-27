using RabbitMQ.Client;

namespace NPipeline.Connectors.RabbitMQ.Models;

/// <summary>
///     Exposes RabbitMQ-specific metadata from a consumed message.
/// </summary>
public interface IRabbitMqMessageMetadata
{
    /// <summary>Gets the exchange the message was published to.</summary>
    string Exchange { get; }

    /// <summary>Gets the routing key used when the message was published.</summary>
    string RoutingKey { get; }

    /// <summary>Gets the delivery tag assigned by the broker.</summary>
    ulong DeliveryTag { get; }

    /// <summary>Gets whether this is a redelivery.</summary>
    bool Redelivered { get; }

    /// <summary>Gets the correlation ID, if present.</summary>
    string? CorrelationId { get; }

    /// <summary>Gets the content type, if present.</summary>
    string? ContentType { get; }

    /// <summary>Gets the content encoding, if present.</summary>
    string? ContentEncoding { get; }

    /// <summary>Gets the reply-to address, if present.</summary>
    string? ReplyTo { get; }

    /// <summary>Gets the message type, if present.</summary>
    string? Type { get; }

    /// <summary>Gets the AMQP timestamp, if present.</summary>
    AmqpTimestamp? Timestamp { get; }

    /// <summary>Gets the message priority (0-9), if present.</summary>
    byte? Priority { get; }

    /// <summary>Gets the message headers.</summary>
    IDictionary<string, object?>? Headers { get; }
}

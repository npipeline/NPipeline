namespace NPipeline.Connectors.Azure.ServiceBus.Models;

/// <summary>
///     Exposes Azure Service Bus-specific metadata from a consumed message.
/// </summary>
public interface IServiceBusMessageMetadata
{
    /// <summary>Gets the session ID, or <c>null</c> if the entity is not session-enabled.</summary>
    string? SessionId { get; }

    /// <summary>Gets the correlation identifier set by the sender, or <c>null</c>.</summary>
    string? CorrelationId { get; }

    /// <summary>Gets the reply-to address, or <c>null</c>.</summary>
    string? ReplyTo { get; }

    /// <summary>Gets the destination address set by the sender, or <c>null</c>.</summary>
    string? To { get; }

    /// <summary>Gets the application-specific label (subject), or <c>null</c>.</summary>
    string? Subject { get; }

    /// <summary>Gets the session ID to which reply messages should be sent, or <c>null</c>.</summary>
    string? ReplyToSessionId { get; }

    /// <summary>Gets the time at which the message was enqueued.</summary>
    DateTimeOffset EnqueuedTime { get; }

    /// <summary>Gets the number of times this message has been delivered.</summary>
    int DeliveryCount { get; }

    /// <summary>Gets the partition key, or <c>null</c>.</summary>
    string? PartitionKey { get; }

    /// <summary>Gets the message Time-To-Live.</summary>
    TimeSpan TimeToLive { get; }

    /// <summary>Gets the MIME content type of the message body.</summary>
    string? ContentType { get; }

    /// <summary>Gets whether the message has already been settled (completed/abandoned/dead-lettered/deferred).</summary>
    bool IsSettled { get; }

    /// <summary>Gets the application-defined properties attached to the message.</summary>
    IReadOnlyDictionary<string, object> ApplicationProperties { get; }
}

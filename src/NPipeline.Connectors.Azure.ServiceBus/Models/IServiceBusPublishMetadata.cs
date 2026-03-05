namespace NPipeline.Connectors.Azure.ServiceBus.Models;

/// <summary>
///     Optional outbound metadata contract for sink payloads published to Azure Service Bus.
/// </summary>
/// <remarks>
///     Implement this interface on payload types to control broker-level message metadata
///     such as scheduling, identifiers, and routing/session properties.
/// </remarks>
public interface IServiceBusPublishMetadata
{
    /// <summary>Gets the message identifier for duplicate detection scenarios.</summary>
    string? MessageId { get; }

    /// <summary>Gets the correlation identifier.</summary>
    string? CorrelationId { get; }

    /// <summary>Gets the session identifier for ordered processing.</summary>
    string? SessionId { get; }

    /// <summary>Gets the partition key.</summary>
    string? PartitionKey { get; }

    /// <summary>Gets the subject (label).</summary>
    string? Subject { get; }

    /// <summary>Gets the content type override.</summary>
    string? ContentType { get; }

    /// <summary>Gets the time-to-live override.</summary>
    TimeSpan? TimeToLive { get; }

    /// <summary>Gets the scheduled enqueue timestamp in UTC.</summary>
    DateTimeOffset? ScheduledEnqueueTimeUtc { get; }

    /// <summary>Gets custom application properties to attach to the outbound message.</summary>
    IReadOnlyDictionary<string, object>? ApplicationProperties { get; }
}

using Azure.Messaging.ServiceBus;
using NPipeline.Connectors.Messaging;

namespace NPipeline.Connectors.Azure.ServiceBus.Models;

/// <summary>
///     A message received from a Service Bus queue or subscription, locked until it is settled: completed, abandoned,
///     dead-lettered or deferred. The source renews its lock while it waits. A message never settled is abandoned when the
///     source's receiver closes, so it is delivered again.
/// </summary>
/// <typeparam name="T">The body type.</typeparam>
public sealed class ServiceBusMessage<T> : IAcknowledgableMessage<T>, IServiceBusReceived
{
    private readonly Lazy<IReadOnlyDictionary<string, object>> _metadata;
    private readonly ServiceBusReceiver _receiver;
    private readonly MessageSettlement _settlement;

    internal ServiceBusMessage(T body, ServiceBusReceivedMessage received, ServiceBusReceiver receiver, MessageSettlement settlement,
        Lazy<IReadOnlyDictionary<string, object>>? metadata = null)
    {
        Body = body;
        Received = received;
        _receiver = receiver;
        _settlement = settlement;
        _metadata = metadata ?? new Lazy<IReadOnlyDictionary<string, object>>(BuildMetadata);
    }

    /// <summary>The message as the SDK received it, with every broker property.</summary>
    public ServiceBusReceivedMessage Received { get; }

    /// <summary>The session, for a session-enabled entity.</summary>
    public string? SessionId => Received.SessionId;

    /// <summary>The correlation id.</summary>
    public string? CorrelationId => Received.CorrelationId;

    /// <summary>The subject (label).</summary>
    public string? Subject => Received.Subject;

    /// <summary>The content type.</summary>
    public string? ContentType => Received.ContentType;

    /// <summary>How many times the message has been delivered, this time included.</summary>
    public int DeliveryCount => Received.DeliveryCount;

    /// <summary>When the broker accepted the message.</summary>
    public DateTimeOffset EnqueuedTime => Received.EnqueuedTime;

    /// <summary>The application properties the sender set.</summary>
    public IReadOnlyDictionary<string, object> ApplicationProperties => Received.ApplicationProperties;

    /// <inheritdoc />
    public T Body { get; }

    object? IAcknowledgableMessage.Body => Body;

    /// <inheritdoc />
    public string MessageId => Received.MessageId;

    /// <inheritdoc />
    public bool IsSettled => _settlement.IsSettled;

    /// <inheritdoc />
    public IReadOnlyDictionary<string, object> Metadata => _metadata.Value;

    /// <summary>Completes the message, removing it from the entity.</summary>
    public Task AcknowledgeAsync(CancellationToken cancellationToken = default) => _settlement.AcknowledgeAsync(cancellationToken);

    /// <summary>Rejects the message: with <paramref name="requeue" /> it is abandoned and delivered again; without, it goes to the dead-letter sub-queue.</summary>
    public Task RejectAsync(bool requeue, CancellationToken cancellationToken = default) => _settlement.RejectAsync(requeue, cancellationToken);

    /// <summary>Moves the message to the dead-letter sub-queue with a reason.</summary>
    public Task DeadLetterAsync(string reason, string? description = null, CancellationToken cancellationToken = default) =>
        _settlement.SettleWith(ct => _receiver.DeadLetterMessageAsync(Received, reason, description, ct), cancellationToken);

    /// <summary>Defers the message: it stays in the entity, to be received later by its sequence number only.</summary>
    public Task DeferAsync(CancellationToken cancellationToken = default) =>
        _settlement.SettleWith(ct => _receiver.DeferMessageAsync(Received, cancellationToken: ct), cancellationToken);

    /// <inheritdoc />
    public IAcknowledgableMessage<TNew> WithBody<TNew>(TNew body) => new ServiceBusMessage<TNew>(body, Received, _receiver, _settlement, _metadata);

    private IReadOnlyDictionary<string, object> BuildMetadata()
    {
        var metadata = new Dictionary<string, object>
        {
            ["SequenceNumber"] = Received.SequenceNumber,
            ["DeliveryCount"] = Received.DeliveryCount,
            ["EnqueuedTime"] = Received.EnqueuedTime,
        };

        if (SessionId is not null)
            metadata["SessionId"] = SessionId;

        if (CorrelationId is not null)
            metadata["CorrelationId"] = CorrelationId;

        if (Subject is not null)
            metadata["Subject"] = Subject;

        foreach (var (key, value) in Received.ApplicationProperties)
        {
            metadata[$"Property.{key}"] = value;
        }

        return metadata;
    }
}

/// <summary>What a Service Bus sink carries on from a received message whatever its body type.</summary>
internal interface IServiceBusReceived
{
    ServiceBusReceivedMessage Received { get; }
}

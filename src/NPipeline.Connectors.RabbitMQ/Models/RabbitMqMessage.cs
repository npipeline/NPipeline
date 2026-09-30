using System.Text;
using NPipeline.Connectors.Messaging;
using RabbitMQ.Client;

namespace NPipeline.Connectors.RabbitMQ.Models;

/// <summary>
///     A message received from a RabbitMQ queue. Acknowledge it once it is handled, or reject it; a message neither
///     acknowledged nor rejected is delivered again once the source's channel closes.
/// </summary>
/// <typeparam name="T">The body type.</typeparam>
public sealed class RabbitMqMessage<T> : IAcknowledgableMessage<T>, IRabbitMqReceived
{
    private readonly Lazy<IReadOnlyDictionary<string, object>> _metadata;
    private readonly MessageSettlement _settlement;

    internal RabbitMqMessage(T body, string messageId, string exchange, string routingKey, ulong deliveryTag, bool redelivered,
        IReadOnlyBasicProperties properties, MessageSettlement settlement)
        : this(body, messageId, exchange, routingKey, deliveryTag, redelivered, properties, settlement, null)
    {
    }

    private RabbitMqMessage(T body, string messageId, string exchange, string routingKey, ulong deliveryTag, bool redelivered,
        IReadOnlyBasicProperties properties, MessageSettlement settlement, Lazy<IReadOnlyDictionary<string, object>>? metadata)
    {
        Body = body;
        MessageId = messageId;
        Exchange = exchange;
        RoutingKey = routingKey;
        DeliveryTag = deliveryTag;
        Redelivered = redelivered;
        Properties = properties;
        _settlement = settlement;
        _metadata = metadata ?? new Lazy<IReadOnlyDictionary<string, object>>(BuildMetadata);
    }

    /// <summary>The exchange the message was published to.</summary>
    public string Exchange { get; }

    /// <summary>The routing key it was published with.</summary>
    public string RoutingKey { get; }

    /// <summary>The delivery tag on the source's channel.</summary>
    public ulong DeliveryTag { get; }

    /// <summary>Whether the broker delivered the message before.</summary>
    public bool Redelivered { get; }

    /// <summary>The message's properties: correlation id, headers, content type, priority and the rest.</summary>
    public IReadOnlyBasicProperties Properties { get; }

    /// <summary>The correlation id, if the publisher set one.</summary>
    public string? CorrelationId => Properties.CorrelationId;

    /// <summary>The headers, if any.</summary>
    public IDictionary<string, object?>? Headers => Properties.Headers;

    /// <inheritdoc />
    public T Body { get; }

    object? IAcknowledgableMessage.Body => Body;

    /// <inheritdoc />
    public string MessageId { get; }

    /// <inheritdoc />
    public bool IsSettled => _settlement.IsSettled;

    /// <inheritdoc />
    public IReadOnlyDictionary<string, object> Metadata => _metadata.Value;

    /// <inheritdoc />
    public Task AcknowledgeAsync(CancellationToken cancellationToken = default) => _settlement.AcknowledgeAsync(cancellationToken);

    /// <summary>Rejects the message: with <paramref name="requeue" /> it goes back on the queue, without it to the queue's dead-letter exchange, if any.</summary>
    public Task RejectAsync(bool requeue, CancellationToken cancellationToken = default) => _settlement.RejectAsync(requeue, cancellationToken);

    /// <inheritdoc />
    public IAcknowledgableMessage<TNew> WithBody<TNew>(TNew body) =>
        new RabbitMqMessage<TNew>(body, MessageId, Exchange, RoutingKey, DeliveryTag, Redelivered, Properties, _settlement, _metadata);

    private IReadOnlyDictionary<string, object> BuildMetadata()
    {
        var metadata = new Dictionary<string, object>
        {
            ["Exchange"] = Exchange,
            ["RoutingKey"] = RoutingKey,
            ["DeliveryTag"] = DeliveryTag,
            ["Redelivered"] = Redelivered,
        };

        if (CorrelationId is not null)
            metadata["CorrelationId"] = CorrelationId;

        if (Properties.ContentType is not null)
            metadata["ContentType"] = Properties.ContentType;

        foreach (var (key, value) in Headers ?? new Dictionary<string, object?>())
        {
            if (value is null)
                continue;

            // AMQP carries string headers as bytes.
            metadata[$"Header.{key}"] = value is byte[] bytes ? Encoding.UTF8.GetString(bytes) : value;
        }

        return metadata;
    }
}

/// <summary>A received message's properties whatever its body type, so a sink can carry them on.</summary>
internal interface IRabbitMqReceived
{
    IReadOnlyBasicProperties Properties { get; }
}

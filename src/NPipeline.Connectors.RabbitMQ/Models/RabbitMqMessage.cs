using System.Text;
using NPipeline.Connectors.Abstractions;
using RabbitMQ.Client;

namespace NPipeline.Connectors.RabbitMQ.Models;

/// <summary>
///     Acknowledgment state for a RabbitMQ message.
/// </summary>
internal enum AckState
{
    Pending,
    Acknowledged,
    NegativelyAcknowledged,
}

/// <summary>
///     RabbitMQ-specific implementation of <see cref="IAcknowledgableMessage{T}" /> that wraps a consumed
///     message with ack/nack callbacks via <see cref="IChannel" /> and delivery tag.
/// </summary>
/// <typeparam name="T">The deserialized message body type.</typeparam>
public sealed class RabbitMqMessage<T> : IAcknowledgableMessage<T>, IRabbitMqMessageMetadata
{
    private readonly Func<CancellationToken, Task>? _ackCallback;
    private readonly Lazy<IReadOnlyDictionary<string, object>> _metadata;
    private readonly Func<bool, CancellationToken, Task>? _nackCallback;
    private int _ackState; // 0 = Pending, 1 = Acked, 2 = Nacked

    /// <summary>
    ///     Production constructor — wraps a consumed message with channel-based ack/nack callbacks.
    /// </summary>
    internal RabbitMqMessage(
        T body,
        string messageId,
        string exchange,
        string routingKey,
        ulong deliveryTag,
        bool redelivered,
        IReadOnlyBasicProperties properties,
        Func<CancellationToken, Task> ackCallback,
        Func<bool, CancellationToken, Task> nackCallback)
    {
        Body = body;
        MessageId = messageId;
        Exchange = exchange;
        RoutingKey = routingKey;
        DeliveryTag = deliveryTag;
        Redelivered = redelivered;
        CorrelationId = properties.CorrelationId;
        ContentType = properties.ContentType;
        ContentEncoding = properties.ContentEncoding;
        ReplyTo = properties.ReplyTo;
        Type = properties.Type;
        Timestamp = properties.Timestamp;
        Priority = properties.Priority;
        Headers = properties.Headers;
        _ackCallback = ackCallback;
        _nackCallback = nackCallback;
        _metadata = new Lazy<IReadOnlyDictionary<string, object>>(BuildMetadata);
    }

    /// <summary>
    ///     Testing constructor — uses delegate callbacks.
    /// </summary>
    public RabbitMqMessage(
        T body,
        string messageId,
        Func<CancellationToken, Task>? ackCallback = null,
        Func<bool, CancellationToken, Task>? nackCallback = null,
        IReadOnlyDictionary<string, object>? metadata = null)
    {
        Body = body;
        MessageId = messageId;
        Exchange = "";
        RoutingKey = "";
        _ackCallback = ackCallback;
        _nackCallback = nackCallback;

        _metadata = metadata is not null
            ? new Lazy<IReadOnlyDictionary<string, object>>(metadata)
            : new Lazy<IReadOnlyDictionary<string, object>>(() => new Dictionary<string, object>());
    }

    /// <summary>
    ///     Private constructor for WithBody projections — preserves all metadata and callbacks.
    /// </summary>
    private RabbitMqMessage(
        T body,
        string messageId,
        string exchange,
        string routingKey,
        ulong deliveryTag,
        bool redelivered,
        string? correlationId,
        string? contentType,
        string? contentEncoding,
        string? replyTo,
        string? type,
        AmqpTimestamp? timestamp,
        byte? priority,
        IDictionary<string, object?>? headers,
        Func<CancellationToken, Task>? ackCallback,
        Func<bool, CancellationToken, Task>? nackCallback,
        Lazy<IReadOnlyDictionary<string, object>> metadata,
        int ackState)
    {
        Body = body;
        MessageId = messageId;
        Exchange = exchange;
        RoutingKey = routingKey;
        DeliveryTag = deliveryTag;
        Redelivered = redelivered;
        CorrelationId = correlationId;
        ContentType = contentType;
        ContentEncoding = contentEncoding;
        ReplyTo = replyTo;
        Type = type;
        Timestamp = timestamp;
        Priority = priority;
        Headers = headers;
        _ackCallback = ackCallback;
        _nackCallback = nackCallback;
        _metadata = metadata;
        _ackState = ackState;
    }

    /// <summary>
    ///     Gets the current acknowledgment state of this message.
    /// </summary>
    internal AckState CurrentAckState => (AckState)Volatile.Read(ref _ackState);

    // IAcknowledgableMessage

    /// <inheritdoc />
    public T Body { get; }

    object IAcknowledgableMessage.Body => Body!;

    /// <inheritdoc />
    public string MessageId { get; }

    /// <inheritdoc />
    public bool IsAcknowledged => Volatile.Read(ref _ackState) == (int)AckState.Acknowledged;

    /// <inheritdoc />
    public IReadOnlyDictionary<string, object> Metadata => _metadata.Value;

    /// <inheritdoc />
    public async Task AcknowledgeAsync(CancellationToken cancellationToken = default)
    {
        var previous = Interlocked.CompareExchange(ref _ackState, (int)AckState.Acknowledged, (int)AckState.Pending);

        if (previous == (int)AckState.Acknowledged)
            return; // Idempotent

        if (previous == (int)AckState.NegativelyAcknowledged)
            throw new InvalidOperationException("Cannot acknowledge a message that has already been negatively acknowledged.");

        if (_ackCallback is not null)
            await _ackCallback(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task NegativeAcknowledgeAsync(bool requeue = true, CancellationToken cancellationToken = default)
    {
        var previous = Interlocked.CompareExchange(
            ref _ackState, (int)AckState.NegativelyAcknowledged, (int)AckState.Pending);

        if (previous == (int)AckState.NegativelyAcknowledged)
            return; // Idempotent

        if (previous == (int)AckState.Acknowledged)
            throw new InvalidOperationException("Cannot negatively acknowledge a message that has already been acknowledged.");

        if (_nackCallback is not null)
            await _nackCallback(requeue, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public IAcknowledgableMessage<TNew> WithBody<TNew>(TNew body)
    {
        return new RabbitMqMessage<TNew>(
            body,
            MessageId,
            Exchange,
            RoutingKey,
            DeliveryTag,
            Redelivered,
            CorrelationId,
            ContentType,
            ContentEncoding,
            ReplyTo,
            Type,
            Timestamp,
            Priority,
            Headers,
            _ackCallback,
            _nackCallback,
            _metadata,
            Volatile.Read(ref _ackState));
    }

    // IRabbitMqMessageMetadata

    /// <inheritdoc />
    public string Exchange { get; }

    /// <inheritdoc />
    public string RoutingKey { get; }

    /// <inheritdoc />
    public ulong DeliveryTag { get; }

    /// <inheritdoc />
    public bool Redelivered { get; }

    /// <inheritdoc />
    public string? CorrelationId { get; }

    /// <inheritdoc />
    public string? ContentType { get; }

    /// <inheritdoc />
    public string? ContentEncoding { get; }

    /// <inheritdoc />
    public string? ReplyTo { get; }

    /// <inheritdoc />
    public string? Type { get; }

    /// <inheritdoc />
    public AmqpTimestamp? Timestamp { get; }

    /// <inheritdoc />
    public byte? Priority { get; }

    /// <inheritdoc />
    public IDictionary<string, object?>? Headers { get; }

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

        if (ContentType is not null)
            metadata["ContentType"] = ContentType;

        if (Headers is not null)
        {
            foreach (var header in Headers)
            {
                if (header.Value is null)
                    continue;

                if (header.Value is byte[] bytes)
                {
                    try
                    {
                        metadata[$"Header.{header.Key}"] = Encoding.UTF8.GetString(bytes);
                    }
                    catch
                    {
                        metadata[$"Header.{header.Key}"] = Convert.ToBase64String(bytes);
                    }
                }
                else
                    metadata[$"Header.{header.Key}"] = header.Value;
            }
        }

        return metadata;
    }
}

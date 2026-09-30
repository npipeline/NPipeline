using System.Text;
using Confluent.Kafka;
using NPipeline.Connectors.Messaging;

namespace NPipeline.Connectors.Kafka.Models;

/// <summary>
///     A message read from a Kafka topic. Acknowledging it lets the group's committed offset move past it, once every
///     earlier message of its partition is settled too; a message never acknowledged is read again after a restart.
/// </summary>
/// <typeparam name="T">The body type.</typeparam>
public sealed class KafkaMessage<T> : IAcknowledgableMessage<T>, IKafkaReceived
{
    private readonly Func<IConsumerGroupMetadata>? _groupMetadata;
    private readonly Lazy<IReadOnlyDictionary<string, object>> _metadata;
    private readonly MessageSettlement _settlement;

    internal KafkaMessage(T body, TopicPartitionOffset position, string? key, DateTimeOffset timestamp, Headers headers, bool isTombstone,
        MessageSettlement settlement, Func<IConsumerGroupMetadata>? groupMetadata, Lazy<IReadOnlyDictionary<string, object>>? metadata = null)
    {
        Body = body;
        Position = position;
        Key = key;
        Timestamp = timestamp;
        Headers = headers;
        IsTombstone = isTombstone;
        _settlement = settlement;
        _groupMetadata = groupMetadata;
        _metadata = metadata ?? new Lazy<IReadOnlyDictionary<string, object>>(BuildMetadata);
    }

    /// <summary>The topic, partition and offset the message was read from.</summary>
    public TopicPartitionOffset Position { get; }

    /// <summary>The topic.</summary>
    public string Topic => Position.Topic;

    /// <summary>The partition.</summary>
    public int Partition => Position.Partition.Value;

    /// <summary>The offset in its partition.</summary>
    public long Offset => Position.Offset.Value;

    /// <summary>The key as UTF-8 text, or <c>null</c> when the message has none.</summary>
    public string? Key { get; }

    /// <summary>When the message was produced, or appended by the broker, as the topic records it.</summary>
    public DateTimeOffset Timestamp { get; }

    /// <summary>The message's headers.</summary>
    public Headers Headers { get; }

    /// <summary>Whether the message is a tombstone: a null value, which marks its key deleted in a compacted topic.</summary>
    public bool IsTombstone { get; }

    /// <inheritdoc />
    public T Body { get; }

    object? IAcknowledgableMessage.Body => Body;

    /// <summary>The message's position, <c>topic/partition/offset</c>, which is unique.</summary>
    public string MessageId => $"{Topic}/{Partition}/{Offset}";

    /// <inheritdoc />
    public bool IsSettled => _settlement.IsSettled;

    /// <inheritdoc />
    public IReadOnlyDictionary<string, object> Metadata => _metadata.Value;

    IConsumerGroupMetadata? IKafkaReceived.GroupMetadata() => _groupMetadata?.Invoke();

    /// <inheritdoc />
    public Task AcknowledgeAsync(CancellationToken cancellationToken = default) => _settlement.AcknowledgeAsync(cancellationToken);

    /// <summary>
    ///     Rejects the message. Without <paramref name="requeue" />, it is skipped: the committed offset may move past it.
    ///     With it, commits for its partition stop at it, so a restart reads it (and what follows) again; Kafka cannot
    ///     redeliver a single message.
    /// </summary>
    public Task RejectAsync(bool requeue, CancellationToken cancellationToken = default) => _settlement.RejectAsync(requeue, cancellationToken);

    /// <inheritdoc />
    public IAcknowledgableMessage<TNew> WithBody<TNew>(TNew body) =>
        new KafkaMessage<TNew>(body, Position, Key, Timestamp, Headers, IsTombstone, _settlement, _groupMetadata, _metadata);

    private IReadOnlyDictionary<string, object> BuildMetadata()
    {
        var metadata = new Dictionary<string, object>
        {
            ["Topic"] = Topic,
            ["Partition"] = Partition,
            ["Offset"] = Offset,
            ["Timestamp"] = Timestamp,
        };

        if (Key is not null)
            metadata["Key"] = Key;

        foreach (var header in Headers)
        {
            metadata[$"Header.{header.Key}"] = Encoding.UTF8.GetString(header.GetValueBytes());
        }

        return metadata;
    }
}

/// <summary>What a Kafka sink needs from a received message whatever its body type: its key, headers, position and group.</summary>
internal interface IKafkaReceived
{
    TopicPartitionOffset Position { get; }

    string? Key { get; }

    Headers Headers { get; }

    /// <summary>The source consumer's group metadata, for a transaction that commits the message's offset.</summary>
    IConsumerGroupMetadata? GroupMetadata();
}

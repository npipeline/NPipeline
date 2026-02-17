using System.Text;
using Confluent.Kafka;
using NPipeline.Connectors.Abstractions;

namespace NPipeline.Connectors.Kafka.Models;

/// <summary>
///     Kafka-specific implementation of <see cref="IAcknowledgableMessage{T}" /> that wraps a Kafka message
///     with acknowledgment capability via offset commit.
/// </summary>
/// <typeparam name="T">The deserialized message body type.</typeparam>
public sealed class KafkaMessage<T> : IAcknowledgableMessage<T>, IKafkaMessageMetadata
{
    private readonly object _ackLock = new();
    private readonly Func<CancellationToken, Task>? _acknowledgeCallback;
    private readonly Dictionary<string, object> _metadata;
    private Task? _ackTask;
    private volatile bool _isAcknowledged;

    /// <summary>
    ///     Initializes a new instance of <see cref="KafkaMessage{T}" />.
    /// </summary>
    /// <param name="body">The deserialized message body.</param>
    /// <param name="topic">The topic the message was consumed from.</param>
    /// <param name="partition">The partition number.</param>
    /// <param name="offset">The offset within the partition.</param>
    /// <param name="key">The message key.</param>
    /// <param name="timestamp">The message timestamp.</param>
    /// <param name="headers">The message headers.</param>
    /// <param name="acknowledgeCallback">The callback to invoke when acknowledging the message.</param>
    /// <param name="consumerGroupMetadata">The consumer group metadata for exactly-once semantics.</param>
    public KafkaMessage(
        T body,
        string topic,
        int partition,
        long offset,
        string key,
        DateTime timestamp,
        Headers headers,
        Func<CancellationToken, Task>? acknowledgeCallback,
        IConsumerGroupMetadata? consumerGroupMetadata = null)
    {
        Body = body;
        Topic = topic;
        Partition = partition;
        Offset = offset;
        Key = key;
        Timestamp = timestamp;
        Headers = headers ?? [];
        _acknowledgeCallback = acknowledgeCallback;
        TopicPartitionOffset = new TopicPartitionOffset(topic, new Partition(partition), new Offset(offset));
        ConsumerGroupMetadata = consumerGroupMetadata;
        _metadata = BuildMetadata();
    }

    /// <summary>
    ///     Gets the topic partition offset for this message, used for exactly-once semantics.
    /// </summary>
    public TopicPartitionOffset TopicPartitionOffset { get; }

    /// <summary>
    ///     Gets the consumer group metadata for exactly-once semantics.
    ///     This is used by the sink to call SendOffsetsToTransaction.
    /// </summary>
    public IConsumerGroupMetadata? ConsumerGroupMetadata { get; }

    /// <summary>
    ///     Gets the deserialized message body.
    /// </summary>
    public T Body { get; }

    object IAcknowledgableMessage.Body => Body!;

    /// <summary>
    ///     Gets a unique identifier for the message (topic-partition-offset format).
    /// </summary>
    public string MessageId => $"{Topic}-{Partition}-{Offset}";

    /// <summary>
    ///     Gets a value indicating whether this message has been acknowledged.
    /// </summary>
    public bool IsAcknowledged => _isAcknowledged;

    /// <summary>
    ///     Gets metadata associated with the message.
    /// </summary>
    public IReadOnlyDictionary<string, object> Metadata => _metadata;

    /// <summary>
    ///     Acknowledges the message by committing its offset.
    ///     This method is idempotent - calling it multiple times has no effect.
    ///     For exactly-once semantics, this is a no-op as offsets are committed via SendOffsetsToTransaction.
    /// </summary>
    public async Task AcknowledgeAsync(CancellationToken cancellationToken = default)
    {
        // For exactly-once semantics, acknowledgment is handled by SendOffsetsToTransaction in the sink
        if (_acknowledgeCallback == null)
        {
            lock (_ackLock)
            {
                _isAcknowledged = true;
            }

            return;
        }

        Task ackTask;

        lock (_ackLock)
        {
            if (_isAcknowledged)
                return;

            _ackTask ??= _acknowledgeCallback(cancellationToken);
            ackTask = _ackTask;
        }

        try
        {
            await ackTask.ConfigureAwait(false);

            lock (_ackLock)
            {
                _isAcknowledged = true;
                _ackTask = Task.CompletedTask;
            }
        }
        catch
        {
            lock (_ackLock)
            {
                if (ReferenceEquals(_ackTask, ackTask))
                    _ackTask = null;
            }

            throw;
        }
    }

    /// <summary>
    ///     Creates a new KafkaMessage with the provided body while preserving acknowledgment behavior.
    /// </summary>
    /// <typeparam name="TNew">The new body type.</typeparam>
    /// <param name="body">The new message body.</param>
    /// <returns>A new KafkaMessage with the same acknowledgment callback.</returns>
    public IAcknowledgableMessage<TNew> WithBody<TNew>(TNew body)
    {
        return new KafkaMessage<TNew>(
            body,
            Topic,
            Partition,
            Offset,
            Key,
            Timestamp,
            Headers,
            _acknowledgeCallback,
            ConsumerGroupMetadata);
    }

    // IKafkaMessageMetadata implementation

    /// <inheritdoc />
    public string Topic { get; }

    /// <inheritdoc />
    public int Partition { get; }

    /// <inheritdoc />
    public long Offset { get; }

    /// <inheritdoc />
    public string Key { get; }

    /// <inheritdoc />
    public DateTime Timestamp { get; }

    /// <inheritdoc />
    public Headers Headers { get; }

    /// <summary>
    ///     Marks the message as acknowledged without invoking the callback.
    ///     Used internally when batch acknowledgment is handled externally.
    /// </summary>
    internal void MarkAcknowledged()
    {
        lock (_ackLock)
        {
            _isAcknowledged = true;
            _ackTask = Task.CompletedTask;
        }
    }

    private Dictionary<string, object> BuildMetadata()
    {
        var metadata = new Dictionary<string, object>
        {
            ["Topic"] = Topic,
            ["Partition"] = Partition,
            ["Offset"] = Offset,
            ["Key"] = Key,
            ["Timestamp"] = Timestamp,
        };

        // Add headers to metadata
        foreach (var header in Headers)
        {
            var value = header.GetValueBytes();

            if (value is { Length: > 0 })
            {
                // Try to decode as string, otherwise store as base64
                try
                {
                    var stringValue = Encoding.UTF8.GetString(value);
                    metadata[$"Header.{header.Key}"] = stringValue;
                }
                catch
                {
                    metadata[$"Header.{header.Key}"] = Convert.ToBase64String(value);
                }
            }
        }

        return metadata;
    }
}

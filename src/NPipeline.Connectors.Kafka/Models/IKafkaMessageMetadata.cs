using Confluent.Kafka;

namespace NPipeline.Connectors.Kafka.Models;

/// <summary>
///     Provides Kafka-specific metadata for messages.
/// </summary>
public interface IKafkaMessageMetadata
{
    /// <summary>
    ///     Gets the topic the message was consumed from or will be produced to.
    /// </summary>
    string Topic { get; }

    /// <summary>
    ///     Gets the partition number.
    /// </summary>
    int Partition { get; }

    /// <summary>
    ///     Gets the offset within the partition.
    /// </summary>
    long Offset { get; }

    /// <summary>
    ///     Gets the message key.
    /// </summary>
    string Key { get; }

    /// <summary>
    ///     Gets the message timestamp.
    /// </summary>
    DateTime Timestamp { get; }

    /// <summary>
    ///     Gets the message headers.
    /// </summary>
    Headers Headers { get; }
}

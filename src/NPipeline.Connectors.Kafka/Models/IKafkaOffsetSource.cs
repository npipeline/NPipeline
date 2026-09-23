using Confluent.Kafka;

namespace NPipeline.Connectors.Kafka.Models;

/// <summary>
///     A consumed message's position, whatever its body type. Lets the transactional sink collect offsets from
///     <see cref="KafkaMessage{T}" /> items without knowing <c>T</c>.
/// </summary>
internal interface IKafkaOffsetSource
{
    TopicPartitionOffset TopicPartitionOffset { get; }

    IConsumerGroupMetadata? ConsumerGroupMetadata { get; }
}

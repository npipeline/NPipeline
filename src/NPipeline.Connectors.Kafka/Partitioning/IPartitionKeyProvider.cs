namespace NPipeline.Connectors.Kafka.Partitioning;

/// <summary>
///     Provides partition keys for messages to enable custom partitioning strategies.
/// </summary>
/// <typeparam name="T">The message type.</typeparam>
public interface IPartitionKeyProvider<T>
{
    /// <summary>
    ///     Gets the partition key for a message.
    /// </summary>
    /// <param name="message">The message to get the partition key for.</param>
    /// <returns>The partition key string.</returns>
    string GetPartitionKey(T message);

    /// <summary>
    ///     Optionally determines the target partition directly.
    ///     Return null to use the default partitioner with the key from <see cref="GetPartitionKey" />.
    /// </summary>
    /// <param name="message">The message to get the partition for.</param>
    /// <param name="partitionCount">The total number of partitions for the topic.</param>
    /// <returns>The target partition number, or null to use default partitioning.</returns>
    int? GetPartition(T message, int partitionCount)
    {
        return null;
    }
}

namespace NPipeline.Connectors.Kafka.Metrics;

/// <summary>
///     Interface for recording Kafka connector metrics.
///     Implementations can integrate with various monitoring systems (Prometheus, Datadog, etc.).
/// </summary>
public interface IKafkaMetrics
{
    // Producer metrics
    /// <summary>
    ///     Records the number of messages produced to a topic.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="count">The number of messages produced.</param>
    void RecordProduced(string topic, int count);

    /// <summary>
    ///     Records the latency of a produce operation.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="latency">The latency of the operation.</param>
    void RecordProduceLatency(string topic, TimeSpan latency);

    /// <summary>
    ///     Records a produce error.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="ex">The exception that occurred.</param>
    void RecordProduceError(string topic, Exception ex);

    /// <summary>
    ///     Records the size of a batch sent to a topic.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="size">The batch size in bytes.</param>
    void RecordBatchSize(string topic, int size);

    // Consumer metrics
    /// <summary>
    ///     Records the number of messages consumed from a topic.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="count">The number of messages consumed.</param>
    void RecordConsumed(string topic, int count);

    /// <summary>
    ///     Records the latency of a poll operation.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="latency">The latency of the poll operation.</param>
    void RecordPollLatency(string topic, TimeSpan latency);

    /// <summary>
    ///     Records the latency of a commit operation.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="latency">The latency of the commit operation.</param>
    void RecordCommitLatency(string topic, TimeSpan latency);

    /// <summary>
    ///     Records a commit error.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="ex">The exception that occurred.</param>
    void RecordCommitError(string topic, Exception ex);

    /// <summary>
    ///     Records the consumer lag for a partition.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="partition">The partition number.</param>
    /// <param name="lag">The lag in number of messages.</param>
    void RecordLag(string topic, int partition, long lag);

    // Serialization metrics
    /// <summary>
    ///     Records the latency of a serialization operation.
    /// </summary>
    /// <param name="type">The type being serialized.</param>
    /// <param name="latency">The latency of the operation.</param>
    void RecordSerializeLatency(Type type, TimeSpan latency);

    /// <summary>
    ///     Records the latency of a deserialization operation.
    /// </summary>
    /// <param name="type">The type being deserialized.</param>
    /// <param name="latency">The latency of the operation.</param>
    void RecordDeserializeLatency(Type type, TimeSpan latency);

    /// <summary>
    ///     Records a serialization error.
    /// </summary>
    /// <param name="type">The type being serialized or deserialized.</param>
    /// <param name="ex">The exception that occurred.</param>
    void RecordSerializeError(Type type, Exception ex);

    /// <summary>
    ///     Records a deserialization error.
    /// </summary>
    /// <param name="type">The type being deserialized.</param>
    /// <param name="ex">The exception that occurred.</param>
    void RecordDeserializeError(Type type, Exception ex);

    // Transaction metrics
    /// <summary>
    ///     Records the latency of a transaction commit.
    /// </summary>
    /// <param name="latency">The latency of the commit operation.</param>
    void RecordTransactionCommit(TimeSpan latency);

    /// <summary>
    ///     Records the latency of a transaction abort.
    /// </summary>
    /// <param name="latency">The latency of the abort operation.</param>
    void RecordTransactionAbort(TimeSpan latency);

    /// <summary>
    ///     Records a transaction error.
    /// </summary>
    /// <param name="ex">The exception that occurred.</param>
    void RecordTransactionError(Exception ex);
}

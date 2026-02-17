namespace NPipeline.Connectors.Kafka.Metrics;

/// <summary>
///     A no-op implementation of <see cref="IKafkaMetrics" /> that discards all metrics.
///     Use this when metrics collection is not needed.
/// </summary>
public sealed class NullKafkaMetrics : IKafkaMetrics
{
    private NullKafkaMetrics()
    {
    }

    /// <summary>
    ///     Gets the singleton instance of <see cref="NullKafkaMetrics" />.
    /// </summary>
    public static NullKafkaMetrics Instance { get; } = new();

    /// <inheritdoc />
    public void RecordProduced(string topic, int count)
    {
    }

    /// <inheritdoc />
    public void RecordProduceLatency(string topic, TimeSpan latency)
    {
    }

    /// <inheritdoc />
    public void RecordProduceError(string topic, Exception ex)
    {
    }

    /// <inheritdoc />
    public void RecordBatchSize(string topic, int size)
    {
    }

    /// <inheritdoc />
    public void RecordConsumed(string topic, int count)
    {
    }

    /// <inheritdoc />
    public void RecordPollLatency(string topic, TimeSpan latency)
    {
    }

    /// <inheritdoc />
    public void RecordCommitLatency(string topic, TimeSpan latency)
    {
    }

    /// <inheritdoc />
    public void RecordCommitError(string topic, Exception ex)
    {
    }

    /// <inheritdoc />
    public void RecordLag(string topic, int partition, long lag)
    {
    }

    /// <inheritdoc />
    public void RecordSerializeLatency(Type type, TimeSpan latency)
    {
    }

    /// <inheritdoc />
    public void RecordDeserializeLatency(Type type, TimeSpan latency)
    {
    }

    /// <inheritdoc />
    public void RecordSerializeError(Type type, Exception ex)
    {
    }

    /// <inheritdoc />
    public void RecordDeserializeError(Type type, Exception ex)
    {
    }

    /// <inheritdoc />
    public void RecordTransactionCommit(TimeSpan latency)
    {
    }

    /// <inheritdoc />
    public void RecordTransactionAbort(TimeSpan latency)
    {
    }

    /// <inheritdoc />
    public void RecordTransactionError(Exception ex)
    {
    }
}

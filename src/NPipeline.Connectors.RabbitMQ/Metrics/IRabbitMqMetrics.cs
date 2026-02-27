namespace NPipeline.Connectors.RabbitMQ.Metrics;

/// <summary>
///     Interface for recording RabbitMQ connector metrics.
/// </summary>
public interface IRabbitMqMetrics
{
    // Source metrics

    /// <summary>Records the number of messages consumed from a queue.</summary>
    void RecordConsumed(string queue, int count);

    /// <summary>Records consume latency in milliseconds.</summary>
    void RecordConsumeLatency(string queue, double milliseconds);

    /// <summary>Records a deserialization error.</summary>
    void RecordDeserializationError(string queue);

    /// <summary>Records successfully acknowledged messages.</summary>
    void RecordAck(string queue, int count);

    /// <summary>Records negatively acknowledged messages.</summary>
    void RecordNack(string queue, int count, bool requeued);

    /// <summary>Records current internal buffer (Channel&lt;T&gt;) size.</summary>
    void RecordConsumerBufferSize(string queue, int count);

    // Sink metrics

    /// <summary>Records the number of messages published.</summary>
    void RecordPublished(string exchange, string routingKey, int count);

    /// <summary>Records publish latency in milliseconds.</summary>
    void RecordPublishLatency(string exchange, double milliseconds);

    /// <summary>Records a publish error.</summary>
    void RecordPublishError(string exchange, string routingKey);

    /// <summary>Records publisher confirm latency in milliseconds.</summary>
    void RecordConfirmLatency(string exchange, double milliseconds);

    /// <summary>Records a batch publish operation.</summary>
    void RecordBatchPublished(string exchange, int batchSize);

    /// <summary>Records an unroutable (returned) message.</summary>
    void RecordReturned(string exchange, string routingKey);

    // Connection metrics

    /// <summary>Records a connection recovery event.</summary>
    void RecordConnectionRecovery();

    /// <summary>Records a channel creation event.</summary>
    void RecordChannelCreated();

    /// <summary>Records a channel closure event.</summary>
    void RecordChannelClosed();
}

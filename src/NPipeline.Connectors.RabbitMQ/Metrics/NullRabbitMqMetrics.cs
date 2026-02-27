namespace NPipeline.Connectors.RabbitMQ.Metrics;

/// <summary>
///     No-op implementation of <see cref="IRabbitMqMetrics" /> that discards all metrics.
///     Used as the default when no metrics implementation is registered.
/// </summary>
public sealed class NullRabbitMqMetrics : IRabbitMqMetrics
{
    private NullRabbitMqMetrics()
    {
    }

    /// <summary>Gets the singleton instance.</summary>
    public static NullRabbitMqMetrics Instance { get; } = new();

    /// <inheritdoc />
    public void RecordConsumed(string queue, int count)
    {
    }

    /// <inheritdoc />
    public void RecordConsumeLatency(string queue, double milliseconds)
    {
    }

    /// <inheritdoc />
    public void RecordDeserializationError(string queue)
    {
    }

    /// <inheritdoc />
    public void RecordAck(string queue, int count)
    {
    }

    /// <inheritdoc />
    public void RecordNack(string queue, int count, bool requeued)
    {
    }

    /// <inheritdoc />
    public void RecordConsumerBufferSize(string queue, int count)
    {
    }

    /// <inheritdoc />
    public void RecordPublished(string exchange, string routingKey, int count)
    {
    }

    /// <inheritdoc />
    public void RecordPublishLatency(string exchange, double milliseconds)
    {
    }

    /// <inheritdoc />
    public void RecordPublishError(string exchange, string routingKey)
    {
    }

    /// <inheritdoc />
    public void RecordConfirmLatency(string exchange, double milliseconds)
    {
    }

    /// <inheritdoc />
    public void RecordBatchPublished(string exchange, int batchSize)
    {
    }

    /// <inheritdoc />
    public void RecordReturned(string exchange, string routingKey)
    {
    }

    /// <inheritdoc />
    public void RecordConnectionRecovery()
    {
    }

    /// <inheritdoc />
    public void RecordChannelCreated()
    {
    }

    /// <inheritdoc />
    public void RecordChannelClosed()
    {
    }
}

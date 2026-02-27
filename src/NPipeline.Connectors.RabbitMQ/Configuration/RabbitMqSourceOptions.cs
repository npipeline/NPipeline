using NPipeline.Connectors.Configuration;

namespace NPipeline.Connectors.RabbitMQ.Configuration;

/// <summary>
///     Consumer-specific settings for <see cref="Nodes.RabbitMqSourceNode{T}" />.
/// </summary>
public sealed record RabbitMqSourceOptions
{
    /// <summary>
    ///     Gets or sets the queue name to consume from. Required.
    /// </summary>
    public required string QueueName { get; init; }

    /// <summary>
    ///     Gets or sets the QoS prefetch count. Default is 100.
    /// </summary>
    public ushort PrefetchCount { get; init; } = 100;

    /// <summary>
    ///     Gets or sets whether prefetch is global (per-channel) instead of per-consumer. Default is false.
    /// </summary>
    public bool PrefetchGlobal { get; init; }

    /// <summary>
    ///     Gets or sets the acknowledgment strategy. Default is <see cref="Connectors.Configuration.AcknowledgmentStrategy.AutoOnSinkSuccess" />.
    /// </summary>
    public AcknowledgmentStrategy AcknowledgmentStrategy { get; init; } = AcknowledgmentStrategy.AutoOnSinkSuccess;

    /// <summary>
    ///     Gets or sets whether to requeue nack'd messages. Default is true.
    /// </summary>
    public bool RequeueOnNack { get; init; } = true;

    /// <summary>
    ///     Gets or sets a custom consumer tag. Null lets the broker generate one.
    /// </summary>
    public string? ConsumerTag { get; init; }

    /// <summary>
    ///     Gets or sets whether the consumer is exclusive. Default is false.
    /// </summary>
    public bool Exclusive { get; init; }

    /// <summary>
    ///     Gets or sets the number of concurrent dispatch callbacks. Default is 1 (preserves ordering).
    /// </summary>
    public int ConsumerDispatchConcurrency { get; init; } = 1;

    /// <summary>
    ///     Gets or sets the internal <see cref="System.Threading.Channels.Channel{T}" /> buffer capacity
    ///     for backpressure. Default is 1000.
    /// </summary>
    public int InternalBufferCapacity { get; init; } = 1000;

    /// <summary>
    ///     Gets or sets optional topology declaration options.
    /// </summary>
    public RabbitMqTopologyOptions? Topology { get; init; }

    /// <summary>
    ///     Gets or sets the maximum number of retries for transient errors. Default is 3.
    /// </summary>
    public int MaxRetries { get; init; } = 3;

    /// <summary>
    ///     Gets or sets the base delay for retry backoff in milliseconds. Default is 100.
    /// </summary>
    public int RetryBaseDelayMs { get; init; } = 100;

    /// <summary>
    ///     Gets or sets whether to continue consuming after a deserialization error. Default is false.
    /// </summary>
    public bool ContinueOnDeserializationError { get; init; }

    /// <summary>
    ///     Gets or sets the maximum delivery attempts before rejecting without requeue.
    ///     Uses x-death count metadata where available. Default is 5.
    /// </summary>
    public int? MaxDeliveryAttempts { get; init; } = 5;

    /// <summary>
    ///     Gets or sets whether to reject messages that exceed <see cref="MaxDeliveryAttempts" />.
    ///     Default is true.
    /// </summary>
    public bool RejectOnMaxDeliveryAttempts { get; init; } = true;

    /// <summary>
    ///     Gets or sets the timeout for draining buffered messages during shutdown.
    ///     Default is 30 seconds.
    /// </summary>
    public TimeSpan ShutdownDrainTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    ///     Validates the source options and throws if invalid.
    /// </summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(QueueName))
            throw new InvalidOperationException("QueueName must not be empty.");

        if (PrefetchCount < 1)
            throw new InvalidOperationException("PrefetchCount must be at least 1.");

        if (InternalBufferCapacity < 1)
            throw new InvalidOperationException("InternalBufferCapacity must be at least 1.");

        if (ConsumerDispatchConcurrency < 1)
            throw new InvalidOperationException("ConsumerDispatchConcurrency must be at least 1.");

        if (MaxRetries < 0)
            throw new InvalidOperationException("MaxRetries must be non-negative.");

        if (RetryBaseDelayMs < 0)
            throw new InvalidOperationException("RetryBaseDelayMs must be non-negative.");

        if (MaxDeliveryAttempts is < 1)
            throw new InvalidOperationException("MaxDeliveryAttempts must be at least 1 when set.");

        if (ShutdownDrainTimeout < TimeSpan.Zero)
            throw new InvalidOperationException("ShutdownDrainTimeout must be non-negative.");
    }
}

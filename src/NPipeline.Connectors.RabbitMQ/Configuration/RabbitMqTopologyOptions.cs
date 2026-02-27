namespace NPipeline.Connectors.RabbitMQ.Configuration;

/// <summary>
///     Topology declaration options for exchanges, queues, and bindings.
/// </summary>
public sealed record RabbitMqTopologyOptions
{
    /// <summary>
    ///     Gets or sets whether to automatically declare topology on initialization. Default is true.
    /// </summary>
    public bool AutoDeclare { get; init; } = true;

    /// <summary>
    ///     Gets or sets the queue type. Default is <see cref="Configuration.QueueType.Quorum" /> for durability.
    /// </summary>
    public QueueType QueueType { get; init; } = QueueType.Quorum;

    /// <summary>
    ///     Gets or sets whether the queue/exchange is durable. Default is true.
    /// </summary>
    public bool Durable { get; init; } = true;

    /// <summary>
    ///     Gets or sets whether the queue/exchange is auto-deleted when no longer used. Default is false.
    /// </summary>
    public bool AutoDelete { get; init; }

    /// <summary>
    ///     Gets or sets whether the queue is exclusive to the declaring connection. Default is false.
    /// </summary>
    public bool Exclusive { get; init; }

    /// <summary>
    ///     Gets or sets the exchange type (direct, fanout, topic, headers). Null means no exchange declaration.
    /// </summary>
    public string? ExchangeType { get; init; }

    /// <summary>
    ///     Gets or sets the dead-letter exchange name. Null disables DLX on the queue.
    /// </summary>
    public string? DeadLetterExchange { get; init; }

    /// <summary>
    ///     Gets or sets the dead-letter routing key.
    /// </summary>
    public string? DeadLetterRoutingKey { get; init; }

    /// <summary>
    ///     Gets or sets the per-queue message TTL in milliseconds.
    /// </summary>
    public int? MessageTtlMs { get; init; }

    /// <summary>
    ///     Gets or sets the maximum queue depth (number of messages).
    /// </summary>
    public int? MaxLength { get; init; }

    /// <summary>
    ///     Gets or sets the maximum queue size in bytes.
    /// </summary>
    public int? MaxLengthBytes { get; init; }

    /// <summary>
    ///     Gets or sets extra queue/exchange arguments.
    /// </summary>
    public IDictionary<string, object>? ExtraArguments { get; init; }

    /// <summary>
    ///     Gets or sets the bindings to declare.
    /// </summary>
    public IReadOnlyList<BindingOptions>? Bindings { get; init; }

    /// <summary>
    ///     Gets or sets whether to use passive declare (validates existing topology without creating).
    ///     Default is false.
    /// </summary>
    public bool PassiveDeclare { get; init; }
}

/// <summary>
///     Binding declaration between an exchange and a queue.
/// </summary>
/// <param name="Exchange">The exchange name to bind to.</param>
/// <param name="RoutingKey">The routing key for the binding.</param>
/// <param name="Arguments">Optional binding arguments.</param>
public sealed record BindingOptions(string Exchange, string RoutingKey, IDictionary<string, object?>? Arguments = null);

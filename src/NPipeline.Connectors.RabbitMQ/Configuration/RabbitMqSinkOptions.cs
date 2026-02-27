namespace NPipeline.Connectors.RabbitMQ.Configuration;

/// <summary>
///     Publisher-specific settings for <see cref="Nodes.RabbitMqSinkNode{T}" />.
/// </summary>
public sealed record RabbitMqSinkOptions
{
    /// <summary>
    ///     Gets or sets the exchange name to publish to. Use "" for the default exchange. Required.
    /// </summary>
    public required string ExchangeName { get; init; }

    /// <summary>
    ///     Gets or sets the default routing key. Default is "".
    /// </summary>
    public string RoutingKey { get; init; } = "";

    /// <summary>
    ///     Gets or sets a dynamic routing key selector. When set, takes precedence over <see cref="RoutingKey" />.
    /// </summary>
    public Func<object, string>? RoutingKeySelector { get; init; }

    /// <summary>
    ///     Gets or sets whether to return unroutable messages. Default is false.
    /// </summary>
    public bool Mandatory { get; init; }

    /// <summary>
    ///     Gets or sets whether publisher confirms are enabled. Default is true.
    /// </summary>
    public bool EnablePublisherConfirms { get; init; } = true;

    /// <summary>
    ///     Gets or sets whether messages should be persisted to disk. Default is true.
    /// </summary>
    public bool Persistent { get; init; } = true;

    /// <summary>
    ///     Gets or sets the content type header. Auto-set from serializer when null.
    /// </summary>
    public string? ContentType { get; init; }

    /// <summary>
    ///     Gets or sets the application ID header.
    /// </summary>
    public string? AppId { get; init; }

    /// <summary>
    ///     Gets or sets batch publishing options. Null disables batching.
    /// </summary>
    public BatchPublishOptions? Batching { get; init; }

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
    ///     Gets or sets whether to continue past publish errors. Default is false.
    /// </summary>
    public bool ContinueOnError { get; init; }

    /// <summary>
    ///     Gets or sets the timeout for waiting for publisher confirms. Default is 5 seconds.
    /// </summary>
    public TimeSpan ConfirmTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    ///     Gets or sets the timeout for flushing remaining messages during shutdown.
    ///     Default is 30 seconds.
    /// </summary>
    public TimeSpan ShutdownFlushTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    ///     Validates the sink options and throws if invalid.
    /// </summary>
    public void Validate()
    {
        if (ExchangeName is null)
            throw new InvalidOperationException("ExchangeName must not be null. Use \"\" for the default exchange.");

        if (ConfirmTimeout <= TimeSpan.Zero)
            throw new InvalidOperationException("ConfirmTimeout must be positive.");

        if (MaxRetries < 0)
            throw new InvalidOperationException("MaxRetries must be non-negative.");

        if (RetryBaseDelayMs < 0)
            throw new InvalidOperationException("RetryBaseDelayMs must be non-negative.");

        Batching?.Validate();
    }
}

/// <summary>
///     Settings for batch publishing.
/// </summary>
public sealed record BatchPublishOptions
{
    /// <summary>
    ///     Gets or sets the number of messages per batch. Default is 100.
    /// </summary>
    public int BatchSize { get; init; } = 100;

    /// <summary>
    ///     Gets or sets the maximum time to accumulate messages before flushing. Default is 50ms.
    /// </summary>
    public TimeSpan LingerTime { get; init; } = TimeSpan.FromMilliseconds(50);

    /// <summary>
    ///     Validates the batch publish options and throws if invalid.
    /// </summary>
    public void Validate()
    {
        if (BatchSize < 1)
            throw new InvalidOperationException("BatchSize must be at least 1.");

        if (LingerTime < TimeSpan.Zero)
            throw new InvalidOperationException("LingerTime must be non-negative.");
    }
}

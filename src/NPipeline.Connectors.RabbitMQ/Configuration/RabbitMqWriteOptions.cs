using NPipeline.Connectors.Messaging;
using NPipeline.Connectors.RabbitMQ.Connection;
using NPipeline.Connectors.RabbitMQ.Reliability;
using NResilience;

namespace NPipeline.Connectors.RabbitMQ.Configuration;

/// <summary>How a RabbitMQ sink publishes. Create one with <see cref="RabbitMqConnector.Sink{T}" />.</summary>
/// <typeparam name="T">The body type.</typeparam>
public sealed record RabbitMqWriteOptions<T>
{
    /// <summary>The exchange; <c>""</c> is the default exchange, which routes to the queue named by the routing key.</summary>
    public required string Exchange { get; init; }

    /// <summary>The routing key, used unless <see cref="RoutingKeySelector" /> is set.</summary>
    public string RoutingKey { get; init; } = string.Empty;

    /// <summary>Chooses each message's routing key from its body.</summary>
    public Func<T, string>? RoutingKeySelector { get; init; }

    /// <summary>The connection. The factories set it; nodes built from DI share the registered one.</summary>
    public required IRabbitMqConnectionManager Connection { get; init; }

    /// <summary>
    ///     Whether each publish waits for the broker's confirm, so a message is only settled once the broker has it.
    ///     Defaults to <c>true</c>. Without confirms, a message is settled once it is written to the connection.
    /// </summary>
    public bool PublisherConfirms { get; init; } = true;

    /// <summary>How long to wait for a confirm before the publish counts as failed (and is retried). Defaults to 5 seconds.</summary>
    public TimeSpan ConfirmTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>Whether messages are persistent (delivery mode 2). Defaults to <c>true</c>.</summary>
    public bool Persistent { get; init; } = true;

    /// <summary>Whether the broker returns a message no queue is bound for, which then fails its publish. Defaults to <c>false</c>.</summary>
    public bool Mandatory { get; init; }

    /// <summary>The content type; <c>null</c> uses the serializer's.</summary>
    public string? ContentType { get; init; }

    /// <summary>The application id set on every message.</summary>
    public string? AppId { get; init; }

    /// <summary>
    ///     Messages published together, whose confirms are awaited together: a batch costs about one round trip, not one per
    ///     message. Defaults to 100.
    /// </summary>
    public int BatchSize { get; init; } = 100;

    /// <summary>The longest a batch waits to fill, measured from its first message. Defaults to 10 ms.</summary>
    public TimeSpan BatchLinger { get; init; } = TimeSpan.FromMilliseconds(10);

    /// <summary>What happens to a message that could not be published after retries. Defaults to <see cref="FailedMessageAction.Fail" />.</summary>
    public FailedMessageAction FailedMessages { get; init; } = FailedMessageAction.Fail;

    /// <summary>
    ///     Whether a message received from RabbitMQ keeps its headers, correlation id and message id when it is published
    ///     again. Defaults to <c>true</c>.
    /// </summary>
    public bool CopyMessageProperties { get; init; } = true;

    /// <summary>Declares the exchange before publishing; <c>null</c> declares nothing.</summary>
    public RabbitMqTopologyOptions? Topology { get; init; }

    /// <summary>The body serializer. Defaults to <see cref="JsonMessageSerializer.Default" />.</summary>
    public IMessageSerializer Serializer { get; init; } = JsonMessageSerializer.Default;

    /// <summary>How a failed publish is retried. Defaults to <see cref="RabbitMqConnectorResilience.Default" />.</summary>
    public Resilience Resilience { get; init; } = RabbitMqConnectorResilience.Default;

    /// <summary>Throws when a setting is out of range.</summary>
    public void Validate()
    {
        ArgumentNullException.ThrowIfNull(Exchange, nameof(Exchange));
        ArgumentNullException.ThrowIfNull(RoutingKey, nameof(RoutingKey));
        ArgumentNullException.ThrowIfNull(Connection, nameof(Connection));
        ArgumentNullException.ThrowIfNull(Serializer, nameof(Serializer));
        ArgumentOutOfRangeException.ThrowIfLessThan(BatchSize, 1, nameof(BatchSize));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(ConfirmTimeout, TimeSpan.Zero, nameof(ConfirmTimeout));

        if (BatchLinger < TimeSpan.Zero && BatchLinger != Timeout.InfiniteTimeSpan)
            throw new ArgumentOutOfRangeException(nameof(BatchLinger), "BatchLinger must be zero or more, or Timeout.InfiniteTimeSpan.");

        ArgumentNullException.ThrowIfNull(Resilience, nameof(Resilience));
        Resilience.Validate();
    }
}

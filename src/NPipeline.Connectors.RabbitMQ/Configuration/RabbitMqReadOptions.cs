using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Files;
using NPipeline.Connectors.Messaging;
using NPipeline.Connectors.RabbitMQ.Connection;

namespace NPipeline.Connectors.RabbitMQ.Configuration;

/// <summary>How a RabbitMQ source consumes a queue. Create one with <see cref="RabbitMqConnector.Source{T}" />.</summary>
public sealed record RabbitMqReadOptions
{
    /// <summary>The queue to consume.</summary>
    public required string Queue { get; init; }

    /// <summary>The connection. The factories set it; nodes built from DI share the registered one.</summary>
    public required IRabbitMqConnectionManager Connection { get; init; }

    /// <summary>
    ///     How many unacknowledged messages the broker delivers ahead. It bounds the messages in flight: once this many are
    ///     neither acknowledged nor rejected, the broker waits. Defaults to 100.
    /// </summary>
    public ushort PrefetchCount { get; init; } = 100;

    /// <summary>A consumer tag; <c>null</c> lets the broker choose one.</summary>
    public string? ConsumerTag { get; init; }

    /// <summary>Whether the consumer is the queue's only one.</summary>
    public bool Exclusive { get; init; }

    /// <summary>Declares the queue, and its bindings, before consuming; <c>null</c> declares nothing.</summary>
    public RabbitMqTopologyOptions? Topology { get; init; }

    /// <summary>The body serializer. Defaults to <see cref="JsonMessageSerializer.Default" />.</summary>
    public IMessageSerializer Serializer { get; init; } = JsonMessageSerializer.Default;

    /// <summary>
    ///     Decides what happens to a message whose body does not deserialize: <c>Fail</c> (the default, when <c>null</c>)
    ///     fails the read and leaves the message to be delivered again; <c>Skip</c> rejects it without requeue, so it goes to
    ///     the queue's dead-letter exchange if it has one; <c>DeadLetter</c> sends it to the pipeline's dead-letter sink as a
    ///     <see cref="MessageFailure" /> and then rejects it.
    /// </summary>
    public RowErrorHandler? RowErrorHandler { get; init; }

    /// <summary>The most characters of a failed body kept in <see cref="RowError.RawExcerpt" />. Defaults to 256.</summary>
    public int RawExcerptLength { get; init; } = FileSourceOptions.DefaultRawExcerptLength;

    /// <summary>
    ///     Rejects, without requeue, a message delivered more than this many times, so a message that keeps failing stops
    ///     coming back. Counted from the <c>x-delivery-count</c> header, which quorum queues set, or <c>x-death</c> after
    ///     dead-letter cycles; classic queues count neither. <c>null</c> (the default) leaves it to the broker: quorum
    ///     queues have a delivery limit of their own (20 by default in RabbitMQ 4).
    /// </summary>
    public int? MaxDeliveryAttempts { get; init; }

    /// <summary>
    ///     How long the source keeps its channel open after the read ends, so a sink can still settle the messages it was
    ///     given. Messages not settled by then are delivered again. Defaults to 30 seconds.
    /// </summary>
    public TimeSpan SettleTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Throws when a setting is out of range.</summary>
    public void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(Queue, nameof(Queue));
        ArgumentNullException.ThrowIfNull(Connection, nameof(Connection));
        ArgumentNullException.ThrowIfNull(Serializer, nameof(Serializer));
        ArgumentOutOfRangeException.ThrowIfZero(PrefetchCount, nameof(PrefetchCount));
        ArgumentOutOfRangeException.ThrowIfNegative(RawExcerptLength, nameof(RawExcerptLength));

        if (MaxDeliveryAttempts is < 1)
            throw new ArgumentOutOfRangeException(nameof(MaxDeliveryAttempts), "MaxDeliveryAttempts must be at least 1 when set.");

        ArgumentOutOfRangeException.ThrowIfLessThan(SettleTimeout, TimeSpan.Zero, nameof(SettleTimeout));
    }
}

using Azure.Core;
using Azure.Messaging.ServiceBus;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Files;
using NPipeline.Connectors.Messaging;

namespace NPipeline.Connectors.Azure.ServiceBus.Configuration;

/// <summary>
///     The connection every Service Bus source and sink has: a <see cref="Client" /> to share, or a connection string, or a
///     namespace with a credential, from which the node creates its own client and disposes it.
/// </summary>
public abstract record ServiceBusNodeOptions
{
    /// <summary>A client to use, which the caller owns.</summary>
    public ServiceBusClient? Client { get; init; }

    /// <summary>A connection string, for a client the node creates.</summary>
    public string? ConnectionString { get; init; }

    /// <summary>The namespace (<c>name.servicebus.windows.net</c>), for a client the node creates with <see cref="Credential" />.</summary>
    public string? FullyQualifiedNamespace { get; init; }

    /// <summary>The credential for <see cref="FullyQualifiedNamespace" />; <c>null</c> uses <c>DefaultAzureCredential</c>.</summary>
    public TokenCredential? Credential { get; init; }

    /// <summary>How the SDK retries operations, for a client the node creates. The SDK is the only layer that retries.</summary>
    public ServiceBusRetryOptions? Retry { get; init; }

    /// <summary>The body serializer. Defaults to <see cref="JsonMessageSerializer.Default" />.</summary>
    public IMessageSerializer Serializer { get; init; } = JsonMessageSerializer.Default;

    /// <summary>Throws when the connection is missing or ambiguous.</summary>
    public virtual void Validate()
    {
        var connections = (Client is null ? 0 : 1) + (string.IsNullOrWhiteSpace(ConnectionString) ? 0 : 1) +
                          (string.IsNullOrWhiteSpace(FullyQualifiedNamespace) ? 0 : 1);

        if (connections != 1)
            throw new ArgumentException("Set exactly one of Client, ConnectionString and FullyQualifiedNamespace.", nameof(ConnectionString));

        ArgumentNullException.ThrowIfNull(Serializer, nameof(Serializer));
    }
}

/// <summary>How a Service Bus source receives. Create one with <see cref="ServiceBusConnector" />.</summary>
public sealed record ServiceBusReadOptions : ServiceBusNodeOptions
{
    /// <summary>The queue, or the topic with <see cref="Subscription" />.</summary>
    public required string Entity { get; init; }

    /// <summary>The subscription, when <see cref="Entity" /> is a topic.</summary>
    public string? Subscription { get; init; }

    /// <summary>A sub-queue to read instead, such as the dead-letter queue.</summary>
    public SubQueue SubQueue { get; init; } = SubQueue.None;

    /// <summary>
    ///     The most messages received and not yet settled. The source receives while it has room, so a sink can batch up to
    ///     this many before it settles any. Defaults to 100.
    /// </summary>
    public int MaxInFlight { get; init; } = 100;

    /// <summary>Messages the SDK fetches ahead of a receive. Defaults to 0.</summary>
    public int PrefetchCount { get; init; }

    /// <summary>How long a received message's lock is renewed while it waits to be settled. Defaults to 5 minutes.</summary>
    public TimeSpan MaxLockRenewal { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>
    ///     Decides what happens to a message whose body does not deserialize: <c>Fail</c> (the default, when <c>null</c>)
    ///     fails the read and abandons the message, so it is delivered again (and dead-lettered by the broker after its
    ///     maximum delivery count); <c>Skip</c> moves it to the dead-letter sub-queue; <c>DeadLetter</c> sends it to the
    ///     pipeline's dead-letter sink as a <see cref="MessageFailure" /> and completes it.
    /// </summary>
    public RowErrorHandler? RowErrorHandler { get; init; }

    /// <summary>The most characters of a failed body kept in <see cref="RowError.RawExcerpt" />. Defaults to 256.</summary>
    public int RawExcerptLength { get; init; } = FileSourceOptions.DefaultRawExcerptLength;

    /// <summary>For a session-enabled entity: how many sessions are received at once. Defaults to 8.</summary>
    public int MaxConcurrentSessions { get; init; } = 8;

    /// <summary>For a session-enabled entity: how long a session with no new messages is kept before the next is accepted. Defaults to 5 seconds.</summary>
    public TimeSpan SessionIdleTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    ///     How long the source keeps its receivers after the read ends, so a sink can still settle the messages it was given.
    ///     Unsettled messages are abandoned then. Defaults to 30 seconds.
    /// </summary>
    public TimeSpan SettleTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <inheritdoc />
    public override void Validate()
    {
        base.Validate();
        ArgumentException.ThrowIfNullOrWhiteSpace(Entity, nameof(Entity));
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxInFlight, 1, nameof(MaxInFlight));
        ArgumentOutOfRangeException.ThrowIfNegative(PrefetchCount, nameof(PrefetchCount));
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxLockRenewal, TimeSpan.Zero, nameof(MaxLockRenewal));
        ArgumentOutOfRangeException.ThrowIfNegative(RawExcerptLength, nameof(RawExcerptLength));
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxConcurrentSessions, 1, nameof(MaxConcurrentSessions));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(SessionIdleTimeout, TimeSpan.Zero, nameof(SessionIdleTimeout));
        ArgumentOutOfRangeException.ThrowIfLessThan(SettleTimeout, TimeSpan.Zero, nameof(SettleTimeout));
    }
}

/// <summary>How a Service Bus sink sends. Create one with <see cref="ServiceBusConnector.Sink{T}" />.</summary>
/// <typeparam name="T">The body type.</typeparam>
public sealed record ServiceBusWriteOptions<T> : ServiceBusNodeOptions
{
    /// <summary>The queue or topic.</summary>
    public required string Entity { get; init; }

    /// <summary>Messages per send, as many as fit a Service Bus batch. Defaults to 100.</summary>
    public int BatchSize { get; init; } = 100;

    /// <summary>The longest a batch waits to fill, measured from its first message. Defaults to 10 ms.</summary>
    public TimeSpan BatchLinger { get; init; } = TimeSpan.FromMilliseconds(10);

    /// <summary>
    ///     Whether a message received from Service Bus keeps its message id, session, correlation id, subject and properties.
    ///     Defaults to <c>true</c>, so duplicate detection and sessions work across the hop.
    /// </summary>
    public bool CopyMessageProperties { get; init; } = true;

    /// <summary>Chooses each message's id, for duplicate detection. When <c>null</c>, a received message's id is kept, else the SDK's.</summary>
    public Func<T, string>? MessageId { get; init; }

    /// <summary>Chooses each message's session, required by a session-enabled entity.</summary>
    public Func<T, string?>? SessionId { get; init; }

    /// <summary>Chooses each message's subject (label).</summary>
    public Func<T, string?>? Subject { get; init; }

    /// <summary>How long each message lives unless received; <c>null</c> uses the entity's default.</summary>
    public TimeSpan? TimeToLive { get; init; }

    /// <summary>What happens to a message that could not be sent after the SDK's retries. Defaults to <see cref="FailedMessageAction.Fail" />.</summary>
    public FailedMessageAction FailedMessages { get; init; } = FailedMessageAction.Fail;

    /// <inheritdoc />
    public override void Validate()
    {
        base.Validate();
        ArgumentException.ThrowIfNullOrWhiteSpace(Entity, nameof(Entity));
        ArgumentOutOfRangeException.ThrowIfLessThan(BatchSize, 1, nameof(BatchSize));

        if (BatchLinger < TimeSpan.Zero && BatchLinger != Timeout.InfiniteTimeSpan)
            throw new ArgumentOutOfRangeException(nameof(BatchLinger), "BatchLinger must be zero or more, or Timeout.InfiniteTimeSpan.");
    }
}

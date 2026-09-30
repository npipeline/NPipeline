using Confluent.Kafka;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Files;
using NPipeline.Connectors.Kafka.Reliability;
using NPipeline.Connectors.Messaging;
using NResilience;

namespace NPipeline.Connectors.Kafka.Configuration;

/// <summary>The client settings every Kafka source and sink has: the brokers, security, and any other librdkafka setting.</summary>
public abstract record KafkaNodeOptions
{
    /// <summary>The brokers, as <c>host:port</c> pairs separated by commas.</summary>
    public required string BootstrapServers { get; init; }

    /// <summary>The client id the brokers see in their logs and quotas.</summary>
    public string? ClientId { get; init; }

    /// <summary>The security protocol; <c>null</c> uses librdkafka's default (plaintext).</summary>
    public SecurityProtocol? SecurityProtocol { get; init; }

    /// <summary>The SASL mechanism, for the SASL protocols.</summary>
    public SaslMechanism? SaslMechanism { get; init; }

    /// <summary>The SASL user name.</summary>
    public string? SaslUsername { get; init; }

    /// <summary>The SASL password.</summary>
    public string? SaslPassword { get; init; }

    /// <summary>The body serializer. Defaults to <see cref="JsonMessageSerializer.Default" />; Avro and Protobuf use a schema registry.</summary>
    public IMessageSerializer Serializer { get; init; } = JsonMessageSerializer.Default;

    /// <summary>Any other librdkafka setting, by its name (for example <c>ssl.ca.location</c>), applied last.</summary>
    public IReadOnlyDictionary<string, string> ClientSettings { get; init; } = new Dictionary<string, string>();

    /// <summary>Throws when a setting is missing or out of range.</summary>
    public virtual void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(BootstrapServers, nameof(BootstrapServers));
        ArgumentNullException.ThrowIfNull(Serializer, nameof(Serializer));
        ArgumentNullException.ThrowIfNull(ClientSettings, nameof(ClientSettings));

        if (SecurityProtocol is Confluent.Kafka.SecurityProtocol.SaslPlaintext or Confluent.Kafka.SecurityProtocol.SaslSsl &&
            SaslMechanism is null or Confluent.Kafka.SaslMechanism.Plain or Confluent.Kafka.SaslMechanism.ScramSha256 or Confluent.Kafka.SaslMechanism.ScramSha512 &&
            (string.IsNullOrWhiteSpace(SaslUsername) || string.IsNullOrWhiteSpace(SaslPassword)))
        {
            throw new ArgumentException("SASL with a user name mechanism needs SaslUsername and SaslPassword.", nameof(SaslUsername));
        }
    }

    /// <summary>Copies these settings onto a librdkafka configuration.</summary>
    internal void Apply(ClientConfig config)
    {
        config.BootstrapServers = BootstrapServers;
        config.ClientId = ClientId;

        // Only set what was asked for: librdkafka warns about a SASL mechanism without a SASL protocol.
        if (SecurityProtocol is { } protocol)
            config.SecurityProtocol = protocol;

        if (SaslMechanism is { } mechanism)
            config.SaslMechanism = mechanism;

        if (SaslUsername is not null)
            config.SaslUsername = SaslUsername;

        if (SaslPassword is not null)
            config.SaslPassword = SaslPassword;

        foreach (var (key, value) in ClientSettings)
        {
            config.Set(key, value);
        }
    }
}

/// <summary>How a Kafka source consumes a topic. Create one with <see cref="KafkaConnector.Source{T}" />.</summary>
public sealed record KafkaReadOptions : KafkaNodeOptions
{
    /// <summary>The topic to consume.</summary>
    public required string Topic { get; init; }

    /// <summary>The consumer group. The group's committed offsets are where a new read starts.</summary>
    public required string GroupId { get; init; }

    /// <summary>A static group member id, which lets a restarted member keep its partitions without a rebalance.</summary>
    public string? GroupInstanceId { get; init; }

    /// <summary>
    ///     Where a group with no committed offset for a partition starts. Defaults to <see cref="Confluent.Kafka.AutoOffsetReset.Latest" />,
    ///     Kafka's default: only messages produced from now on. <see cref="Confluent.Kafka.AutoOffsetReset.Earliest" /> reads the
    ///     partition from its start.
    /// </summary>
    public AutoOffsetReset AutoOffsetReset { get; init; } = AutoOffsetReset.Latest;

    /// <summary>Whether transactional messages are read only once committed. <c>null</c> uses librdkafka's default, read-committed.</summary>
    public IsolationLevel? IsolationLevel { get; init; }

    /// <summary>
    ///     How often acknowledged offsets are committed. Acknowledging a message stores its offset, and the consumer commits
    ///     stored offsets on this interval and when the read ends. Defaults to 5 seconds.
    /// </summary>
    public TimeSpan CommitInterval { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    ///     Decides what happens to a message whose value does not deserialize: <c>Fail</c> (the default, when <c>null</c>)
    ///     fails the read, and a restart reads the message again; <c>Skip</c> moves past it; <c>DeadLetter</c> sends it to the
    ///     pipeline's dead-letter sink as a <see cref="MessageFailure" /> and moves past it.
    /// </summary>
    public RowErrorHandler? RowErrorHandler { get; init; }

    /// <summary>The most characters of a failed value kept in <see cref="RowError.RawExcerpt" />. Defaults to 256.</summary>
    public int RawExcerptLength { get; init; } = FileSourceOptions.DefaultRawExcerptLength;

    /// <summary>
    ///     Whether tombstones (messages with a null value, which mark a key deleted in a compacted topic) are skipped.
    ///     Defaults to <c>true</c>; with <c>false</c> they are handed on with a default body and <see cref="Models.KafkaMessage{T}.IsTombstone" /> set.
    /// </summary>
    public bool SkipTombstones { get; init; } = true;

    /// <summary>How long a poll waits for a message before polling again.</summary>
    public TimeSpan PollTimeout { get; init; } = TimeSpan.FromMilliseconds(100);

    /// <summary>How a failed poll is retried. Defaults to <see cref="KafkaConnectorResilience.Default" />.</summary>
    public Resilience Resilience { get; init; } = KafkaConnectorResilience.Default;

    /// <summary>
    ///     How long the source keeps its consumer after the read ends, so a sink can still acknowledge the messages it was
    ///     given before the offsets are committed and the consumer leaves the group. Defaults to 30 seconds.
    /// </summary>
    public TimeSpan SettleTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <inheritdoc />
    public override void Validate()
    {
        base.Validate();
        ArgumentException.ThrowIfNullOrWhiteSpace(Topic, nameof(Topic));
        ArgumentException.ThrowIfNullOrWhiteSpace(GroupId, nameof(GroupId));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(CommitInterval, TimeSpan.Zero, nameof(CommitInterval));
        ArgumentOutOfRangeException.ThrowIfNegative(RawExcerptLength, nameof(RawExcerptLength));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(PollTimeout, TimeSpan.Zero, nameof(PollTimeout));
        ArgumentOutOfRangeException.ThrowIfLessThan(SettleTimeout, TimeSpan.Zero, nameof(SettleTimeout));
        ArgumentNullException.ThrowIfNull(Resilience, nameof(Resilience));
        Resilience.Validate();
    }
}

/// <summary>How a Kafka sink produces. Create one with <see cref="KafkaConnector.Sink{T}" />.</summary>
/// <typeparam name="T">The body type.</typeparam>
public sealed record KafkaWriteOptions<T> : KafkaNodeOptions
{
    /// <summary>The topic to produce to.</summary>
    public required string Topic { get; init; }

    /// <summary>
    ///     Chooses each message's key, which decides its partition. When <c>null</c>, a message received from Kafka keeps
    ///     its key and any other message has none, so librdkafka spreads them over the partitions.
    /// </summary>
    public Func<T, string?>? KeySelector { get; init; }

    /// <summary>Whether a message received from Kafka keeps its headers. Defaults to <c>false</c>.</summary>
    public bool CopyHeaders { get; init; }

    /// <summary>How many replicas must have a message before the broker acknowledges it. Defaults to <see cref="Confluent.Kafka.Acks.All" />.</summary>
    public Acks Acks { get; init; } = Acks.All;

    /// <summary>Whether the producer is idempotent, so its own retries never duplicate a message. Defaults to <c>true</c>.</summary>
    public bool EnableIdempotence { get; init; } = true;

    /// <summary>How long librdkafka waits to fill a batch. Defaults to 5 ms.</summary>
    public TimeSpan Linger { get; init; } = TimeSpan.FromMilliseconds(5);

    /// <summary>The compression codec; <c>null</c> sends uncompressed.</summary>
    public CompressionType? Compression { get; init; }

    /// <summary>How long librdkafka keeps retrying a message before it fails. Defaults to 2 minutes.</summary>
    public TimeSpan DeliveryTimeout { get; init; } = TimeSpan.FromMinutes(2);

    /// <summary>
    ///     Messages whose deliveries are awaited together: the sink produces this many, then settles each once the broker
    ///     has it. Defaults to 1,000.
    /// </summary>
    public int BatchSize { get; init; } = 1_000;

    /// <summary>The longest a batch waits to fill, measured from its first message. Defaults to 10 ms.</summary>
    public TimeSpan BatchLinger { get; init; } = TimeSpan.FromMilliseconds(10);

    /// <summary>What happens to a message librdkafka could not deliver. Defaults to <see cref="FailedMessageAction.Fail" />.</summary>
    public FailedMessageAction FailedMessages { get; init; } = FailedMessageAction.Fail;

    /// <summary>
    ///     A transactional id turns on exactly-once writes: each batch is produced in a transaction, together with the offsets
    ///     of the Kafka messages it came from, so a batch and its source offsets commit or abort together. Each producer
    ///     instance needs its own id. <c>null</c> (the default) produces without transactions.
    /// </summary>
    public string? TransactionalId { get; init; }

    /// <summary>How long a transactional producer's setup, commit or abort may take. Defaults to 30 seconds.</summary>
    public TimeSpan TransactionTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <inheritdoc />
    public override void Validate()
    {
        base.Validate();
        ArgumentException.ThrowIfNullOrWhiteSpace(Topic, nameof(Topic));
        ArgumentOutOfRangeException.ThrowIfLessThan(BatchSize, 1, nameof(BatchSize));
        ArgumentOutOfRangeException.ThrowIfLessThan(Linger, TimeSpan.Zero, nameof(Linger));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(DeliveryTimeout, TimeSpan.Zero, nameof(DeliveryTimeout));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(TransactionTimeout, TimeSpan.Zero, nameof(TransactionTimeout));

        if (BatchLinger < TimeSpan.Zero && BatchLinger != Timeout.InfiniteTimeSpan)
            throw new ArgumentOutOfRangeException(nameof(BatchLinger), "BatchLinger must be zero or more, or Timeout.InfiniteTimeSpan.");

        if (TransactionalId is not null && FailedMessages != FailedMessageAction.Fail)
            throw new ArgumentException("A transactional sink aborts a batch that fails, so FailedMessages must be Fail.", nameof(FailedMessages));

        if (TransactionalId is not null && !EnableIdempotence)
            throw new ArgumentException("A transactional producer is idempotent.", nameof(EnableIdempotence));
    }
}

using Confluent.Kafka;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.Kafka.Reliability;
using NResilience;

namespace NPipeline.Connectors.Kafka.Configuration;

/// <summary>
///     Configuration for Kafka source and sink nodes.
/// </summary>
public sealed class KafkaConfiguration
{
    // Connection Settings
    /// <summary>
    ///     Gets or sets the Kafka bootstrap servers (comma-separated list).
    /// </summary>
    public string BootstrapServers { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the client identifier for this connection.
    /// </summary>
    public string? ClientId { get; set; }

    /// <summary>
    ///     Gets or sets the SASL username for authentication.
    /// </summary>
    public string? SaslUsername { get; set; }

    /// <summary>
    ///     Gets or sets the SASL password for authentication.
    /// </summary>
    public string? SaslPassword { get; set; }

    /// <summary>
    ///     Gets or sets the security protocol to use.
    /// </summary>
    public SecurityProtocol SecurityProtocol { get; set; } = SecurityProtocol.Plaintext;

    /// <summary>
    ///     Gets or sets the SASL mechanism to use.
    /// </summary>
    public SaslMechanism SaslMechanism { get; set; } = SaslMechanism.Plain;

    // Consumer Settings
    /// <summary>
    ///     Gets or sets the source topic to consume from.
    /// </summary>
    public string SourceTopic { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the consumer group ID.
    /// </summary>
    public string ConsumerGroupId { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the group instance ID for static group membership.
    /// </summary>
    public string? GroupInstanceId { get; set; }

    /// <summary>
    ///     Gets or sets the auto offset reset policy.
    /// </summary>
    public AutoOffsetReset AutoOffsetReset { get; set; } = AutoOffsetReset.Latest;

    /// <summary>
    ///     Gets or sets whether auto-commit is enabled.
    /// </summary>
    public bool EnableAutoCommit { get; set; }

    /// <summary>
    ///     Gets or sets whether auto offset store is enabled.
    /// </summary>
    public bool EnableAutoOffsetStore { get; set; } = true;

    /// <summary>
    ///     Gets or sets the maximum number of records to poll per batch.
    /// </summary>
    public int MaxPollRecords { get; set; } = 500;

    /// <summary>
    ///     Gets or sets the poll timeout in milliseconds.
    /// </summary>
    public int PollTimeoutMs { get; set; } = 100;

    /// <summary>
    ///     Gets or sets the minimum bytes to fetch per request.
    /// </summary>
    public int FetchMinBytes { get; set; } = 1;

    /// <summary>
    ///     Gets or sets the maximum bytes to fetch per request.
    /// </summary>
    public int FetchMaxBytes { get; set; } = 52428800;

    /// <summary>
    ///     Gets or sets the maximum bytes to fetch per partition.
    /// </summary>
    public int MaxPartitionFetchBytes { get; set; } = 1048576;

    // Producer Settings
    /// <summary>
    ///     Gets or sets the sink topic to produce to.
    /// </summary>
    public string SinkTopic { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets whether idempotent production is enabled.
    /// </summary>
    public bool EnableIdempotence { get; set; } = true;

    /// <summary>
    ///     Gets or sets the maximum number of messages per batch.
    /// </summary>
    public int BatchSize { get; set; } = 16384;

    /// <summary>
    ///     Gets or sets the linger time in milliseconds for batching.
    /// </summary>
    public int LingerMs { get; set; } = 5;

    /// <summary>
    ///     Gets or sets the batch linger time for pipeline batching.
    /// </summary>
    public int BatchLingerMs { get; set; } = 100;

    /// <summary>
    ///     Gets or sets the compression type to use.
    /// </summary>
    public CompressionType CompressionType { get; set; } = CompressionType.None;

    /// <summary>
    ///     Gets or sets the maximum message size in bytes.
    /// </summary>
    public int MessageMaxBytes { get; set; } = 1000000;

    /// <summary>
    ///     Gets or sets the acknowledgment mode for produced messages.
    /// </summary>
    public Acks Acks { get; set; } = Acks.All;

    /// <summary>
    ///     Gets or sets how long librdkafka keeps retrying a produce before it fails the message, in milliseconds
    ///     (<c>delivery.timeout.ms</c>, also called <c>message.timeout.ms</c>). Default is 300000 (five minutes), the
    ///     librdkafka default; 0 retries without limit. Must not be less than <see cref="LingerMs" />.
    /// </summary>
    /// <remarks>
    ///     librdkafka is the only layer that retries a produce, so this bounds how long a produce can take to fail.
    /// </remarks>
    public int DeliveryTimeoutMs { get; set; } = 300000;

    /// <summary>
    ///     Gets or sets the delay before librdkafka's first retry of a failed produce request, in milliseconds
    ///     (<c>retry.backoff.ms</c>). librdkafka doubles it on each retry, with jitter, up to
    ///     <see cref="RetryBackoffMaxMs" />. Default is 100, the librdkafka default.
    /// </summary>
    public int RetryBackoffMs { get; set; } = 100;

    /// <summary>
    ///     Gets or sets the longest delay between librdkafka's produce retries, in milliseconds
    ///     (<c>retry.backoff.max.ms</c>). Default is 1000, the librdkafka default.
    /// </summary>
    public int RetryBackoffMaxMs { get; set; } = 1000;

    /// <summary>
    ///     Gets or sets how long the sink waits for the sink topic's metadata when it starts, in milliseconds.
    ///     Default is 10000. If the brokers cannot be reached in that time, the sink fails with an
    ///     <see cref="InvalidOperationException" /> instead of blocking.
    /// </summary>
    public int MetadataTimeoutMs { get; set; } = 10000;

    // Serialization
    /// <summary>
    ///     Gets or sets the serialization format to use.
    /// </summary>
    public SerializationFormat SerializationFormat { get; set; } = SerializationFormat.Json;

    /// <summary>
    ///     Gets or sets the Schema Registry configuration.
    /// </summary>
    public SchemaRegistryConfiguration? SchemaRegistry { get; set; }

    // Delivery Semantics
    /// <summary>
    ///     Gets or sets the delivery semantic guarantee.
    /// </summary>
    public DeliverySemantic DeliverySemantic { get; set; } = DeliverySemantic.AtLeastOnce;

    /// <summary>
    ///     Gets or sets the acknowledgment strategy for processed messages.
    /// </summary>
    public AcknowledgmentStrategy AcknowledgmentStrategy { get; set; } = AcknowledgmentStrategy.AutoOnSinkSuccess;

    // Performance
    /// <summary>
    ///     Gets or sets the maximum degree of parallelism for processing.
    /// </summary>
    public int MaxDegreeOfParallelism { get; set; } = Environment.ProcessorCount;

    /// <summary>
    ///     Gets or sets whether parallel processing is enabled.
    /// </summary>
    public bool EnableParallelProcessing { get; set; } = true;

    /// <summary>
    ///     Gets or sets the maximum connection pool size.
    /// </summary>
    public int MaxConnectionPoolSize { get; set; } = 10;

    /// <summary>
    ///     Gets or sets the statistics reporting interval in milliseconds.
    /// </summary>
    public int StatisticsIntervalMs { get; set; }

    // Transactions
    /// <summary>
    ///     Gets or sets whether transactional production is enabled.
    /// </summary>
    public bool EnableTransactions { get; set; }

    /// <summary>
    ///     Gets or sets the transactional ID for exactly-once semantics.
    /// </summary>
    public string? TransactionalId { get; set; }

    /// <summary>
    ///     Gets or sets the transaction initialization timeout in milliseconds.
    /// </summary>
    public int TransactionInitTimeoutMs { get; set; } = 30000;

    /// <summary>
    ///     Gets or sets the isolation level for consuming messages.
    /// </summary>
    public IsolationLevel IsolationLevel { get; set; } = IsolationLevel.ReadUncommitted;

    // Error Handling
    /// <summary>
    ///     Gets or sets how the source retries a failed consume. Defaults to <see cref="KafkaConnectorResilience.Default" />:
    ///     four attempts with exponential backoff from 100 milliseconds, retrying only errors Kafka reports as retriable.
    ///     A fatal error, a deserialization error, and any other non-retriable error surface on the first attempt.
    /// </summary>
    /// <remarks>
    ///     The sink does not use this policy. librdkafka already retries every produce until
    ///     <c>delivery.timeout.ms</c>, and the idempotent producer removes the duplicates those retries would cause; a
    ///     second retry layer above it cannot be deduplicated. A produce error therefore surfaces as soon as librdkafka
    ///     gives up.
    /// </remarks>
    public Resilience Resilience { get; init; } = KafkaConnectorResilience.Default;

    /// <summary>
    ///     Gets or sets whether to continue processing on errors.
    /// </summary>
    public bool ContinueOnError { get; set; }

    /// <summary>
    ///     Validates the configuration for source operations.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when configuration is invalid.</exception>
    public void ValidateSource()
    {
        if (string.IsNullOrWhiteSpace(BootstrapServers))
            throw new InvalidOperationException("BootstrapServers is required for Kafka source.");

        if (string.IsNullOrWhiteSpace(SourceTopic))
            throw new InvalidOperationException("SourceTopic is required for Kafka source.");

        if (string.IsNullOrWhiteSpace(ConsumerGroupId))
            throw new InvalidOperationException("ConsumerGroupId is required for Kafka source.");

        if (MaxPollRecords <= 0)
            throw new InvalidOperationException("MaxPollRecords must be greater than zero.");

        if (PollTimeoutMs <= 0)
            throw new InvalidOperationException("PollTimeoutMs must be greater than zero.");

        if (FetchMinBytes < 0)
            throw new InvalidOperationException("FetchMinBytes cannot be negative.");

        if (FetchMaxBytes <= 0)
            throw new InvalidOperationException("FetchMaxBytes must be greater than zero.");

        if (MaxPartitionFetchBytes <= 0)
            throw new InvalidOperationException("MaxPartitionFetchBytes must be greater than zero.");

        ArgumentNullException.ThrowIfNull(Resilience);
        Resilience.Validate();

        ValidateSecurityCredentials();
    }

    /// <summary>
    ///     Validates the configuration for sink operations.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when configuration is invalid.</exception>
    public void ValidateSink()
    {
        if (string.IsNullOrWhiteSpace(BootstrapServers))
            throw new InvalidOperationException("BootstrapServers is required for Kafka sink.");

        if (string.IsNullOrWhiteSpace(SinkTopic))
            throw new InvalidOperationException("SinkTopic is required for Kafka sink.");

        if (BatchSize <= 0)
            throw new InvalidOperationException("BatchSize must be greater than zero.");

        if (LingerMs < 0)
            throw new InvalidOperationException("LingerMs cannot be negative.");

        if (MessageMaxBytes <= 0)
            throw new InvalidOperationException("MessageMaxBytes must be greater than zero.");

        if (DeliveryTimeoutMs < 0)
            throw new InvalidOperationException("DeliveryTimeoutMs cannot be negative.");

        if (DeliveryTimeoutMs > 0 && DeliveryTimeoutMs < LingerMs)
            throw new InvalidOperationException("DeliveryTimeoutMs must not be less than LingerMs.");

        if (RetryBackoffMs <= 0)
            throw new InvalidOperationException("RetryBackoffMs must be greater than zero.");

        if (RetryBackoffMaxMs < RetryBackoffMs)
            throw new InvalidOperationException("RetryBackoffMaxMs must not be less than RetryBackoffMs.");

        if (MetadataTimeoutMs <= 0)
            throw new InvalidOperationException("MetadataTimeoutMs must be greater than zero.");

        ValidateSecurityCredentials();
    }

    private void ValidateSecurityCredentials()
    {
        if (SecurityProtocol is not (SecurityProtocol.SaslPlaintext or SecurityProtocol.SaslSsl))
            return;

        var requiresUserPassword = SaslMechanism is SaslMechanism.Plain
            or SaslMechanism.ScramSha256
            or SaslMechanism.ScramSha512;

        if (!requiresUserPassword)
            return;

        if (string.IsNullOrWhiteSpace(SaslUsername))
            throw new InvalidOperationException("SaslUsername is required for the configured SASL mechanism.");

        if (string.IsNullOrWhiteSpace(SaslPassword))
            throw new InvalidOperationException("SaslPassword is required for the configured SASL mechanism.");
    }

    /// <summary>
    ///     Validates serialization configuration.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when configuration is invalid.</exception>
    public void ValidateSerialization()
    {
        if (SerializationFormat == SerializationFormat.Avro && SchemaRegistry == null)
        {
            throw new InvalidOperationException(
                "SchemaRegistry configuration is required for Avro serialization.");
        }

        if (SerializationFormat == SerializationFormat.Protobuf && SchemaRegistry == null)
        {
            throw new InvalidOperationException(
                "SchemaRegistry configuration is required for Protobuf serialization.");
        }

        SchemaRegistry?.Validate();
    }

    /// <summary>
    ///     Validates transaction configuration.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when configuration is invalid.</exception>
    public void ValidateTransactions()
    {
        if (EnableTransactions && string.IsNullOrWhiteSpace(TransactionalId))
        {
            throw new InvalidOperationException(
                "TransactionalId is required when EnableTransactions is true.");
        }

        if (EnableTransactions && TransactionInitTimeoutMs <= 0)
        {
            throw new InvalidOperationException(
                "TransactionInitTimeoutMs must be greater than zero when EnableTransactions is true.");
        }

        if (EnableTransactions && DeliverySemantic != DeliverySemantic.ExactlyOnce)
        {
            throw new InvalidOperationException(
                "Transactions require DeliverySemantic.ExactlyOnce.");
        }

        if (EnableTransactions && !EnableIdempotence)
        {
            throw new InvalidOperationException(
                "EnableIdempotence is required when EnableTransactions is true.");
        }
    }

    /// <summary>
    ///     Validates the entire configuration.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when configuration is invalid.</exception>
    public void Validate()
    {
        ValidateSource();
        ValidateSink();
        ValidateSerialization();
        ValidateTransactions();

        if (MaxDegreeOfParallelism <= 0)
            throw new InvalidOperationException("MaxDegreeOfParallelism must be greater than zero.");

        if (MaxConnectionPoolSize <= 0)
            throw new InvalidOperationException("MaxConnectionPoolSize must be greater than zero.");
    }
}

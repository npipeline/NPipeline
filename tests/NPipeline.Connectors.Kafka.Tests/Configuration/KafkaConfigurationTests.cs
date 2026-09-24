using Confluent.Kafka;
using NPipeline.Connectors.Kafka.Configuration;
using NPipeline.Connectors.Kafka.Reliability;
using NResilience;

namespace NPipeline.Connectors.Kafka.Tests.Configuration;

/// <summary>
///     Unit tests for <see cref="KafkaConfiguration" />.
/// </summary>
public class KafkaConfigurationTests
{
    #region Default Values Tests

    [Fact]
    public void KafkaConfiguration_ShouldHaveCorrectDefaults()
    {
        // Arrange & Act
        var config = new KafkaConfiguration();

        // Assert
        config.DeliverySemantic.Should().Be(DeliverySemantic.AtLeastOnce);
        config.EnableIdempotence.Should().BeTrue();
        config.EnableTransactions.Should().BeFalse();
        config.EnableAutoCommit.Should().BeFalse();
        config.EnableAutoOffsetStore.Should().BeTrue();
        config.BatchSize.Should().Be(16384);
        config.LingerMs.Should().Be(5);
        config.Resilience.Should().BeSameAs(KafkaConnectorResilience.Default);
        config.SerializationFormat.Should().Be(SerializationFormat.Json);
        config.AutoOffsetReset.Should().Be(AutoOffsetReset.Latest);
        config.Acks.Should().Be(Acks.All);
        config.SecurityProtocol.Should().Be(SecurityProtocol.Plaintext);
        config.IsolationLevel.Should().Be(IsolationLevel.ReadUncommitted);
        config.MaxPollRecords.Should().Be(500);
        config.PollTimeoutMs.Should().Be(100);
        config.TransactionInitTimeoutMs.Should().Be(30000);
    }

    #endregion

    #region ValidateTransactions Tests

    [Fact]
    public void ValidateTransactions_WhenTransactionsDisabled_ShouldNotThrow()
    {
        // Arrange
        var config = new KafkaConfiguration
        {
            BootstrapServers = "localhost:9092",
            EnableTransactions = false,
        };

        // Act & Assert
        var act = () => config.ValidateTransactions();
        act.Should().NotThrow();
    }

    [Fact]
    public void ValidateTransactions_WhenTransactionsEnabledWithoutTransactionalId_ShouldThrow()
    {
        // Arrange
        var config = new KafkaConfiguration
        {
            BootstrapServers = "localhost:9092",
            EnableTransactions = true,
            TransactionalId = null!,
            DeliverySemantic = DeliverySemantic.ExactlyOnce,
            EnableIdempotence = true,
        };

        // Act & Assert
        var act = () => config.ValidateTransactions();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("TransactionalId is required when EnableTransactions is true.");
    }

    [Fact]
    public void ValidateTransactions_WhenTransactionsEnabledWithEmptyTransactionalId_ShouldThrow()
    {
        // Arrange
        var config = new KafkaConfiguration
        {
            BootstrapServers = "localhost:9092",
            EnableTransactions = true,
            TransactionalId = "",
            DeliverySemantic = DeliverySemantic.ExactlyOnce,
            EnableIdempotence = true,
        };

        // Act & Assert
        var act = () => config.ValidateTransactions();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("TransactionalId is required when EnableTransactions is true.");
    }

    [Fact]
    public void ValidateTransactions_WhenTransactionsEnabledWithAtLeastOnceSemantic_ShouldThrow()
    {
        // Arrange
        var config = new KafkaConfiguration
        {
            BootstrapServers = "localhost:9092",
            EnableTransactions = true,
            TransactionalId = "my-transactional-id",
            DeliverySemantic = DeliverySemantic.AtLeastOnce,
            EnableIdempotence = true,
        };

        // Act & Assert
        var act = () => config.ValidateTransactions();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("Transactions require DeliverySemantic.ExactlyOnce.");
    }

    [Fact]
    public void ValidateTransactions_WhenTransactionsEnabledWithoutIdempotence_ShouldThrow()
    {
        // Arrange
        var config = new KafkaConfiguration
        {
            BootstrapServers = "localhost:9092",
            EnableTransactions = true,
            TransactionalId = "my-transactional-id",
            DeliverySemantic = DeliverySemantic.ExactlyOnce,
            EnableIdempotence = false,
        };

        // Act & Assert
        var act = () => config.ValidateTransactions();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("EnableIdempotence is required when EnableTransactions is true.");
    }

    [Fact]
    public void ValidateTransactions_WhenTransactionInitTimeoutInvalid_ShouldThrow()
    {
        // Arrange
        var config = new KafkaConfiguration
        {
            BootstrapServers = "localhost:9092",
            EnableTransactions = true,
            TransactionalId = "my-transactional-id",
            DeliverySemantic = DeliverySemantic.ExactlyOnce,
            EnableIdempotence = true,
            TransactionInitTimeoutMs = 0,
        };

        // Act & Assert
        var act = () => config.ValidateTransactions();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("TransactionInitTimeoutMs must be greater than zero when EnableTransactions is true.");
    }

    [Fact]
    public void ValidateTransactions_WhenAllTransactionRequirementsMet_ShouldNotThrow()
    {
        // Arrange
        var config = new KafkaConfiguration
        {
            BootstrapServers = "localhost:9092",
            EnableTransactions = true,
            TransactionalId = "my-transactional-id",
            DeliverySemantic = DeliverySemantic.ExactlyOnce,
            EnableIdempotence = true,
        };

        // Act & Assert
        var act = () => config.ValidateTransactions();
        act.Should().NotThrow();
    }

    #endregion

    #region ValidateSource Tests

    [Fact]
    public void ValidateSource_WhenValid_ShouldNotThrow()
    {
        // Arrange
        var config = new KafkaConfiguration
        {
            BootstrapServers = "localhost:9092",
            SourceTopic = "input-topic",
            ConsumerGroupId = "test-group",
        };

        // Act & Assert
        var act = () => config.ValidateSource();
        act.Should().NotThrow();
    }

    [Fact]
    public void ValidateSource_WithoutBootstrapServers_ShouldThrow()
    {
        // Arrange
        var config = new KafkaConfiguration
        {
            BootstrapServers = "",
            SourceTopic = "input-topic",
            ConsumerGroupId = "test-group",
        };

        // Act & Assert
        var act = () => config.ValidateSource();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("BootstrapServers is required for Kafka source.");
    }

    [Fact]
    public void ValidateSource_WithoutSourceTopic_ShouldThrow()
    {
        // Arrange
        var config = new KafkaConfiguration
        {
            BootstrapServers = "localhost:9092",
            SourceTopic = "",
            ConsumerGroupId = "test-group",
        };

        // Act & Assert
        var act = () => config.ValidateSource();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("SourceTopic is required for Kafka source.");
    }

    [Fact]
    public void ValidateSource_WithoutConsumerGroupId_ShouldThrow()
    {
        // Arrange
        var config = new KafkaConfiguration
        {
            BootstrapServers = "localhost:9092",
            SourceTopic = "input-topic",
            ConsumerGroupId = "",
        };

        // Act & Assert
        var act = () => config.ValidateSource();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("ConsumerGroupId is required for Kafka source.");
    }

    [Fact]
    public void ValidateSource_WithInvalidPollTimeout_ShouldThrow()
    {
        // Arrange
        var config = new KafkaConfiguration
        {
            BootstrapServers = "localhost:9092",
            SourceTopic = "input-topic",
            ConsumerGroupId = "test-group",
            PollTimeoutMs = 0,
        };

        // Act & Assert
        var act = () => config.ValidateSource();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("PollTimeoutMs must be greater than zero.");
    }

    #endregion

    #region ValidateSink Tests

    [Fact]
    public void ValidateSink_WhenValid_ShouldNotThrow()
    {
        // Arrange
        var config = new KafkaConfiguration
        {
            BootstrapServers = "localhost:9092",
            SinkTopic = "output-topic",
        };

        // Act & Assert
        var act = () => config.ValidateSink();
        act.Should().NotThrow();
    }

    [Fact]
    public void ValidateSink_WithoutBootstrapServers_ShouldThrow()
    {
        // Arrange
        var config = new KafkaConfiguration
        {
            BootstrapServers = "",
            SinkTopic = "output-topic",
        };

        // Act & Assert
        var act = () => config.ValidateSink();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("BootstrapServers is required for Kafka sink.");
    }

    [Fact]
    public void ValidateSink_WithoutSinkTopic_ShouldThrow()
    {
        // Arrange
        var config = new KafkaConfiguration
        {
            BootstrapServers = "localhost:9092",
            SinkTopic = "",
        };

        // Act & Assert
        var act = () => config.ValidateSink();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("SinkTopic is required for Kafka sink.");
    }

    #endregion

    #region ValidateSerialization Tests

    [Fact]
    public void ValidateSerialization_WithJsonFormat_ShouldNotRequireSchemaRegistry()
    {
        // Arrange
        var config = new KafkaConfiguration
        {
            SerializationFormat = SerializationFormat.Json,
        };

        // Act & Assert
        var act = () => config.ValidateSerialization();
        act.Should().NotThrow();
    }

    [Fact]
    public void ValidateSerialization_WithAvroWithoutSchemaRegistry_ShouldThrow()
    {
        // Arrange
        var config = new KafkaConfiguration
        {
            SerializationFormat = SerializationFormat.Avro,
            SchemaRegistry = null,
        };

        // Act & Assert
        var act = () => config.ValidateSerialization();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("SchemaRegistry configuration is required for Avro serialization.");
    }

    [Fact]
    public void ValidateSerialization_WithProtobufWithoutSchemaRegistry_ShouldThrow()
    {
        // Arrange
        var config = new KafkaConfiguration
        {
            SerializationFormat = SerializationFormat.Protobuf,
            SchemaRegistry = null,
        };

        // Act & Assert
        var act = () => config.ValidateSerialization();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("SchemaRegistry configuration is required for Protobuf serialization.");
    }

    #endregion

    #region Validate Tests

    [Fact]
    public void Validate_WithValidConfiguration_ShouldNotThrow()
    {
        // Arrange
        var config = new KafkaConfiguration
        {
            BootstrapServers = "localhost:9092",
            SourceTopic = "input-topic",
            SinkTopic = "output-topic",
            ConsumerGroupId = "test-group",
        };

        // Act & Assert
        var act = () => config.Validate();
        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_WithInvalidMaxDegreeOfParallelism_ShouldThrow()
    {
        // Arrange
        var config = new KafkaConfiguration
        {
            BootstrapServers = "localhost:9092",
            SourceTopic = "input-topic",
            SinkTopic = "output-topic",
            ConsumerGroupId = "test-group",
            MaxDegreeOfParallelism = 0,
        };

        // Act & Assert
        var act = () => config.Validate();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("MaxDegreeOfParallelism must be greater than zero.");
    }

    [Fact]
    public void ProducerRetrySettings_DefaultToLibrdkafkasDefaults()
    {
        var config = new KafkaConfiguration();

        config.DeliveryTimeoutMs.Should().Be(300000);
        config.RetryBackoffMs.Should().Be(100);
        config.RetryBackoffMaxMs.Should().Be(1000);
        config.MetadataTimeoutMs.Should().Be(10000);
    }

    [Theory]
    [InlineData(-1, 100, 1000, 10000, "DeliveryTimeoutMs cannot be negative.")]
    [InlineData(1, 100, 1000, 10000, "DeliveryTimeoutMs must not be less than LingerMs.")]
    [InlineData(300000, 0, 1000, 10000, "RetryBackoffMs must be greater than zero.")]
    [InlineData(300000, 500, 100, 10000, "RetryBackoffMaxMs must not be less than RetryBackoffMs.")]
    [InlineData(300000, 100, 1000, 0, "MetadataTimeoutMs must be greater than zero.")]
    public void ValidateSink_WithInvalidProducerRetrySettings_ShouldThrow(
        int deliveryTimeoutMs, int retryBackoffMs, int retryBackoffMaxMs, int metadataTimeoutMs, string message)
    {
        var config = new KafkaConfiguration
        {
            BootstrapServers = "localhost:9092",
            SinkTopic = "output-topic",
            LingerMs = 5,
            DeliveryTimeoutMs = deliveryTimeoutMs,
            RetryBackoffMs = retryBackoffMs,
            RetryBackoffMaxMs = retryBackoffMaxMs,
            MetadataTimeoutMs = metadataTimeoutMs,
        };

        var act = () => config.ValidateSink();

        act.Should().Throw<InvalidOperationException>().WithMessage(message);
    }

    [Fact]
    public void ValidateSink_WithUnlimitedDeliveryTimeout_ShouldNotThrow()
    {
        var config = new KafkaConfiguration { BootstrapServers = "localhost:9092", SinkTopic = "output-topic", DeliveryTimeoutMs = 0 };

        var act = () => config.ValidateSink();

        act.Should().NotThrow();
    }

    [Fact]
    public void ValidateSource_WithInvalidResilience_ShouldThrow()
    {
        // Arrange
        var config = new KafkaConfiguration
        {
            BootstrapServers = "localhost:9092",
            SourceTopic = "input-topic",
            ConsumerGroupId = "test-group",
#pragma warning disable NRES003 // The invalid value is the point of the test.
            Resilience = KafkaConnectorResilience.Default with { Attempts = 0 },
#pragma warning restore NRES003
        };

        // Act & Assert
        var act = () => config.ValidateSource();

        act.Should().Throw<ResilienceConfigurationException>();
    }

    #endregion
}

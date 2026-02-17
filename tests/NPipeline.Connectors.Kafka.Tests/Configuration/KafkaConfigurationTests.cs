using Confluent.Kafka;
using NPipeline.Connectors.Kafka.Configuration;

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
        config.MaxRetries.Should().Be(3);
        config.RetryBaseDelayMs.Should().Be(100);
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
    public void Validate_WithInvalidMaxRetries_ShouldThrow()
    {
        // Arrange
        var config = new KafkaConfiguration
        {
            BootstrapServers = "localhost:9092",
            SourceTopic = "input-topic",
            SinkTopic = "output-topic",
            ConsumerGroupId = "test-group",
            MaxRetries = -1,
        };

        // Act & Assert
        var act = () => config.Validate();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("MaxRetries cannot be negative.");
    }

    #endregion
}

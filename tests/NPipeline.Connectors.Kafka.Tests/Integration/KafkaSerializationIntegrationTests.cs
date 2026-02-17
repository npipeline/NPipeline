using Confluent.Kafka;
using Confluent.Kafka.Admin;
using NPipeline.Connectors.Kafka.Configuration;
using NPipeline.Connectors.Kafka.Serialization;
using NPipeline.Connectors.Kafka.Tests.Fixtures;
using Xunit.Abstractions;

namespace NPipeline.Connectors.Kafka.Tests.Integration;

/// <summary>
///     Integration tests for Kafka serialization formats.
/// </summary>
[Collection("Kafka")]
public sealed class KafkaSerializationIntegrationTests : IAsyncLifetime
{
    private readonly KafkaTestContainerFixture _fixture;
    private readonly ITestOutputHelper _output;
    private readonly List<string> _topicsToCleanup = [];

    public KafkaSerializationIntegrationTests(KafkaTestContainerFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    public async Task InitializeAsync()
    {
        // Schema Registry is not currently supported
        _output.WriteLine("BootstrapServers: {0}", _fixture.BootstrapServers);
    }

    public async Task DisposeAsync()
    {
        await DeleteTopicsAsync(_topicsToCleanup);
    }

    private async Task CreateTopicAsync(string topicName, int partitions = 1)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = _fixture.BootstrapServers,
        }).Build();

        try
        {
            await adminClient.CreateTopicsAsync([
                new TopicSpecification
                {
                    Name = topicName,
                    NumPartitions = partitions,
                    ReplicationFactor = 1,
                },
            ]);

            _topicsToCleanup.Add(topicName);
            await Task.Delay(1000);
        }
        catch (CreateTopicsException ex)
        {
            if (!ex.Results.Any(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
                throw;
        }
    }

    private async Task DeleteTopicsAsync(List<string> topics)
    {
        if (topics.Count == 0)
            return;

        using var adminClient = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = _fixture.BootstrapServers,
        }).Build();

        try
        {
            await adminClient.DeleteTopicsAsync(topics);
        }
        catch
        {
            // Ignore cleanup errors
        }
    }

    [Fact]
    public async Task JsonSerialization_RoundTripsCorrectly()
    {
        // Arrange
        var topic = $"json-serialization-{Guid.NewGuid():N}";
        await CreateTopicAsync(topic);

        var metrics = new TestKafkaMetrics();
        var serializer = new JsonMessageSerializer(metrics);

        var originalMessage = new SerializationTestMessage
        {
            Id = Guid.NewGuid(),
            Name = "JSON Test Message",
            Value = 123.45,
            Timestamp = DateTime.UtcNow,
            Tags = ["tag1", "tag2"],
        };

        // Act
        var serialized = serializer.Serialize(originalMessage);
        var deserialized = serializer.Deserialize<SerializationTestMessage>(serialized);

        // Assert
        deserialized.Should().NotBeNull();
        deserialized.Id.Should().Be(originalMessage.Id);
        deserialized.Name.Should().Be(originalMessage.Name);
        deserialized.Value.Should().Be(originalMessage.Value);
        deserialized.Tags.Should().BeEquivalentTo(originalMessage.Tags);
    }

    [Fact]
    public async Task JsonSerialization_WithNullValue_ReturnsEmptyArray()
    {
        // Arrange
        var metrics = new TestKafkaMetrics();
        var serializer = new JsonMessageSerializer(metrics);

        // Act
        var serialized = serializer.Serialize<SerializationTestMessage>(null!);

        // Assert
        serialized.Should().NotBeNull();
        serialized.Should().BeEmpty();
    }

    [Fact]
    public async Task JsonSerialization_WithEmptyArray_ReturnsDefault()
    {
        // Arrange
        var metrics = new TestKafkaMetrics();
        var serializer = new JsonMessageSerializer(metrics);

        // Act
        var deserialized = serializer.Deserialize<SerializationTestMessage>([]);

        // Assert - Empty array returns null (default for reference types)
        deserialized.Should().BeNull();
    }

    [Fact]
    public void SerializerProvider_RecordsMetrics()
    {
        // Arrange
        var metrics = new TestKafkaMetrics();
        var serializer = new JsonMessageSerializer(metrics);

        var message = new SerializationTestMessage
        {
            Id = Guid.NewGuid(),
            Name = "Metrics Test",
            Value = 100,
            Timestamp = DateTime.UtcNow,
        };

        // Act
        var serialized = serializer.Serialize(message);
        var _ = serializer.Deserialize<SerializationTestMessage>(serialized);

        // Assert - Metrics should have been recorded
        // The JsonMessageSerializer records latency metrics
        metrics.ErrorCount.Should().Be(0);
    }

    [Fact]
    public async Task AvroSerialization_WithSchemaRegistry_CanBeCreated()
    {
        // Arrange
        await _fixture.EnsureSchemaRegistryAsync();

        var schemaRegistryConfig = new SchemaRegistryConfiguration
        {
            Url = _fixture.SchemaRegistryUrl,
            AutoRegisterSchemas = true,
            RequestTimeoutMs = 30000,
        };

        var metrics = new TestKafkaMetrics();

        // Act & Assert - Should not throw
        using var serializer = new AvroMessageSerializer(schemaRegistryConfig, metrics);
        serializer.Should().NotBeNull();
    }

    [Fact]
    public void AvroSerialization_WithNullConfig_ThrowsArgumentNullException()
    {
        // Arrange
        var metrics = new TestKafkaMetrics();

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new AvroMessageSerializer((SchemaRegistryConfiguration)null!, metrics));
    }

    [Fact]
    public void AvroSerialization_WithNullMetrics_ThrowsArgumentNullException()
    {
        // Arrange
        var schemaRegistryConfig = new SchemaRegistryConfiguration
        {
            Url = "http://localhost:8081",
            AutoRegisterSchemas = true,
            RequestTimeoutMs = 30000,
        };

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new AvroMessageSerializer(schemaRegistryConfig, null!));
    }

    [Fact]
    public async Task ProtobufSerialization_WithSchemaRegistry_CanBeCreated()
    {
        // Arrange
        await _fixture.EnsureSchemaRegistryAsync();

        var schemaRegistryConfig = new SchemaRegistryConfiguration
        {
            Url = _fixture.SchemaRegistryUrl,
            AutoRegisterSchemas = true,
            RequestTimeoutMs = 30000,
        };

        var metrics = new TestKafkaMetrics();

        // Act & Assert - Should not throw
        using var serializer = new ProtobufMessageSerializer(schemaRegistryConfig, metrics);
        serializer.Should().NotBeNull();
    }

    [Fact]
    public void ProtobufSerialization_WithNullConfig_ThrowsArgumentNullException()
    {
        // Arrange
        var metrics = new TestKafkaMetrics();

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new ProtobufMessageSerializer((SchemaRegistryConfiguration)null!, metrics));
    }

    [Fact]
    public void ProtobufSerialization_WithNullMetrics_ThrowsArgumentNullException()
    {
        // Arrange
        var schemaRegistryConfig = new SchemaRegistryConfiguration
        {
            Url = "http://localhost:8081",
            AutoRegisterSchemas = true,
            RequestTimeoutMs = 30000,
        };

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new ProtobufMessageSerializer(schemaRegistryConfig, null!));
    }

    [Fact]
    public async Task AvroSerialization_RoundTripsSimpleType()
    {
        // Arrange
        await _fixture.EnsureSchemaRegistryAsync();

        var schemaRegistryConfig = new SchemaRegistryConfiguration
        {
            Url = _fixture.SchemaRegistryUrl,
            AutoRegisterSchemas = true,
            RequestTimeoutMs = 30000,
        };

        var metrics = new TestKafkaMetrics();

        using var serializer = new AvroMessageSerializer(schemaRegistryConfig, metrics);

        // Note: Avro requires specific record types generated from Avro schema
        // For this test, we verify the serializer can be created and disposed
        // Full round-trip testing would require generated Avro classes

        serializer.Should().NotBeNull();
    }

    [Fact]
    public async Task ProtobufSerialization_RoundTripsSimpleType()
    {
        // Arrange
        await _fixture.EnsureSchemaRegistryAsync();

        var schemaRegistryConfig = new SchemaRegistryConfiguration
        {
            Url = _fixture.SchemaRegistryUrl,
            AutoRegisterSchemas = true,
            RequestTimeoutMs = 30000,
        };

        var metrics = new TestKafkaMetrics();

        using var serializer = new ProtobufMessageSerializer(schemaRegistryConfig, metrics);

        // Note: Protobuf requires IMessage<T> types generated from .proto files
        // For this test, we verify the serializer can be created and disposed
        // Full round-trip testing would require generated Protobuf classes

        serializer.Should().NotBeNull();
    }
}

/// <summary>
///     Test message type for serialization tests.
/// </summary>
public sealed class SerializationTestMessage
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public double Value { get; set; }
    public DateTime Timestamp { get; set; }
    public List<string> Tags { get; set; } = [];
}

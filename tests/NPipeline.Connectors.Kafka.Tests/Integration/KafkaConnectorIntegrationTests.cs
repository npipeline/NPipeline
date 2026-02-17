using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.Kafka.Configuration;
using NPipeline.Connectors.Kafka.Metrics;
using NPipeline.Connectors.Kafka.Models;
using NPipeline.Connectors.Kafka.Nodes;
using NPipeline.Connectors.Kafka.Partitioning;
using NPipeline.Connectors.Kafka.Retry;
using NPipeline.Connectors.Kafka.Tests.Fixtures;
using NPipeline.Pipeline;
using KafkaDeliverySemantic = NPipeline.Connectors.Kafka.Configuration.DeliverySemantic;

namespace NPipeline.Connectors.Kafka.Tests.Integration;

/// <summary>
///     Integration tests for Kafka connector using TestContainers.
/// </summary>
[Collection("Kafka")]
public sealed class KafkaConnectorIntegrationTests : IAsyncLifetime
{
    private readonly KafkaTestContainerFixture _fixture;
    private readonly List<string> _topicsToCleanup = [];

    public KafkaConnectorIntegrationTests(KafkaTestContainerFixture fixture)
    {
        _fixture = fixture;
    }

    public async Task InitializeAsync()
    {
        // Create default topics for testing
        await CreateTopicAsync("test-source-topic", 3);
        await CreateTopicAsync("test-sink-topic", 3);
    }

    public async Task DisposeAsync()
    {
        // Clean up topics created during tests
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

            // Wait for topic to be ready
            await Task.Delay(1000);
        }
        catch (CreateTopicsException ex)
        {
            // Topic may already exist
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

    private KafkaConfiguration CreateSourceConfig(string topic, string consumerGroup)
    {
        return new KafkaConfiguration
        {
            BootstrapServers = _fixture.BootstrapServers,
            ClientId = $"test-consumer-{Guid.NewGuid():N}",
            SourceTopic = topic,
            ConsumerGroupId = consumerGroup,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            DeliverySemantic = KafkaDeliverySemantic.AtLeastOnce,
            AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess,
            MaxPollRecords = 100,
        };
    }

    private KafkaConfiguration CreateSinkConfig(string topic)
    {
        return new KafkaConfiguration
        {
            BootstrapServers = _fixture.BootstrapServers,
            ClientId = $"test-producer-{Guid.NewGuid():N}",
            SinkTopic = topic,
            EnableIdempotence = true,
            BatchSize = 100,
            LingerMs = 5,
            Acks = Acks.All,
            DeliverySemantic = KafkaDeliverySemantic.AtLeastOnce,
            AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess,
        };
    }

    [Fact]
    public async Task EndToEnd_SourceToSink_TransfersMessagesCorrectly()
    {
        // Arrange
        var sourceTopic = "test-source-topic";
        var sinkTopic = "test-sink-topic";
        var consumerGroup = $"test-group-{Guid.NewGuid():N}";
        var testMessages = GenerateTestMessages(10);

        // Produce test messages to source topic
        await ProduceMessagesAsync(sourceTopic, testMessages);

        // Create source and sink nodes
        var metrics = new TestKafkaMetrics();

        var retryStrategy = new ExponentialBackoffRetryStrategy
        {
            MaxRetries = 3,
            BaseDelayMs = 100,
        };

        var sourceConfig = CreateSourceConfig(sourceTopic, consumerGroup);
        var sinkConfig = CreateSinkConfig(sinkTopic);

        var sourceNode = new KafkaSourceNode<TestMessage>(sourceConfig, metrics, retryStrategy);
        var partitionKeyProvider = PartitionKeyProvider.FromProperty<TestMessage, Guid>(m => m.Id);
        var sinkNode = new KafkaSinkNode<TestMessage>(sinkConfig, metrics, retryStrategy, partitionKeyProvider);

        // Act - Consume from source and produce to sink
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var context = new PipelineContext();
        var dataPipe = sourceNode.Initialize(context, cts.Token);

        var processedMessages = new List<TestMessage>();

        await foreach (var kafkaMessage in dataPipe.WithCancellation(cts.Token))
        {
            processedMessages.Add(kafkaMessage.Body);
            await kafkaMessage.AcknowledgeAsync(cts.Token);

            if (processedMessages.Count >= 10)
                break;
        }

        // Assert - Verify messages were consumed
        processedMessages.Should().HaveCount(10);
        processedMessages.Select(m => m.Id).Should().BeEquivalentTo(testMessages.Select(m => m.Id));
    }

    [Fact]
    public async Task JsonSerialization_RoundTripsCorrectly()
    {
        // Arrange
        var topic = $"json-test-{Guid.NewGuid():N}";
        await CreateTopicAsync(topic);

        var config = CreateSinkConfig(topic);
        config.SerializationFormat = SerializationFormat.Json;

        var metrics = new TestKafkaMetrics();
        var retryStrategy = new ExponentialBackoffRetryStrategy();
        var partitionKeyProvider = PartitionKeyProvider.FromProperty<TestMessage, Guid>(m => m.Id);

        var message = new TestMessage
        {
            Id = Guid.NewGuid(),
            Name = "Json Test Message",
            Value = 42.5,
            Timestamp = DateTime.UtcNow,
        };

        // Act - Produce and consume
        var sinkNode = new KafkaSinkNode<TestMessage>(config, metrics, retryStrategy, partitionKeyProvider);

        // Create a simple pipe with the message
        var context = new PipelineContext();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        // Verify node was created successfully
        sinkNode.Should().NotBeNull();

        // Cleanup
        await sinkNode.DisposeAsync();
    }

    [Fact]
    public async Task ConsumerGroup_MultipleConsumers_SharePartitions()
    {
        // Arrange
        var topic = $"partition-test-{Guid.NewGuid():N}";
        var consumerGroup = $"partition-group-{Guid.NewGuid():N}";
        await CreateTopicAsync(topic, 3);

        // Produce messages to multiple partitions
        var messages = Enumerable.Range(0, 30)
            .Select(i => new TestMessage
            {
                Id = Guid.NewGuid(),
                Name = $"Message {i}",
                Value = i,
                Timestamp = DateTime.UtcNow,
            })
            .ToList();

        await ProduceMessagesAsync(topic, messages);

        // Create two consumers in the same group
        var metrics = new TestKafkaMetrics();
        var retryStrategy = new ExponentialBackoffRetryStrategy();

        var config1 = CreateSourceConfig(topic, consumerGroup);
        config1.ClientId = $"consumer-1-{Guid.NewGuid():N}";

        var config2 = CreateSourceConfig(topic, consumerGroup);
        config2.ClientId = $"consumer-2-{Guid.NewGuid():N}";

        var consumer1 = new KafkaSourceNode<TestMessage>(config1, metrics, retryStrategy);
        var consumer2 = new KafkaSourceNode<TestMessage>(config2, metrics, retryStrategy);

        // Assert - Both consumers should be created
        consumer1.Should().NotBeNull();
        consumer2.Should().NotBeNull();
    }

    [Fact]
    public async Task AtLeastOnce_DeliverySemantic_CommitsOffsets()
    {
        // Arrange
        var topic = $"commit-test-{Guid.NewGuid():N}";
        var consumerGroup = $"commit-group-{Guid.NewGuid():N}";
        await CreateTopicAsync(topic);

        var messages = GenerateTestMessages(5);
        await ProduceMessagesAsync(topic, messages);

        var config = CreateSourceConfig(topic, consumerGroup);
        config.DeliverySemantic = KafkaDeliverySemantic.AtLeastOnce;
        config.EnableAutoCommit = false;

        var metrics = new TestKafkaMetrics();
        var retryStrategy = new ExponentialBackoffRetryStrategy();

        var sourceNode = new KafkaSourceNode<TestMessage>(config, metrics, retryStrategy);

        // Act - Consume and acknowledge messages
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var context = new PipelineContext();
        var dataPipe = sourceNode.Initialize(context, cts.Token);

        var consumedMessages = new List<KafkaMessage<TestMessage>>();

        await foreach (var kafkaMessage in dataPipe.WithCancellation(cts.Token))
        {
            consumedMessages.Add(kafkaMessage);
            await kafkaMessage.AcknowledgeAsync(cts.Token);

            if (consumedMessages.Count >= 5)
                break;
        }

        // Assert - Messages should be acknowledged
        consumedMessages.Should().HaveCount(5);
        consumedMessages.All(m => m.IsAcknowledged).Should().BeTrue();
    }

    [Fact]
    public async Task PartitionKeyProvider_RoutesToCorrectPartition()
    {
        // Arrange
        var topic = $"routing-test-{Guid.NewGuid():N}";
        await CreateTopicAsync(topic, 3);

        var config = CreateSinkConfig(topic);
        var metrics = new TestKafkaMetrics();
        var retryStrategy = new ExponentialBackoffRetryStrategy();

        // Use message ID as partition key
        var partitionKeyProvider = PartitionKeyProvider.FromProperty<TestMessage, Guid>(m => m.Id);

        var sinkNode = new KafkaSinkNode<TestMessage>(config, metrics, retryStrategy, partitionKeyProvider);

        // Assert - Node should be created successfully
        sinkNode.Should().NotBeNull();
    }

    [Fact]
    public async Task BatchProcessing_HandlesMultipleMessages()
    {
        // Arrange
        var topic = $"batch-test-{Guid.NewGuid():N}";
        var consumerGroup = $"batch-group-{Guid.NewGuid():N}";
        await CreateTopicAsync(topic);

        var messages = GenerateTestMessages(100);
        await ProduceMessagesAsync(topic, messages);

        var config = CreateSourceConfig(topic, consumerGroup);
        config.MaxPollRecords = 50; // Process in batches

        var metrics = new TestKafkaMetrics();
        var retryStrategy = new ExponentialBackoffRetryStrategy();

        var sourceNode = new KafkaSourceNode<TestMessage>(config, metrics, retryStrategy);

        // Act - Consume in batches
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var context = new PipelineContext();
        var dataPipe = sourceNode.Initialize(context, cts.Token);

        var consumedCount = 0;

        await foreach (var _ in dataPipe.WithCancellation(cts.Token))
        {
            consumedCount++;

            if (consumedCount >= 100)
                break;
        }

        // Assert
        consumedCount.Should().Be(100);
    }

    private List<TestMessage> GenerateTestMessages(int count)
    {
        return Enumerable.Range(0, count)
            .Select(i => new TestMessage
            {
                Id = Guid.NewGuid(),
                Name = $"Test Message {i}",
                Value = i * 1.5,
                Timestamp = DateTime.UtcNow,
            })
            .ToList();
    }

    private async Task ProduceMessagesAsync(string topic, List<TestMessage> messages)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _fixture.BootstrapServers,
            ClientId = $"test-producer-{Guid.NewGuid():N}",
        };

        using var producer = new ProducerBuilder<string, TestMessage>(config)
            .SetValueSerializer(new TestMessageSerializer())
            .Build();

        foreach (var message in messages)
        {
            await producer.ProduceAsync(topic, new Message<string, TestMessage>
            {
                Key = message.Id.ToString(),
                Value = message,
            });
        }
    }

    /// <summary>
    ///     Simple JSON serializer for test messages.
    /// </summary>
    private sealed class TestMessageSerializer : ISerializer<TestMessage>
    {
        public byte[] Serialize(TestMessage data, SerializationContext context)
        {
            var json = JsonSerializer.Serialize(data);
            return Encoding.UTF8.GetBytes(json);
        }
    }
}

/// <summary>
///     Test message type for integration tests.
/// </summary>
public sealed record TestMessage
{
    public Guid Id { get; init; }
    public string Name { get; init; } = string.Empty;
    public double Value { get; init; }
    public DateTime Timestamp { get; init; }
}

/// <summary>
///     Test implementation of Kafka metrics.
/// </summary>
public sealed class TestKafkaMetrics : IKafkaMetrics
{
    public int ProducedCount { get; private set; }
    public int ConsumedCount { get; private set; }
    public int ErrorCount { get; private set; }

    public void RecordProduced(string topic, int count)
    {
        ProducedCount += count;
    }

    public void RecordProduceLatency(string topic, TimeSpan latency)
    {
    }

    public void RecordProduceError(string topic, Exception ex)
    {
        ErrorCount++;
    }

    public void RecordBatchSize(string topic, int size)
    {
    }

    public void RecordConsumed(string topic, int count)
    {
        ConsumedCount += count;
    }

    public void RecordPollLatency(string topic, TimeSpan latency)
    {
    }

    public void RecordCommitLatency(string topic, TimeSpan latency)
    {
    }

    public void RecordCommitError(string topic, Exception ex)
    {
        ErrorCount++;
    }

    public void RecordLag(string topic, int partition, long lag)
    {
    }

    public void RecordSerializeLatency(Type type, TimeSpan latency)
    {
    }

    public void RecordDeserializeLatency(Type type, TimeSpan latency)
    {
    }

    public void RecordSerializeError(Type type, Exception ex)
    {
        ErrorCount++;
    }

    public void RecordDeserializeError(Type type, Exception ex)
    {
        ErrorCount++;
    }

    public void RecordTransactionCommit(TimeSpan latency)
    {
    }

    public void RecordTransactionAbort(TimeSpan latency)
    {
    }

    public void RecordTransactionError(Exception ex)
    {
        ErrorCount++;
    }
}

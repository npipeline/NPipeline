using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.SchemaRegistry;
using FakeItEasy;
using Google.Protobuf.WellKnownTypes;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Kafka.Configuration;
using NPipeline.Connectors.Kafka.Nodes;
using NPipeline.Connectors.Kafka.Serialization;
using NPipeline.Connectors.Kafka.Tests.Fixtures;
using NPipeline.Connectors.Kafka.Tests.Integration;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Kafka.Tests.Serialization;

/// <summary>
///     Tests for the sink's value serializer: an acknowledgable message is written as its body, serialized as the
///     body's runtime type rather than as <see cref="object" />.
/// </summary>
public sealed class KafkaValueSerializerTests
{
    private static readonly SerializationContext Context = new(MessageComponentType.Value, "orders");

    [Fact]
    public void AcknowledgableMessage_IsSerializedAsItsBodysRuntimeType()
    {
        var provider = A.Fake<ISerializerProvider>();

        A.CallTo(() => provider.Serialize("order-1", A<SerializationContext>.That.Matches(c =>
                c.Topic == "orders" && c.Component == MessageComponentType.Value)))
            .Returns([1, 2, 3]);

        var serializer = new KafkaValueSerializer<IAcknowledgableMessage>(provider);

        serializer.Serialize(Message("order-1"), Context).Should().Equal(1, 2, 3);
        A.CallTo(() => provider.Serialize<object>(A<object>._, A<SerializationContext>._)).MustNotHaveHappened();
    }

    [Fact]
    public void PlainValue_IsSerializedAsTheSinksType()
    {
        var provider = A.Fake<ISerializerProvider>();
        A.CallTo(() => provider.Serialize("order-1", A<SerializationContext>.That.Matches(c => c.Topic == "orders"))).Returns([7]);

        new KafkaValueSerializer<string>(provider).Serialize("order-1", Context).Should().Equal(7);
    }

    [Fact]
    public void AcknowledgableMessage_WithNoBody_IsEmpty()
    {
        var serializer = new KafkaValueSerializer<IAcknowledgableMessage>(A.Fake<ISerializerProvider>());

        serializer.Serialize(Message(null), Context).Should().BeEmpty();
    }

    internal static IAcknowledgableMessage Message(object? body)
    {
        var message = A.Fake<IAcknowledgableMessage>();
        A.CallTo(() => message.Body).Returns(body!);
        return message;
    }
}

/// <summary>
///     Avro and Protobuf choose the schema from the type they serialize, and the Schema Registry subject from the topic.
///     These tests run against the container's Schema Registry under the default subject name strategy.
/// </summary>
[Collection("Kafka")]
public sealed class KafkaValueSerializerSchemaRegistryTests(KafkaTestContainerFixture fixture)
{
    [Fact]
    public async Task Avro_AcknowledgableMessage_RoundTripsItsBody_UnderTheTopicsSubject()
    {
        var topic = $"avro-{Guid.NewGuid():N}";
        var registry = await RegistryAsync();
        using var avro = new AvroMessageSerializer(registry, new TestKafkaMetrics());
        var serializer = new KafkaValueSerializer<IAcknowledgableMessage>(avro);
        var context = new SerializationContext(MessageComponentType.Value, topic);

        var bytes = serializer.Serialize(KafkaValueSerializerTests.Message("order-1"), context);

        avro.Deserialize<string>(bytes, context).Should().Be("order-1");
        (await SubjectsAsync(registry)).Should().Contain($"{topic}-value");
    }

    [Fact]
    public async Task Protobuf_AcknowledgableMessage_RoundTripsItsBody_UnderTheTopicsSubject()
    {
        var topic = $"protobuf-{Guid.NewGuid():N}";
        var registry = await RegistryAsync();
        using var protobuf = new ProtobufMessageSerializer(registry, new TestKafkaMetrics());
        var serializer = new KafkaValueSerializer<IAcknowledgableMessage>(protobuf);
        var context = new SerializationContext(MessageComponentType.Value, topic);

        var bytes = serializer.Serialize(KafkaValueSerializerTests.Message(new StringValue { Value = "order-1" }), context);

        protobuf.Deserialize<StringValue>(bytes, context).Value.Should().Be("order-1");
        (await SubjectsAsync(registry)).Should().Contain($"{topic}-value");
    }

    [Fact]
    public async Task TwoSinks_WritingDifferentTypesToDifferentTopics_BothSucceed_AndTheSourceReadsThemBack()
    {
        var registry = await RegistryAsync();
        var stringTopic = $"avro-strings-{Guid.NewGuid():N}";
        var longTopic = $"avro-longs-{Guid.NewGuid():N}";

        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = fixture.BootstrapServers }).Build())
        {
            await admin.CreateTopicsAsync(
            [
                new TopicSpecification { Name = stringTopic, NumPartitions = 1, ReplicationFactor = 1 },
                new TopicSpecification { Name = longTopic, NumPartitions = 1, ReplicationFactor = 1 },
            ]);
        }

        // Before the fix both types registered under the one subject "-value", and the second failed with a 409.
        await using (var strings = new KafkaSinkNode<string>(SinkConfiguration(stringTopic, registry)))
        await using (var input = new InMemoryDataStream<string>(["order-1"]))
        {
            await strings.ConsumeAsync(input, new PipelineContext(), CancellationToken.None);
        }

        await using (var longs = new KafkaSinkNode<long>(SinkConfiguration(longTopic, registry)))
        await using (var input = new InMemoryDataStream<long>([42L]))
        {
            await longs.ConsumeAsync(input, new PipelineContext(), CancellationToken.None);
        }

        (await SubjectsAsync(registry)).Should().Contain([$"{stringTopic}-value", $"{longTopic}-value"]);

        (await ReadOneAsync<string>(stringTopic, registry)).Should().Be("order-1");
        (await ReadOneAsync<long>(longTopic, registry)).Should().Be(42L);
    }

    private KafkaConfiguration SinkConfiguration(string topic, SchemaRegistryConfiguration registry) =>
        new()
        {
            BootstrapServers = fixture.BootstrapServers,
            SinkTopic = topic,
            BatchSize = 1,
            SerializationFormat = SerializationFormat.Avro,
            SchemaRegistry = registry,
        };

    private async Task<T> ReadOneAsync<T>(string topic, SchemaRegistryConfiguration registry)
    {
        var source = new KafkaSourceNode<T>(new KafkaConfiguration
        {
            BootstrapServers = fixture.BootstrapServers,
            SourceTopic = topic,
            ConsumerGroupId = $"verify-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            SerializationFormat = SerializationFormat.Avro,
            SchemaRegistry = registry,
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await foreach (var message in source.OpenStream(new PipelineContext(), cts.Token))
        {
            return message.Body;
        }

        throw new InvalidOperationException($"No message was read from {topic}.");
    }

    private static async Task<List<string>> SubjectsAsync(SchemaRegistryConfiguration registry)
    {
        using var client = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = registry.Url });
        return await client.GetAllSubjectsAsync();
    }

    private async Task<SchemaRegistryConfiguration> RegistryAsync()
    {
        await fixture.EnsureSchemaRegistryAsync();

        return new SchemaRegistryConfiguration
        {
            Url = fixture.SchemaRegistryUrl,
            AutoRegisterSchemas = true,
            RequestTimeoutMs = 30000,
        };
    }
}

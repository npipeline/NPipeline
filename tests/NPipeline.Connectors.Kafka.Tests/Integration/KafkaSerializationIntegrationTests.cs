using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Google.Protobuf.WellKnownTypes;
using NPipeline.Connectors.Kafka.Configuration;
using NPipeline.Connectors.Kafka.Serialization;
using NPipeline.Connectors.Kafka.Tests.Fixtures;
using NPipeline.Connectors.Messaging;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Kafka.Tests.Integration;

/// <summary>
///     The schema-registry serializers. Avro and Protobuf choose the schema from the type they serialize and the subject from
///     the destination; these tests run against the container's Schema Registry under the default subject name strategy.
/// </summary>
[Collection("Kafka")]
public sealed class KafkaSerializationIntegrationTests(KafkaTestContainerFixture fixture)
{
    private static readonly SchemaRegistryConfiguration Offline = new() { Url = "http://localhost:1" };

    [Fact]
    public void Constructors_RejectANullConfiguration()
    {
        ((Action)(() => _ = new AvroMessageSerializer((SchemaRegistryConfiguration)null!))).Should().Throw<ArgumentNullException>();
        ((Action)(() => _ = new AvroMessageSerializer((CachedSchemaRegistryClient)null!))).Should().Throw<ArgumentNullException>();
        ((Action)(() => _ = new ProtobufMessageSerializer((SchemaRegistryConfiguration)null!))).Should().Throw<ArgumentNullException>();
        ((Action)(() => _ = new ProtobufMessageSerializer((CachedSchemaRegistryClient)null!))).Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void ContentTypes_NameTheFormat()
    {
        using var avro = new AvroMessageSerializer(Offline);
        using var protobuf = new ProtobufMessageSerializer(Offline);

        avro.ContentType.Should().Be("application/vnd.apache.avro");
        protobuf.ContentType.Should().Be("application/x-protobuf");
    }

    [Fact]
    public void EmptyBody_DoesNotDeserialize()
    {
        using var avro = new AvroMessageSerializer(Offline);
        using var protobuf = new ProtobufMessageSerializer(Offline);

        ((Action)(() => avro.Deserialize<string>([], new MessageContext("orders")))).Should().Throw<InvalidDataException>();
        ((Action)(() => protobuf.Deserialize<StringValue>([], new MessageContext("orders")))).Should().Throw<InvalidDataException>();
    }

    [Fact]
    public void NullValue_SerializesToAnEmptyBody()
    {
        using var avro = new AvroMessageSerializer(Offline);

        avro.Serialize<string?>(null, new MessageContext("orders")).Should().BeEmpty();
    }

    [Fact]
    public void Protobuf_RejectsATypeThatIsNotAProtobufMessage()
    {
        using var protobuf = new ProtobufMessageSerializer(Offline);

        var act = () => protobuf.Serialize("not a message", new MessageContext("orders"));

        act.Should().Throw<InvalidOperationException>().WithMessage("*IMessage*");
    }

    [Fact]
    public void SchemaRegistryConfiguration_Validates()
    {
        new SchemaRegistryConfiguration().Invoking(c => c.Validate()).Should().Throw<InvalidOperationException>().WithMessage("*URL*");
        (Offline with { RequestTimeoutMs = 0 }).Invoking(c => c.Validate()).Should().Throw<InvalidOperationException>();
        (Offline with { SchemaCacheCapacity = 0 }).Invoking(c => c.Validate()).Should().Throw<InvalidOperationException>();
        Offline.Invoking(c => c.Validate()).Should().NotThrow();
    }

    [Fact]
    public async Task Avro_RoundTrips_UnderTheDestinationsSubject()
    {
        var topic = $"avro-{Guid.NewGuid():N}";
        var registry = await RegistryAsync();
        using var avro = new AvroMessageSerializer(registry);
        var context = new MessageContext(topic);

        var bytes = avro.Serialize("order-1", context);

        avro.Deserialize<string>(bytes, context).Should().Be("order-1");
        (await SubjectsAsync(registry)).Should().Contain($"{topic}-value");
    }

    [Fact]
    public async Task Avro_Key_UsesTheKeySubject()
    {
        var topic = $"avro-key-{Guid.NewGuid():N}";
        var registry = await RegistryAsync();
        using var avro = new AvroMessageSerializer(registry);
        var context = new MessageContext(topic, IsKey: true);

        var bytes = avro.Serialize(42L, context);

        avro.Deserialize<long>(bytes, context).Should().Be(42L);
        (await SubjectsAsync(registry)).Should().Contain($"{topic}-key");
    }

    [Fact]
    public async Task Protobuf_RoundTrips_UnderTheDestinationsSubject()
    {
        var topic = $"protobuf-{Guid.NewGuid():N}";
        var registry = await RegistryAsync();
        using var protobuf = new ProtobufMessageSerializer(registry);
        var context = new MessageContext(topic);

        var bytes = protobuf.Serialize(new StringValue { Value = "order-1" }, context);

        protobuf.Deserialize<StringValue>(bytes, context).Value.Should().Be("order-1");
        (await SubjectsAsync(registry)).Should().Contain($"{topic}-value");
    }

    [Fact]
    public async Task TwoSinks_WritingDifferentTypesToDifferentTopics_BothSucceed_AndTheSourceReadsThemBack()
    {
        var registry = await RegistryAsync();
        using var avro = new AvroMessageSerializer(registry);
        var stringTopic = await fixture.CreateTopicAsync("avro-strings");
        var longTopic = await fixture.CreateTopicAsync("avro-longs");

        // Each type registers under its own topic's subject; one shared subject would fail the second with a 409.
        await using (var strings = KafkaConnector.Sink<string>(fixture.BootstrapServers, stringTopic, o => o with { Serializer = avro }))
        {
            await strings.ConsumeAsync(new InMemoryDataStream<string>(["order-1"]), new PipelineContext(), CancellationToken.None);
        }

        await using (var longs = KafkaConnector.Sink<long>(fixture.BootstrapServers, longTopic, o => o with { Serializer = avro }))
        {
            await longs.ConsumeAsync(new InMemoryDataStream<long>([42L]), new PipelineContext(), CancellationToken.None);
        }

        (await SubjectsAsync(registry)).Should().Contain([$"{stringTopic}-value", $"{longTopic}-value"]);

        (await ReadOneAsync<string>(stringTopic, avro)).Should().Be("order-1");
        (await ReadOneAsync<long>(longTopic, avro)).Should().Be(42L);
    }

    [Fact]
    public async Task ProtobufSink_ThenSource_RoundTrips()
    {
        var registry = await RegistryAsync();
        using var protobuf = new ProtobufMessageSerializer(registry);
        var topic = await fixture.CreateTopicAsync("protobuf-nodes");

        await using (var sink = KafkaConnector.Sink<StringValue>(fixture.BootstrapServers, topic, o => o with { Serializer = protobuf }))
        {
            await sink.ConsumeAsync(new InMemoryDataStream<StringValue>([new StringValue { Value = "order-1" }]), new PipelineContext(),
                CancellationToken.None);
        }

        (await ReadOneAsync<StringValue>(topic, protobuf)).Value.Should().Be("order-1");
    }

    private async Task<T> ReadOneAsync<T>(string topic, IMessageSerializer serializer)
    {
        await using var source = KafkaConnector.Source<T>(fixture.BootstrapServers, topic, $"verify-{Guid.NewGuid():N}",
            o => o with { AutoOffsetReset = AutoOffsetReset.Earliest, Serializer = serializer });

        var read = await source.OpenStream(new PipelineContext(), CancellationToken.None).TakeAsync(1, TimeSpan.FromSeconds(30));

        return read.Should().ContainSingle($"a message was written to {topic}").Subject.Body;
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

using Confluent.Kafka;
using Confluent.Kafka.Admin;
using NPipeline.Connectors.Kafka;
using NPipeline.Connectors.Kafka.Models;
using NPipeline.Connectors.Messaging.RoundTrip.Tests.Harnesses;
using NPipeline.Connectors.Messaging.RoundTrip.Tests.Infrastructure;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Messaging.RoundTrip.Tests.Brokers;

public sealed class KafkaHarness(string bootstrapServers) : MessagingHarness
{
    public override string Name => "Kafka";

    public override bool RemovesSettledMessages => false;

    public string BootstrapServers => bootstrapServers;

    public override async Task<string> CreateDestinationAsync()
    {
        var topic = $"rt-{Guid.NewGuid():N}";
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();
        await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }]);
        return topic;
    }

    public override async Task PublishRawAsync(string destination, params string[] bodies)
    {
        using var producer = new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build();

        foreach (var body in bodies)
        {
            _ = await producer.ProduceAsync(destination, new Message<Null, string> { Value = body });
        }
    }

    public override Task<List<string>> ReceiveRawAsync(string destination, int count, TimeSpan timeout)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"raw-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        consumer.Subscribe(destination);
        var bodies = new List<string>();
        var deadline = DateTime.UtcNow + timeout;

        while (bodies.Count < count && DateTime.UtcNow < deadline)
        {
            if (consumer.Consume(TimeSpan.FromMilliseconds(250)) is { Message: { } message })
                bodies.Add(message.Value);
        }

        consumer.Close();
        return Task.FromResult(bodies);
    }

    // One group per topic, so a second read resumes where the first one committed.
    public override IAsyncEnumerable<IAcknowledgableMessage<T>> ReadAsync<T>(string destination, ReadSettings settings, PipelineContext context,
        CancellationToken cancellationToken) =>
        Enumerate<KafkaMessage<T>, T>(KafkaConnector.Source<T>(bootstrapServers, destination, $"{destination}-group",
            o => o with { AutoOffsetReset = AutoOffsetReset.Earliest, RowErrorHandler = settings.Handler }), context, cancellationToken);

    protected override SinkNode<T> Sink<T>(string destination) => KafkaConnector.Sink<T>(bootstrapServers, destination);
}

[Collection(KafkaBrokers.Name)]
public sealed class KafkaRoundTripTests(KafkaBrokerFixture broker) : MessagingRoundTripTests(new KafkaHarness(broker.BootstrapServers))
{
    [Fact]
    public async Task Acknowledging_commits_the_next_offset()
    {
        var topic = await Harness.CreateDestinationAsync();
        await Harness.PublishRawAsync(topic, """{"id":1}""", """{"id":2}""", """{"id":3}""");

        (await Harness.ReadAsync<Order>(topic, ReadSettings.Defaults, new PipelineContext(), CancellationToken.None)
            .TakeAsync(3, TimeSpan.FromSeconds(30), m => m.AcknowledgeAsync())).Should().HaveCount(3);

        // Closing the consumer commits what it stored.
        await Harness.DisposeNodesAsync();

        Committed(topic).Should().Be(3, "Kafka resumes from the committed offset, the next one to read");
    }

    [Fact]
    public async Task Commits_stop_at_a_message_not_yet_acknowledged()
    {
        var topic = await Harness.CreateDestinationAsync();
        await Harness.PublishRawAsync(topic, """{"id":1}""", """{"id":2}""", """{"id":3}""");

        var read = await Harness.ReadAsync<Order>(topic, ReadSettings.Defaults, new PipelineContext(), CancellationToken.None)
            .TakeAsync(3, TimeSpan.FromSeconds(30));

        // The first and third are acknowledged; the second is requeued, as a failed write does.
        await read[0].AcknowledgeAsync();
        await read[2].AcknowledgeAsync();
        await read[1].RejectAsync(true);
        await Harness.DisposeNodesAsync();

        Committed(topic).Should().Be(1, "committing past the second message would lose it");
    }

    private long Committed(string topic)
    {
        using var consumer = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig { BootstrapServers = ((KafkaHarness)Harness).BootstrapServers, GroupId = $"{topic}-group" })
            .Build();

        return consumer.Committed([new TopicPartition(topic, 0)], TimeSpan.FromSeconds(10)).Single().Offset.Value;
    }
}

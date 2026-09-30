using System.Text;
using Confluent.Kafka;
using NPipeline.Configuration;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Kafka.DeadLetter;
using NPipeline.Connectors.Kafka.Models;
using NPipeline.Connectors.Kafka.Tests.Fixtures;
using NPipeline.Connectors.Messaging;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Kafka.Tests.Integration;

/// <summary>
///     The source and sink against the container's broker.
/// </summary>
[Collection("Kafka")]
public sealed class KafkaConnectorIntegrationTests(KafkaTestContainerFixture fixture)
{
    private static readonly TimeSpan ReadTimeout = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task Sink_ThenSource_RoundTripsBodiesKeysAndPartitions()
    {
        var topic = await fixture.CreateTopicAsync("round-trip", 3);
        var orders = Enumerable.Range(1, 12).Select(i => new TestMessage(i, $"customer-{i % 3}", i * 1.5m)).ToList();

        await using (var sink = KafkaConnector.Sink<TestMessage>(fixture.BootstrapServers, topic, o => o with { KeySelector = m => m.Customer }))
        {
            await sink.ConsumeAsync(new InMemoryDataStream<TestMessage>(orders), new PipelineContext(), CancellationToken.None);
        }

        await using var source = KafkaConnector.Source<TestMessage>(fixture.BootstrapServers, topic, $"group-{Guid.NewGuid():N}",
            o => o with { AutoOffsetReset = AutoOffsetReset.Earliest });

        var read = await source.OpenStream(new PipelineContext(), CancellationToken.None).TakeAsync(orders.Count, ReadTimeout);

        read.Select(m => m.Body).Should().BeEquivalentTo(orders);
        read.Should().OnlyContain(m => m.Key == m.Body.Customer && m.Topic == topic && m.MessageId == $"{topic}/{m.Partition}/{m.Offset}");

        read.GroupBy(m => m.Key).Should().OnlyContain(g => g.Select(m => m.Partition).Distinct().Count() == 1, "one key goes to one partition");

        foreach (var partition in read.GroupBy(m => m.Partition))
        {
            partition.Select(m => m.Body.Id).Should().BeInAscendingOrder("a partition keeps the order it was written in");
        }
    }

    [Fact]
    public async Task Sink_WritesJsonBodies_WithNoKey_ByDefault()
    {
        var topic = await fixture.CreateTopicAsync("json");

        await using (var sink = KafkaConnector.Sink<TestMessage>(fixture.BootstrapServers, topic))
        {
            await sink.ConsumeAsync(new InMemoryDataStream<TestMessage>([new TestMessage(1, "acme", 2.5m)]), new PipelineContext(),
                CancellationToken.None);
        }

        var record = fixture.ConsumeRaw(topic, 1, ReadTimeout).Should().ContainSingle().Subject;
        record.Message.Key.Should().BeNull();
        record.Message.Value.Should().Be("""{"id":1,"customer":"acme","total":2.5}""");
    }

    [Fact]
    public async Task SourceIntoAcknowledgingSink_CommitsTheGroupsOffset_AndKeepsKeysAndHeaders()
    {
        var input = await fixture.CreateTopicAsync("ack-in");
        var output = await fixture.CreateTopicAsync("ack-out");
        var group = $"group-{Guid.NewGuid():N}";

        using (var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = fixture.BootstrapServers }).Build())
        {
            for (var i = 1; i <= 3; i++)
            {
                _ = await producer.ProduceAsync(input, new Message<string, string>
                {
                    Key = $"key-{i}",
                    Value = $$"""{"id":{{i}},"customer":"c","total":1}""",
                    Headers = new Headers { { "trace", Encoding.UTF8.GetBytes($"t-{i}") } },
                });
            }
        }

        var source = KafkaConnector.Source<TestMessage>(fixture.BootstrapServers, input, group, o => o with { AutoOffsetReset = AutoOffsetReset.Earliest });

        await using (var sink = KafkaConnector.Sink<TestMessage>(fixture.BootstrapServers, output, o => o with { CopyHeaders = true }))
        {
            var messages = source.OpenStream(new PipelineContext(), CancellationToken.None).Limit<KafkaMessage<TestMessage>, IAcknowledgableMessage<TestMessage>>(3);
            await sink.Acknowledging().ConsumeAsync(messages, new PipelineContext(), CancellationToken.None);
        }

        // Closing the consumer commits the offsets the sink's acknowledgements stored.
        await source.DisposeAsync();

        fixture.Committed(input, group).Should().Equal([3L], "Kafka resumes from the committed offset, the next one to read");

        var written = fixture.ConsumeRaw(output, 3, ReadTimeout);
        written.Select(r => r.Message.Key).Should().Equal("key-1", "key-2", "key-3");
        written.Select(r => Encoding.UTF8.GetString(r.Message.Headers.GetLastBytes("trace"))).Should().Equal("t-1", "t-2", "t-3");
    }

    [Fact]
    public async Task RequeuedMessage_HoldsTheCommit()
    {
        var topic = await fixture.CreateTopicAsync("requeue");
        var group = $"group-{Guid.NewGuid():N}";
        await fixture.ProduceRawAsync(topic, (null, """{"id":1}"""), (null, """{"id":2}"""), (null, """{"id":3}"""));

        var source = KafkaConnector.Source<TestMessage>(fixture.BootstrapServers, topic, group, o => o with { AutoOffsetReset = AutoOffsetReset.Earliest });
        var read = await source.OpenStream(new PipelineContext(), CancellationToken.None).TakeAsync(3, ReadTimeout);
        read.Should().HaveCount(3);

        await read[0].AcknowledgeAsync();
        await read[1].RejectAsync(true);
        await read[2].AcknowledgeAsync();
        await source.DisposeAsync();

        fixture.Committed(topic, group).Should().Equal([1L], "committing past the requeued message would lose it");
    }

    [Fact]
    public async Task UndeserializableMessage_WithSkip_IsSkipped_AndTheNextOneRead()
    {
        var topic = await fixture.CreateTopicAsync("skip");
        await fixture.ProduceRawAsync(topic, (null, "not json"), (null, """{"id":2,"customer":"c","total":1}"""));

        await using var source = KafkaConnector.Source<TestMessage>(fixture.BootstrapServers, topic, $"group-{Guid.NewGuid():N}",
            o => o with { AutoOffsetReset = AutoOffsetReset.Earliest, RowErrorHandler = _ => RowErrorAction.Skip });

        var read = await source.OpenStream(new PipelineContext(), CancellationToken.None).TakeAsync(1, ReadTimeout);

        read.Should().ContainSingle().Which.Body.Id.Should().Be(2);
        read[0].Offset.Should().Be(1);
    }

    [Fact]
    public async Task UndeserializableMessage_WithoutAHandler_FailsTheRead()
    {
        var topic = await fixture.CreateTopicAsync("fail");
        await fixture.ProduceRawAsync(topic, (null, "not json"));

        await using var source = KafkaConnector.Source<TestMessage>(fixture.BootstrapServers, topic, $"group-{Guid.NewGuid():N}",
            o => o with { AutoOffsetReset = AutoOffsetReset.Earliest });

        var act = () => source.OpenStream(new PipelineContext(), CancellationToken.None).TakeAsync(1, ReadTimeout);

        _ = await act.Should().ThrowAsync<RecordMappingException>();
    }

    [Fact]
    public async Task UndeserializableMessage_WithDeadLetter_GoesToTheKafkaDeadLetterTopic_WithItsOriginalValue()
    {
        var topic = await fixture.CreateTopicAsync("dead-letter-in");
        var deadLetterTopic = await fixture.CreateTopicAsync("dead-letter-out");
        await fixture.ProduceRawAsync(topic, ("key-1", "not json"), (null, """{"id":2,"customer":"c","total":1}"""));

        using var producer = new ProducerBuilder<byte[]?, byte[]>(new ProducerConfig { BootstrapServers = fixture.BootstrapServers }).Build();
        var context = new PipelineContext(new PipelineContextConfiguration(DeadLetterSink: new KafkaDeadLetterSink(producer, deadLetterTopic)));

        await using var source = KafkaConnector.Source<TestMessage>(fixture.BootstrapServers, topic, $"group-{Guid.NewGuid():N}",
            o => o with { AutoOffsetReset = AutoOffsetReset.Earliest, RowErrorHandler = _ => RowErrorAction.DeadLetter });

        var read = await source.OpenStream(context, CancellationToken.None).TakeAsync(1, ReadTimeout);
        read.Should().ContainSingle().Which.Body.Id.Should().Be(2);

        var dead = fixture.ConsumeRaw(deadLetterTopic, 1, ReadTimeout).Should().ContainSingle().Subject;
        dead.Message.Key.Should().Be("key-1");
        dead.Message.Value.Should().Be("not json");
        Encoding.UTF8.GetString(dead.Message.Headers.GetLastBytes("x-dead-letter-message-id")).Should().Be($"{topic}/0/0");
        Encoding.UTF8.GetString(dead.Message.Headers.GetLastBytes("x-dead-letter-source")).Should().Be(topic);
    }

    [Fact]
    public async Task Tombstones_AreSkipped_ByDefault_AndHandedOnWhenAsked()
    {
        var topic = await fixture.CreateTopicAsync("tombstone");
        await fixture.ProduceRawAsync(topic, ("key-1", null), ("key-2", """{"id":2,"customer":"c","total":1}"""));

        await using (var skipping = KafkaConnector.Source<TestMessage>(fixture.BootstrapServers, topic, $"group-{Guid.NewGuid():N}",
                         o => o with { AutoOffsetReset = AutoOffsetReset.Earliest }))
        {
            var read = await skipping.OpenStream(new PipelineContext(), CancellationToken.None).TakeAsync(1, ReadTimeout);
            read.Should().ContainSingle().Which.Key.Should().Be("key-2");
        }

        await using var keeping = KafkaConnector.Source<TestMessage>(fixture.BootstrapServers, topic, $"group-{Guid.NewGuid():N}",
            o => o with { AutoOffsetReset = AutoOffsetReset.Earliest, SkipTombstones = false });

        var all = await keeping.OpenStream(new PipelineContext(), CancellationToken.None).TakeAsync(2, ReadTimeout);
        all.Should().HaveCount(2);
        all[0].IsTombstone.Should().BeTrue();
        all[0].Key.Should().Be("key-1");
        all[0].Body.Should().BeNull();
        all[1].IsTombstone.Should().BeFalse();
    }

    [Fact]
    public async Task ExactlyOnce_WritesTheBatch_AndCommitsTheSourceOffsets_InOneTransaction()
    {
        var input = await fixture.CreateTopicAsync("eos-in");
        var output = await fixture.CreateTopicAsync("eos-out");
        var group = $"group-{Guid.NewGuid():N}";
        await fixture.ProduceRawAsync(input, ("k1", """{"id":1,"customer":"a","total":1}"""), ("k2", """{"id":2,"customer":"b","total":2}"""),
            ("k3", """{"id":3,"customer":"c","total":3}"""));

        // A commit interval longer than the test: only the transaction can move the group's offset while the source is open.
        await using var source = KafkaConnector.Source<TestMessage>(fixture.BootstrapServers, input, group,
            o => o with { AutoOffsetReset = AutoOffsetReset.Earliest, CommitInterval = TimeSpan.FromHours(1) });

        await using (var sink = KafkaConnector.Sink<TestMessage>(fixture.BootstrapServers, output,
                         o => o with
                         {
                             TransactionalId = $"tx-{Guid.NewGuid():N}",
                             BatchLinger = Timeout.InfiniteTimeSpan,
                         }))
        {
            var messages = source.OpenStream(new PipelineContext(), CancellationToken.None).Limit<KafkaMessage<TestMessage>, IAcknowledgableMessage<TestMessage>>(3);
            await sink.Acknowledging().ConsumeAsync(messages, new PipelineContext(), CancellationToken.None);
        }

        fixture.Committed(input, group).Should().Equal([3L], "the transaction committed the source offsets with the batch");

        var written = fixture.ConsumeRaw(output, 3, ReadTimeout, IsolationLevel.ReadCommitted);
        written.Select(r => r.Message.Key).Should().Equal("k1", "k2", "k3");
    }

    [Fact]
    public async Task FailedDelivery_WithFail_FailsTheWrite_AndSettlesOnlyTheDeliveredMessages()
    {
        var topic = await fixture.CreateTopicAsync("fail-delivery");
        var messages = new[] { Received("small-1"), Received(Oversized), Received("small-2") };

        await using var sink = KafkaConnector.Sink<string>(fixture.BootstrapServers, topic, o => o with { BatchLinger = Timeout.InfiniteTimeSpan });

        var act = () => sink.Acknowledging().ConsumeAsync(new InMemoryDataStream<IAcknowledgableMessage<string>>(messages), new PipelineContext(),
            CancellationToken.None);

        (await act.Should().ThrowAsync<ProduceException<byte[]?, byte[]>>()).Which.Error.Code.Should().Be(ErrorCode.MsgSizeTooLarge);

        messages[0].IsSettled.Should().BeTrue();
        messages[1].IsSettled.Should().BeFalse("it was not delivered, so it must be read again");
        messages[2].IsSettled.Should().BeTrue();
    }

    [Fact]
    public async Task FailedDelivery_WithRequeue_RejectsTheMessageWithRequeue_AndContinues()
    {
        var topic = await fixture.CreateTopicAsync("requeue-delivery");
        var requeued = new List<bool>();
        var messages = new[] { Received("small-1"), Received(Oversized, requeued), Received("small-2") };

        await using var sink = KafkaConnector.Sink<string>(fixture.BootstrapServers, topic, o => o with { FailedMessages = FailedMessageAction.Requeue });

        await sink.Acknowledging().ConsumeAsync(new InMemoryDataStream<IAcknowledgableMessage<string>>(messages), new PipelineContext(),
            CancellationToken.None);

        requeued.Should().Equal(true);
        messages.Should().OnlyContain(m => m.IsSettled);
        fixture.ConsumeRaw(topic, 2, ReadTimeout).Select(r => r.Message.Value).Should().Equal("\"small-1\"", "\"small-2\"");
    }

    [Fact]
    public async Task FailedDelivery_WithDeadLetter_SendsTheBodyToTheDeadLetterSink_AndAcknowledges()
    {
        var topic = await fixture.CreateTopicAsync("dead-letter-delivery");
        var deadLetters = new CapturingDeadLetterSink();
        var context = new PipelineContext(new PipelineContextConfiguration(DeadLetterSink: deadLetters));
        var requeued = new List<bool>();
        var messages = new[] { Received("small-1"), Received(Oversized, requeued) };

        await using var sink = KafkaConnector.Sink<string>(fixture.BootstrapServers, topic, o => o with { FailedMessages = FailedMessageAction.DeadLetter });

        await sink.Acknowledging().ConsumeAsync(new InMemoryDataStream<IAcknowledgableMessage<string>>(messages), context, CancellationToken.None);

        deadLetters.Captured.Should().ContainSingle().Which.Item.Should().Be(Oversized);
        messages.Should().OnlyContain(m => m.IsSettled);
        requeued.Should().BeEmpty("a dead-lettered message is acknowledged");
    }

    // Larger than librdkafka's default message.max.bytes, so the producer rejects it locally.
    private static readonly string Oversized = new('x', 1_100_000);

    private static KafkaMessage<string> Received(string body, List<bool>? rejected = null) =>
        new(body, new TopicPartitionOffset("upstream", 0, 0), null, DateTimeOffset.UnixEpoch, [], false,
            new MessageSettlement(_ => Task.CompletedTask, (requeue, _) =>
            {
                rejected?.Add(requeue);
                return Task.CompletedTask;
            }), null);

    private sealed class CapturingDeadLetterSink : IDeadLetterSink
    {
        public List<DeadLetterEnvelope> Captured { get; } = [];

        public Task HandleAsync(DeadLetterEnvelope envelope, PipelineContext context, CancellationToken cancellationToken)
        {
            lock (Captured)
            {
                Captured.Add(envelope);
            }

            return Task.CompletedTask;
        }
    }
}

/// <summary>The record the integration tests send.</summary>
public sealed record TestMessage(int Id, string Customer, decimal Total);

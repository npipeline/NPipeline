using System.Text;
using Confluent.Kafka;
using FakeItEasy;
using NPipeline.Connectors.Kafka.Models;
using NPipeline.Connectors.Messaging;

namespace NPipeline.Connectors.Kafka.Tests.Models;

/// <summary>
///     Unit tests for <see cref="KafkaMessage{T}" />.
/// </summary>
public sealed class KafkaMessageTests
{
    private static readonly DateTimeOffset ProducedAt = new(2026, 9, 30, 10, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Properties_DescribeThePositionAndTheRecord()
    {
        var headers = new Headers { { "trace", Encoding.UTF8.GetBytes("abc") } };
        var message = Message(new TestMessage { Id = 1, Name = "Test" }, headers: headers);

        message.Body.Id.Should().Be(1);
        message.Position.Should().Be(new TopicPartitionOffset("orders", 2, 100));
        message.Topic.Should().Be("orders");
        message.Partition.Should().Be(2);
        message.Offset.Should().Be(100);
        message.Key.Should().Be("order-1");
        message.Timestamp.Should().Be(ProducedAt);
        message.Headers.Should().BeSameAs(headers);
        message.IsTombstone.Should().BeFalse();
        message.IsSettled.Should().BeFalse();
    }

    [Fact]
    public void MessageId_IsTopicPartitionOffset()
    {
        Message("body").MessageId.Should().Be("orders/2/100");
    }

    [Fact]
    public void UntypedBody_IsTheBody()
    {
        IAcknowledgableMessage message = Message("body");

        message.Body.Should().Be("body");
    }

    [Fact]
    public void Metadata_HasThePositionKeyTimestampAndHeaders()
    {
        var headers = new Headers { { "trace", Encoding.UTF8.GetBytes("abc") }, { "tenant", Encoding.UTF8.GetBytes("acme") } };
        var message = Message("body", headers: headers);

        message.Metadata.Should().BeEquivalentTo(new Dictionary<string, object>
        {
            ["Topic"] = "orders",
            ["Partition"] = 2,
            ["Offset"] = 100L,
            ["Timestamp"] = ProducedAt,
            ["Key"] = "order-1",
            ["Header.trace"] = "abc",
            ["Header.tenant"] = "acme",
        });
    }

    [Fact]
    public void Metadata_WithoutAKey_HasNoKey()
    {
        Message("body", key: null).Metadata.Should().NotContainKey("Key");
    }

    [Fact]
    public async Task AcknowledgeAsync_SettlesOnce()
    {
        var acknowledged = 0;
        var rejected = 0;
        var message = Message("body", Settlement(() => acknowledged++, _ => rejected++));

        await message.AcknowledgeAsync();
        await message.AcknowledgeAsync();
        await message.RejectAsync(true);

        acknowledged.Should().Be(1);
        rejected.Should().Be(0, "the first settlement wins");
        message.IsSettled.Should().BeTrue();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RejectAsync_PassesRequeue(bool requeue)
    {
        var requeues = new List<bool>();
        var message = Message("body", Settlement(() => { }, requeues.Add));

        await message.RejectAsync(requeue);
        await message.AcknowledgeAsync();

        requeues.Should().Equal(requeue);
        message.IsSettled.Should().BeTrue();
    }

    [Fact]
    public async Task AcknowledgeAsync_WhenTheSettlementFails_ReportsTheFailure()
    {
        var message = Message("body", new MessageSettlement(_ => throw new InvalidOperationException("broker gone"), (_, _) => Task.CompletedTask));

        var act = () => message.AcknowledgeAsync();

        _ = await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("broker gone");
    }

    [Fact]
    public void WithBody_KeepsTheRecord_WithTheNewBody()
    {
        var headers = new Headers { { "trace", Encoding.UTF8.GetBytes("abc") } };
        var original = Message(new TestMessage { Id = 7, Name = "Seven" }, headers: headers);

        var mapped = original.WithBody(original.Body.Name);

        var kafka = mapped.Should().BeOfType<KafkaMessage<string>>().Subject;
        kafka.Body.Should().Be("Seven");
        kafka.Position.Should().Be(original.Position);
        kafka.Key.Should().Be(original.Key);
        kafka.Timestamp.Should().Be(original.Timestamp);
        kafka.Headers.Should().BeSameAs(headers);
        kafka.MessageId.Should().Be(original.MessageId);
        kafka.Metadata.Should().BeSameAs(original.Metadata, "the metadata is shared");
    }

    [Fact]
    public async Task WithBody_SharesSettlement()
    {
        var acknowledged = 0;
        var original = Message("body", Settlement(() => acknowledged++, _ => { }));
        var mapped = original.WithBody(42);

        await mapped.AcknowledgeAsync();
        await original.AcknowledgeAsync();
        await original.RejectAsync(false);

        acknowledged.Should().Be(1);
        original.IsSettled.Should().BeTrue("settling the copy settles the original");
        mapped.IsSettled.Should().BeTrue();
    }

    [Fact]
    public void Tombstone_IsReported()
    {
        var message = new KafkaMessage<string>(default!, new TopicPartitionOffset("orders", 0, 5), "deleted-key", ProducedAt, [], true,
            Settlement(() => { }, _ => { }), null);

        message.IsTombstone.Should().BeTrue();
        message.Body.Should().BeNull();
    }

    [Fact]
    public void GroupMetadata_ComesFromTheConsumer_WhenThereIsOne()
    {
        var group = A.Fake<IConsumerGroupMetadata>();
        IKafkaReceived withGroup = new KafkaMessage<string>("body", new TopicPartitionOffset("orders", 0, 1), null, ProducedAt, [], false,
            Settlement(() => { }, _ => { }), () => group);

        IKafkaReceived withoutGroup = Message("body");

        withGroup.GroupMetadata().Should().BeSameAs(group);
        withoutGroup.GroupMetadata().Should().BeNull();
    }

    private static KafkaMessage<T> Message<T>(T body, MessageSettlement? settlement = null, string? key = "order-1", Headers? headers = null) =>
        new(body, new TopicPartitionOffset("orders", 2, 100), key, ProducedAt, headers ?? [], false, settlement ?? Settlement(() => { }, _ => { }), null);

    private static MessageSettlement Settlement(Action acknowledge, Action<bool> reject) =>
        new(_ =>
            {
                acknowledge();
                return Task.CompletedTask;
            },
            (requeue, _) =>
            {
                reject(requeue);
                return Task.CompletedTask;
            });

    private sealed class TestMessage
    {
        public int Id { get; init; }

        public string Name { get; init; } = string.Empty;
    }
}

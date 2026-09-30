using System.Reflection;
using System.Text;
using NPipeline.Connectors.Messaging;
using NPipeline.Connectors.RabbitMQ.Models;
using RabbitMQ.Client;

namespace NPipeline.Connectors.RabbitMQ.Tests.Models;

public sealed class RabbitMqMessageTests
{
    [Fact]
    public void Constructor_Sets_Properties()
    {
        var properties = new BasicProperties { CorrelationId = "corr-1" };
        var message = Create("hello", "msg-1", properties: properties);

        message.Body.Should().Be("hello");
        ((IAcknowledgableMessage)message).Body.Should().Be("hello");
        message.MessageId.Should().Be("msg-1");
        message.Exchange.Should().Be("orders");
        message.RoutingKey.Should().Be("rk");
        message.DeliveryTag.Should().Be(7UL);
        message.Redelivered.Should().BeFalse();
        message.CorrelationId.Should().Be("corr-1");
        message.IsSettled.Should().BeFalse();
    }

    [Fact]
    public async Task AcknowledgeAsync_Calls_Callback()
    {
        var acks = 0;
        var message = Create("hello", "msg-1", new Settlement(onAck: () => acks++));

        await message.AcknowledgeAsync();

        acks.Should().Be(1);
        message.IsSettled.Should().BeTrue();
    }

    [Fact]
    public async Task AcknowledgeAsync_Is_Idempotent()
    {
        var acks = 0;
        var message = Create("hello", "msg-1", new Settlement(onAck: () => acks++));

        await message.AcknowledgeAsync();
        await message.AcknowledgeAsync();

        acks.Should().Be(1);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task RejectAsync_Calls_Callback_With_Requeue(bool requeue)
    {
        bool? requeued = null;
        var message = Create("hello", "msg-1", new Settlement(onReject: r => requeued = r));

        await message.RejectAsync(requeue);

        requeued.Should().Be(requeue);
        message.IsSettled.Should().BeTrue();
    }

    [Fact]
    public async Task RejectAsync_Is_Idempotent()
    {
        var rejects = 0;
        var message = Create("hello", "msg-1", new Settlement(onReject: _ => rejects++));

        await message.RejectAsync(true);
        await message.RejectAsync(false);

        rejects.Should().Be(1);
    }

    [Fact]
    public async Task AcknowledgeAsync_After_Reject_Is_A_NoOp()
    {
        var settlement = new Settlement();
        var message = Create("hello", "msg-1", settlement);

        await message.RejectAsync(true);
        await message.AcknowledgeAsync();

        settlement.Acks.Should().Be(0, "the first settlement wins");
        settlement.Rejects.Should().Be(1);
    }

    [Fact]
    public async Task RejectAsync_After_Ack_Is_A_NoOp()
    {
        var settlement = new Settlement();
        var message = Create("hello", "msg-1", settlement);

        await message.AcknowledgeAsync();
        await message.RejectAsync(true);

        settlement.Acks.Should().Be(1);
        settlement.Rejects.Should().Be(0, "the first settlement wins");
    }

    [Fact]
    public void WithBody_Projects_Body_And_Preserves_Message()
    {
        var message = Create("hello", "msg-1");

        var projected = message.WithBody(42);

        projected.Body.Should().Be(42);
        projected.MessageId.Should().Be("msg-1");
        projected.Metadata.Should().BeSameAs(message.Metadata);
        projected.Should().BeOfType<RabbitMqMessage<int>>().Which.DeliveryTag.Should().Be(7UL);
    }

    [Fact]
    public async Task WithBody_Copy_Shares_Settlement()
    {
        var settlement = new Settlement();
        var message = Create("hello", "msg-1", settlement);
        var projected = message.WithBody(42);

        await projected.AcknowledgeAsync();

        settlement.Acks.Should().Be(1);
        projected.IsSettled.Should().BeTrue();
        message.IsSettled.Should().BeTrue("settling the copy settles the original");

        await message.AcknowledgeAsync();
        await message.RejectAsync(true);
        await projected.RejectAsync(false);

        settlement.Acks.Should().Be(1, "a second settlement is a no-op");
        settlement.Rejects.Should().Be(0);
    }

    [Fact]
    public void Metadata_Includes_Delivery_Details_And_Headers()
    {
        var properties = new BasicProperties
        {
            CorrelationId = "corr-1",
            ContentType = "application/json",
            Headers = new Dictionary<string, object?>
            {
                ["tenant"] = Encoding.UTF8.GetBytes("acme"),
                ["attempt"] = 2,
                ["ignored"] = null,
            },
        };

        var message = Create("hello", "msg-1", properties: properties);

        message.Metadata["Exchange"].Should().Be("orders");
        message.Metadata["RoutingKey"].Should().Be("rk");
        message.Metadata["DeliveryTag"].Should().Be(7UL);
        message.Metadata["Redelivered"].Should().Be(false);
        message.Metadata["CorrelationId"].Should().Be("corr-1");
        message.Metadata["ContentType"].Should().Be("application/json");
        message.Metadata["Header.tenant"].Should().Be("acme", "AMQP carries string headers as bytes");
        message.Metadata["Header.attempt"].Should().Be(2);
        message.Metadata.Should().NotContainKey("Header.ignored");
    }

    [Fact]
    public async Task Concurrent_Ack_Attempts_Only_One_Succeeds()
    {
        var acks = 0;
        var message = Create("hello", "msg-1", new Settlement(onAck: () => Interlocked.Increment(ref acks)));

        var tasks = Enumerable.Range(0, 100)
            .Select(_ => Task.Run(() => message.AcknowledgeAsync()))
            .ToArray();

        await Task.WhenAll(tasks);

        acks.Should().Be(1);
        message.IsSettled.Should().BeTrue();
    }

    /// <summary>Messages are only created by the source, through an internal constructor, so the tests reach it by reflection.</summary>
    private static RabbitMqMessage<T> Create<T>(T body, string messageId, Settlement? settlement = null, BasicProperties? properties = null)
    {
        var constructor = typeof(RabbitMqMessage<T>)
            .GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic)
            .Single(c => c.GetParameters().Length == 8);

        return (RabbitMqMessage<T>)constructor.Invoke(
        [
            body, messageId, "orders", "rk", 7UL, false, properties ?? new BasicProperties(), (settlement ?? new Settlement()).State,
        ]);
    }

    private sealed class Settlement
    {
        public Settlement(Action? onAck = null, Action<bool>? onReject = null)
        {
            State = new MessageSettlement(
                _ =>
                {
                    Acks++;
                    onAck?.Invoke();
                    return Task.CompletedTask;
                },
                (requeue, _) =>
                {
                    Rejects++;
                    onReject?.Invoke(requeue);
                    return Task.CompletedTask;
                });
        }

        public MessageSettlement State { get; }

        public int Acks { get; private set; }

        public int Rejects { get; private set; }
    }
}

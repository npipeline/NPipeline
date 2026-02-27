using NPipeline.Connectors.RabbitMQ.Models;

namespace NPipeline.Connectors.RabbitMQ.Tests.Models;

public sealed class RabbitMqMessageTests
{
    [Fact]
    public void Constructor_Sets_Properties()
    {
        var message = new RabbitMqMessage<string>("hello", "msg-1");

        message.Body.Should().Be("hello");
        message.MessageId.Should().Be("msg-1");
        message.Exchange.Should().Be("");
        message.RoutingKey.Should().Be("");
        message.IsAcknowledged.Should().BeFalse();
    }

    [Fact]
    public async Task AcknowledgeAsync_Calls_Callback()
    {
        var ackCalled = false;

        var message = new RabbitMqMessage<string>(
            "hello", "msg-1",
            _ =>
            {
                ackCalled = true;
                return Task.CompletedTask;
            });

        await message.AcknowledgeAsync();

        ackCalled.Should().BeTrue();
        message.IsAcknowledged.Should().BeTrue();
    }

    [Fact]
    public async Task AcknowledgeAsync_Is_Idempotent()
    {
        var callCount = 0;

        var message = new RabbitMqMessage<string>(
            "hello", "msg-1",
            _ =>
            {
                callCount++;
                return Task.CompletedTask;
            });

        await message.AcknowledgeAsync();
        await message.AcknowledgeAsync();

        callCount.Should().Be(1);
    }

    [Fact]
    public async Task NegativeAcknowledgeAsync_Calls_Callback()
    {
        var nackCalled = false;
        var requeueValue = false;

        var message = new RabbitMqMessage<string>(
            "hello", "msg-1",
            nackCallback: (requeue, _) =>
            {
                nackCalled = true;
                requeueValue = requeue;
                return Task.CompletedTask;
            });

        await message.NegativeAcknowledgeAsync();

        nackCalled.Should().BeTrue();
        requeueValue.Should().BeTrue();
    }

    [Fact]
    public async Task NegativeAcknowledgeAsync_Is_Idempotent()
    {
        var callCount = 0;

        var message = new RabbitMqMessage<string>(
            "hello", "msg-1",
            nackCallback: (_, _) =>
            {
                callCount++;
                return Task.CompletedTask;
            });

        await message.NegativeAcknowledgeAsync();
        await message.NegativeAcknowledgeAsync();

        callCount.Should().Be(1);
    }

    [Fact]
    public async Task AcknowledgeAsync_After_Nack_Throws()
    {
        var message = new RabbitMqMessage<string>(
            "hello", "msg-1",
            nackCallback: (_, _) => Task.CompletedTask);

        await message.NegativeAcknowledgeAsync();

        var act = async () => await message.AcknowledgeAsync();

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*negatively acknowledged*");
    }

    [Fact]
    public async Task NegativeAcknowledgeAsync_After_Ack_Throws()
    {
        var message = new RabbitMqMessage<string>(
            "hello", "msg-1",
            _ => Task.CompletedTask);

        await message.AcknowledgeAsync();

        var act = async () => await message.NegativeAcknowledgeAsync();

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*already been acknowledged*");
    }

    [Fact]
    public void WithBody_Projects_Body_And_Preserves_MessageId()
    {
        var message = new RabbitMqMessage<string>("hello", "msg-1");

        var projected = message.WithBody(42);

        projected.Body.Should().Be(42);
        projected.MessageId.Should().Be("msg-1");
    }

    [Fact]
    public async Task WithBody_Projected_Message_Shares_Ack_Callback()
    {
        var ackCalled = false;

        var message = new RabbitMqMessage<string>(
            "hello", "msg-1",
            _ =>
            {
                ackCalled = true;
                return Task.CompletedTask;
            });

        var projected = message.WithBody(42);

        await projected.AcknowledgeAsync();

        ackCalled.Should().BeTrue();
        projected.IsAcknowledged.Should().BeTrue();
    }

    [Fact]
    public void Metadata_Returns_Dictionary()
    {
        var metadata = new Dictionary<string, object> { ["key"] = "value" };
        var message = new RabbitMqMessage<string>("hello", "msg-1", metadata: metadata);

        message.Metadata.Should().ContainKey("key");
        message.Metadata["key"].Should().Be("value");
    }

    [Fact]
    public async Task Concurrent_Ack_Attempts_Only_One_Succeeds()
    {
        var callCount = 0;

        var message = new RabbitMqMessage<string>(
            "hello", "msg-1",
            _ =>
            {
                Interlocked.Increment(ref callCount);
                return Task.CompletedTask;
            });

        var tasks = Enumerable.Range(0, 100)
            .Select(_ => message.AcknowledgeAsync())
            .ToArray();

        await Task.WhenAll(tasks);

        callCount.Should().Be(1);
        message.IsAcknowledged.Should().BeTrue();
    }
}

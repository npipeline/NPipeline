using System.Reflection;
using Azure.Messaging.ServiceBus;
using FakeItEasy;
using NPipeline.Connectors.Azure.ServiceBus.Models;
using NPipeline.Connectors.Messaging;

namespace NPipeline.Connectors.Azure.ServiceBus.Tests.Models;

public class ServiceBusMessageTests
{
    private static readonly DateTimeOffset Enqueued = new(2026, 9, 30, 10, 0, 0, TimeSpan.Zero);

    private readonly List<string> _settled = [];
    private readonly ServiceBusReceiver _receiver = A.Fake<ServiceBusReceiver>();

    private static ServiceBusReceivedMessage Received(string? sessionId = "session-1", string? correlationId = "corr-1", string? subject = "orders",
        IDictionary<string, object>? properties = null) =>
        ServiceBusModelFactory.ServiceBusReceivedMessage(
            BinaryData.FromString("{}"),
            "message-1",
            sessionId: sessionId,
            correlationId: correlationId,
            subject: subject,
            contentType: "application/json",
            properties: properties ?? new Dictionary<string, object> { ["tenant"] = "a", ["priority"] = 3 },
            sequenceNumber: 42,
            deliveryCount: 2,
            enqueuedTime: Enqueued);

    private MessageSettlement Settlement() =>
        new(_ =>
        {
            _settled.Add("ack");
            return Task.CompletedTask;
        }, (requeue, _) =>
        {
            _settled.Add(requeue ? "requeue" : "reject");
            return Task.CompletedTask;
        });

    private static ServiceBusMessage<T> Create<T>(T body, ServiceBusReceivedMessage received, ServiceBusReceiver receiver, MessageSettlement settlement)
    {
        var constructor = typeof(ServiceBusMessage<T>).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance).Single();
        return (ServiceBusMessage<T>)constructor.Invoke([body, received, receiver, settlement, null]);
    }

    private ServiceBusMessage<string> Create(ServiceBusReceivedMessage? received = null) => Create("body", received ?? Received(), _receiver, Settlement());

    [Fact]
    public void Properties_ComeFromTheReceivedMessage()
    {
        var received = Received();
        var message = Create(received);

        message.Body.Should().Be("body");
        ((IAcknowledgableMessage)message).Body.Should().Be("body");
        message.Received.Should().BeSameAs(received);
        message.MessageId.Should().Be("message-1");
        message.SessionId.Should().Be("session-1");
        message.CorrelationId.Should().Be("corr-1");
        message.Subject.Should().Be("orders");
        message.ContentType.Should().Be("application/json");
        message.DeliveryCount.Should().Be(2);
        message.EnqueuedTime.Should().Be(Enqueued);
        message.ApplicationProperties.Should().Contain("tenant", "a");
        message.IsSettled.Should().BeFalse();
    }

    [Fact]
    public void Metadata_HasTheBrokerPropertiesAndApplicationProperties()
    {
        var metadata = Create().Metadata;

        metadata.Should().Contain("SequenceNumber", 42L)
            .And.Contain("DeliveryCount", 2)
            .And.Contain("EnqueuedTime", Enqueued)
            .And.Contain("SessionId", "session-1")
            .And.Contain("CorrelationId", "corr-1")
            .And.Contain("Subject", "orders")
            .And.Contain("Property.tenant", "a")
            .And.Contain("Property.priority", 3);
    }

    [Fact]
    public void Metadata_LeavesOutMissingProperties()
    {
        var metadata = Create(Received(null, null, null, new Dictionary<string, object>())).Metadata;

        metadata.Keys.Should().BeEquivalentTo("SequenceNumber", "DeliveryCount", "EnqueuedTime");
    }

    [Fact]
    public void Metadata_IsBuiltOnce()
    {
        var message = Create();

        message.Metadata.Should().BeSameAs(message.Metadata);
    }

    [Fact]
    public async Task AcknowledgeAsync_SettlesTheMessage()
    {
        var message = Create();

        await message.AcknowledgeAsync();

        message.IsSettled.Should().BeTrue();
        _settled.Should().Equal("ack");
    }

    [Theory]
    [InlineData(true, "requeue")]
    [InlineData(false, "reject")]
    public async Task RejectAsync_PassesRequeue(bool requeue, string expected)
    {
        var message = Create();

        await message.RejectAsync(requeue);

        message.IsSettled.Should().BeTrue();
        _settled.Should().Equal(expected);
    }

    [Fact]
    public async Task FirstSettlement_Wins()
    {
        var message = Create();

        await message.AcknowledgeAsync();
        await message.RejectAsync(true);
        await message.AcknowledgeAsync();

        _settled.Should().Equal("ack");
    }

    [Fact]
    public async Task DeadLetterAsync_DeadLettersWithTheReceiverAndSettles()
    {
        var received = Received();
        var message = Create(received);

        await message.DeadLetterAsync("bad", "very bad");
        await message.AcknowledgeAsync();

        A.CallTo(() => _receiver.DeadLetterMessageAsync(received, "bad", "very bad", A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        message.IsSettled.Should().BeTrue();
        _settled.Should().BeEmpty("the dead-lettering settled the message first");
    }

    [Fact]
    public async Task DeferAsync_DefersWithTheReceiverAndSettles()
    {
        var received = Received();
        var message = Create(received);

        await message.DeferAsync();
        await message.DeadLetterAsync("late");

        A.CallTo(() => _receiver.DeferMessageAsync(received, A<IDictionary<string, object>>._, A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        A.CallTo(() => _receiver.DeadLetterMessageAsync(A<ServiceBusReceivedMessage>._, A<string>._, A<string>._, A<CancellationToken>._)).MustNotHaveHappened();
        message.IsSettled.Should().BeTrue();
    }

    [Fact]
    public async Task SettlementFailure_IsReported()
    {
        var failing = new MessageSettlement(_ => Task.FromException(new ServiceBusException("lost", ServiceBusFailureReason.MessageLockLost)),
            (_, _) => Task.CompletedTask);

        var message = Create("body", Received(), _receiver, failing);

        var acknowledge = () => message.AcknowledgeAsync();

        (await acknowledge.Should().ThrowAsync<ServiceBusException>()).Which.Reason.Should().Be(ServiceBusFailureReason.MessageLockLost);
    }

    public class WithBody
    {
        private readonly List<string> _settled = [];

        private ServiceBusMessage<string> Create()
        {
            var settlement = new MessageSettlement(_ =>
            {
                _settled.Add("ack");
                return Task.CompletedTask;
            }, (requeue, _) =>
            {
                _settled.Add(requeue ? "requeue" : "reject");
                return Task.CompletedTask;
            });

            return ServiceBusMessageTests.Create("body", Received(), A.Fake<ServiceBusReceiver>(), settlement);
        }

        [Fact]
        public void KeepsTheMessage()
        {
            var original = Create();

            var copy = original.WithBody(7);

            copy.Should().BeOfType<ServiceBusMessage<int>>();
            copy.Body.Should().Be(7);
            copy.MessageId.Should().Be(original.MessageId);
            copy.Metadata.Should().BeSameAs(original.Metadata);
            ((ServiceBusMessage<int>)copy).Received.Should().BeSameAs(original.Received);
        }

        [Fact]
        public async Task SettlingTheCopy_SettlesTheOriginal()
        {
            var original = Create();
            var copy = original.WithBody(7);

            await copy.AcknowledgeAsync();

            original.IsSettled.Should().BeTrue();
            copy.IsSettled.Should().BeTrue();
        }

        [Fact]
        public async Task SettlingBoth_SettlesOnce()
        {
            var original = Create();
            var copy = original.WithBody(7);

            await original.RejectAsync(true);
            await copy.AcknowledgeAsync();
            await ((ServiceBusMessage<int>)copy).DeadLetterAsync("x");

            _settled.Should().Equal("requeue");
        }
    }
}

using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Azure.ServiceBus.Models;

namespace NPipeline.Connectors.Azure.ServiceBus.Tests.Models;

public class ServiceBusMessageTests
{
    private static ServiceBusMessage<TestModel> CreateMessage(
        TestModel? body = null,
        string messageId = "test-message-id",
        Func<CancellationToken, Task>? completeCallback = null,
        Func<IDictionary<string, object>?, CancellationToken, Task>? abandonCallback = null,
        Func<string?, string?, CancellationToken, Task>? deadLetterCallback = null,
        Func<IDictionary<string, object>?, CancellationToken, Task>? deferCallback = null)
    {
        body ??= new TestModel { Id = 1, Name = "Test" };

        return new ServiceBusMessage<TestModel>(
            body,
            messageId,
            completeCallback,
            abandonCallback,
            deadLetterCallback,
            deferCallback);
    }

    public class Constructor
    {
        [Fact]
        public void Constructor_WithBody_InitializesProperties()
        {
            var body = new TestModel { Id = 42, Name = "Order" };
            var message = new ServiceBusMessage<TestModel>(body, "msg-1");

            message.Body.Should().BeSameAs(body);
            message.MessageId.Should().Be("msg-1");
            message.IsAcknowledged.Should().BeFalse();
            message.IsSettled.Should().BeFalse();
        }

        [Fact]
        public void Constructor_WithAllCallbacks_DoesNotInvokeCallbacksImmediately()
        {
            var completeCalled = false;
            var abandonCalled = false;

            var message = new ServiceBusMessage<TestModel>(
                new TestModel(),
                "msg-1",
                _ =>
                {
                    completeCalled = true;
                    return Task.CompletedTask;
                },
                (_, _) =>
                {
                    abandonCalled = true;
                    return Task.CompletedTask;
                });

            completeCalled.Should().BeFalse();
            abandonCalled.Should().BeFalse();
        }

        [Fact]
        public void Constructor_WithNoCallbacks_SetsDefaultNoopCallbacks()
        {
            var message = new ServiceBusMessage<TestModel>(new TestModel(), "msg-1");

            // Should not throw when calling settle methods (uses no-op callbacks)
            message.Invoking(m => m.CompleteAsync()).Should().NotThrowAsync();
        }

        [Fact]
        public void Constructor_WithApplicationProperties_ExposesThemViaProperty()
        {
            var props = new Dictionary<string, object> { ["key"] = "value" };

            var message = new ServiceBusMessage<TestModel>(
                new TestModel(), "msg-1",
                applicationProperties: props);

            message.ApplicationProperties.Should().ContainKey("key");
            message.ApplicationProperties["key"].Should().Be("value");
        }
    }

    public class CompleteAsync
    {
        [Fact]
        public async Task CompleteAsync_CallsCompleteCallback()
        {
            var completeCalled = false;

            var message = CreateMessage(completeCallback: _ =>
            {
                completeCalled = true;
                return Task.CompletedTask;
            });

            await message.CompleteAsync();

            completeCalled.Should().BeTrue();
        }

        [Fact]
        public async Task CompleteAsync_SetsIsAcknowledged()
        {
            var message = CreateMessage();
            await message.CompleteAsync();
            message.IsAcknowledged.Should().BeTrue();
        }

        [Fact]
        public async Task CompleteAsync_SetsIsSettled()
        {
            var message = CreateMessage();
            await message.CompleteAsync();
            message.IsSettled.Should().BeTrue();
        }

        [Fact]
        public async Task CompleteAsync_CalledTwice_InvokesCallbackOnce()
        {
            var callCount = 0;

            var message = CreateMessage(completeCallback: _ =>
            {
                Interlocked.Increment(ref callCount);
                return Task.CompletedTask;
            });

            await message.CompleteAsync();
            await message.CompleteAsync();

            callCount.Should().Be(1);
        }

        [Fact]
        public async Task CompleteAsync_CalledConcurrently_InvokesCallbackOnce()
        {
            var callCount = 0;

            var message = CreateMessage(completeCallback: async _ =>
            {
                Interlocked.Increment(ref callCount);
                await Task.Yield();
            });

            await Task.WhenAll(Enumerable.Range(0, 50).Select(_ => message.CompleteAsync()));

            callCount.Should().Be(1);
        }
    }

    public class AbandonAsync
    {
        [Fact]
        public async Task AbandonAsync_CallsAbandonCallback()
        {
            var abandonCalled = false;

            var message = CreateMessage(abandonCallback: (_, _) =>
            {
                abandonCalled = true;
                return Task.CompletedTask;
            });

            await message.AbandonAsync();

            abandonCalled.Should().BeTrue();
        }

        [Fact]
        public async Task AbandonAsync_SetsIsSettled()
        {
            var message = CreateMessage();
            await message.AbandonAsync();
            message.IsSettled.Should().BeTrue();
        }

        [Fact]
        public async Task AbandonAsync_WithProperties_PassesPropertiesToCallback()
        {
            IDictionary<string, object>? capturedProps = null;

            var message = CreateMessage(abandonCallback: (props, _) =>
            {
                capturedProps = props;
                return Task.CompletedTask;
            });

            var props = new Dictionary<string, object> { ["key"] = "value" };

            await message.AbandonAsync(props);

            capturedProps.Should().NotBeNull();
            capturedProps!["key"].Should().Be("value");
        }

        [Fact]
        public async Task AbandonAsync_CalledAfterComplete_IsNoOp()
        {
            var callCount = 0;

            var message = CreateMessage(
                completeCallback: _ =>
                {
                    Interlocked.Increment(ref callCount);
                    return Task.CompletedTask;
                },
                abandonCallback: (_, _) =>
                {
                    Interlocked.Increment(ref callCount);
                    return Task.CompletedTask;
                });

            await message.CompleteAsync();
            await message.AbandonAsync();

            callCount.Should().Be(1); // Only complete callback was called
        }
    }

    public class DeadLetterAsync
    {
        [Fact]
        public async Task DeadLetterAsync_CallsDeadLetterCallback()
        {
            var deadLetterCalled = false;

            var message = CreateMessage(deadLetterCallback: (_, _, _) =>
            {
                deadLetterCalled = true;
                return Task.CompletedTask;
            });

            await message.DeadLetterAsync();

            deadLetterCalled.Should().BeTrue();
        }

        [Fact]
        public async Task DeadLetterAsync_WithReasonAndDescription_PassesToCallback()
        {
            string? capturedReason = null;
            string? capturedDesc = null;

            var message = CreateMessage(deadLetterCallback: (reason, desc, _) =>
            {
                capturedReason = reason;
                capturedDesc = desc;
                return Task.CompletedTask;
            });

            await message.DeadLetterAsync("TestReason", "TestDescription");

            capturedReason.Should().Be("TestReason");
            capturedDesc.Should().Be("TestDescription");
        }

        [Fact]
        public async Task DeadLetterAsync_SetsIsSettled()
        {
            var message = CreateMessage();
            await message.DeadLetterAsync();
            message.IsSettled.Should().BeTrue();
        }
    }

    public class DeferAsync
    {
        [Fact]
        public async Task DeferAsync_CallsDeferCallback()
        {
            var deferCalled = false;

            var message = CreateMessage(deferCallback: (_, _) =>
            {
                deferCalled = true;
                return Task.CompletedTask;
            });

            await message.DeferAsync();

            deferCalled.Should().BeTrue();
        }

        [Fact]
        public async Task DeferAsync_SetsIsSettled()
        {
            var message = CreateMessage();
            await message.DeferAsync();
            message.IsSettled.Should().BeTrue();
        }
    }

    public class AcknowledgeAsync
    {
        [Fact]
        public async Task AcknowledgeAsync_DelegatesToCompleteAsync()
        {
            var completeCalled = false;

            var message = CreateMessage(completeCallback: _ =>
            {
                completeCalled = true;
                return Task.CompletedTask;
            });

            await message.AcknowledgeAsync();

            completeCalled.Should().BeTrue();
        }

        [Fact]
        public async Task AcknowledgeAsync_SetsIsAcknowledged()
        {
            var message = CreateMessage();
            await message.AcknowledgeAsync();
            message.IsAcknowledged.Should().BeTrue();
        }
    }

    public class NegativeAcknowledgeAsync
    {
        [Fact]
        public async Task NegativeAcknowledgeAsync_WhenRequeue_CallsAbandonCallback()
        {
            var abandonCalled = false;

            var message = CreateMessage(abandonCallback: (_, _) =>
            {
                abandonCalled = true;
                return Task.CompletedTask;
            });

            await message.NegativeAcknowledgeAsync(true);

            abandonCalled.Should().BeTrue();
        }

        [Fact]
        public async Task NegativeAcknowledgeAsync_WhenNoRequeue_CallsDeadLetterCallback()
        {
            var deadLetterCalled = false;

            var message = CreateMessage(deadLetterCallback: (_, _, _) =>
            {
                deadLetterCalled = true;
                return Task.CompletedTask;
            });

            await message.NegativeAcknowledgeAsync(false);

            deadLetterCalled.Should().BeTrue();
        }

        [Fact]
        public async Task NegativeAcknowledgeAsync_DefaultIsRequeue()
        {
            var abandonCalled = false;

            var message = CreateMessage(abandonCallback: (_, _) =>
            {
                abandonCalled = true;
                return Task.CompletedTask;
            });

            await message.NegativeAcknowledgeAsync();

            abandonCalled.Should().BeTrue();
        }
    }

    public class WithBody
    {
        [Fact]
        public void WithBody_ReturnsNewMessageWithNewBody()
        {
            var message = CreateMessage();
            var newBody = new OtherModel { Value = "new" };

            var newMessage = message.WithBody(newBody);

            newMessage.Should().NotBeSameAs(message);
            newMessage.Body.Should().BeSameAs(newBody);
        }

        [Fact]
        public void WithBody_PreservesMessageId()
        {
            var message = CreateMessage(messageId: "original-id");
            var newMessage = message.WithBody(new OtherModel());

            newMessage.MessageId.Should().Be("original-id");
        }

        [Fact]
        public async Task WithBody_NewMessageSharesCallbacks()
        {
            var completeCalled = false;

            var message = CreateMessage(completeCallback: _ =>
            {
                completeCalled = true;
                return Task.CompletedTask;
            });

            var newMessage = message.WithBody(new OtherModel());
            await newMessage.AcknowledgeAsync();

            completeCalled.Should().BeTrue();
        }

        [Fact]
        public void WithBody_NewMessageIsAssignableToInterface()
        {
            var message = CreateMessage();
            var newMessage = message.WithBody(new OtherModel());

            newMessage.Should().BeAssignableTo<IAcknowledgableMessage<OtherModel>>();
        }
    }

    public class SettlementIdempotency
    {
        [Fact]
        public async Task AfterAnySettlement_AllSubsequentSettlementAttemptsAreNoOps()
        {
            var callCount = 0;

            var message = new ServiceBusMessage<TestModel>(
                new TestModel(), "msg-1",
                _ =>
                {
                    Interlocked.Increment(ref callCount);
                    return Task.CompletedTask;
                },
                (_, _) =>
                {
                    Interlocked.Increment(ref callCount);
                    return Task.CompletedTask;
                },
                (_, _, _) =>
                {
                    Interlocked.Increment(ref callCount);
                    return Task.CompletedTask;
                },
                (_, _) =>
                {
                    Interlocked.Increment(ref callCount);
                    return Task.CompletedTask;
                });

            await message.CompleteAsync();
            await message.AbandonAsync();
            await message.DeadLetterAsync();
            await message.DeferAsync();
            await message.AcknowledgeAsync();
            await message.NegativeAcknowledgeAsync();

            callCount.Should().Be(1);
        }
    }

    private class TestModel
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class OtherModel
    {
        public string Value { get; set; } = string.Empty;
    }
}

using FakeItEasy;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.RabbitMQ.Configuration;
using NPipeline.Connectors.RabbitMQ.Connection;
using NPipeline.Connectors.RabbitMQ.Nodes;
using NPipeline.Connectors.Serialization;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Pipeline;
using RabbitMQ.Client;

// Tests skipped with a defect ID pin known bugs; the phase that fixes each one removes its skip.
#pragma warning disable xUnit1004

namespace NPipeline.Connectors.RabbitMQ.Tests.Reliability.Behavior;

/// <summary>
///     Behavior tests for the RabbitMQ sink's publish retry loop. IDs refer to the defect register in
///     <c>plans/resilience-improvements.md</c>.
/// </summary>
public sealed class RabbitMqSinkRetryBehaviorTests
{
    private const string Q1 = "Q1 (Phase 1): an acknowledgement failure re-publishes the message";

    [Fact(Skip = Q1)]
    public async Task AcknowledgementFailure_AfterASuccessfulPublish_DoesNotRepublish()
    {
        var channel = A.Fake<IChannel>();
        var connectionManager = A.Fake<IRabbitMqConnectionManager>();
        A.CallTo(() => connectionManager.GetPooledChannelAsync(A<CancellationToken>._)).Returns(channel);

        // The source message's acknowledgement fails after the publish to the exchange has succeeded.
        var sourceMessage = A.Fake<IAcknowledgableMessage>();
        A.CallTo(() => sourceMessage.Body).Returns("order-1");
        A.CallTo(() => sourceMessage.IsAcknowledged).Returns(false);
        A.CallTo(() => sourceMessage.AcknowledgeAsync(A<CancellationToken>._)).ThrowsAsync(new InvalidOperationException("ack failed"));

        var options = new RabbitMqSinkOptions { ExchangeName = "orders", MaxRetries = 3, RetryBaseDelayMs = 1 };
        var sink = new RabbitMqSinkNode<IAcknowledgableMessage>(options, connectionManager, A.Fake<IMessageSerializer>());

        await using var input = new InMemoryDataStream<IAcknowledgableMessage>([sourceMessage]);

        try
        {
            await sink.ConsumeAsync(input, new PipelineContext(), CancellationToken.None);
        }
        catch (InvalidOperationException)
        {
            // Whether the acknowledgement failure surfaces is not what this test is about.
        }

        var publishes = Fake.GetCalls(channel).Count(call => call.Method.Name == nameof(IChannel.BasicPublishAsync));
        publishes.Should().Be(1, "the message already reached the exchange; publishing it again duplicates it");
    }
}

using NPipeline.Connectors.Messaging;
using NPipeline.Connectors.RabbitMQ.Configuration;
using NPipeline.Connectors.RabbitMQ.Connection;
using NPipeline.Connectors.RabbitMQ.Models;
using NPipeline.Pipeline;
using RabbitMQ.Client;

namespace NPipeline.Connectors.RabbitMQ.Tests.Integration;

[Collection("RabbitMQ")]
public sealed class RabbitMqSourceNodeIntegrationTests : IAsyncDisposable
{
    private readonly IRabbitMqConnectionManager _connectionManager;

    public RabbitMqSourceNodeIntegrationTests(RabbitMqContainerFixture fixture)
    {
        _connectionManager = fixture.Connect();
    }

    public async ValueTask DisposeAsync()
    {
        await _connectionManager.DisposeAsync();
    }

    [Fact]
    public async Task SourceNode_Consumes_Published_Messages()
    {
        // Arrange
        var queueName = $"test-source-{Guid.NewGuid():N}";
        await PublishAsync(queueName, true, Enumerable.Range(0, 5).Select(i => new TestMessage($"Message-{i}", i)));

        // Act - consume the messages
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var sourceNode = RabbitMqConnector.Source<TestMessage>(_connectionManager, queueName, o => o with { PrefetchCount = 10 });

        var pipe = sourceNode.OpenStream(new PipelineContext(), cts.Token);
        var consumed = new List<RabbitMqMessage<TestMessage>>();

        await foreach (var msg in pipe.WithCancellation(cts.Token))
        {
            consumed.Add(msg);
            await msg.AcknowledgeAsync(cts.Token);

            if (consumed.Count >= 5)
                break;
        }

        // Assert
        consumed.Select(m => m.Body.Name).Should().BeEquivalentTo(Enumerable.Range(0, 5).Select(i => $"Message-{i}"));
        consumed.Should().AllSatisfy(m => m.RoutingKey.Should().Be(queueName));
    }

    [Fact]
    public async Task SourceNode_Acknowledges_Messages()
    {
        // Arrange
        var queueName = $"test-ack-{Guid.NewGuid():N}";
        await PublishAsync(queueName, false, [new TestMessage("ack-test", 1)]);

        // Consume and ack
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await using (var sourceNode = RabbitMqConnector.Source<TestMessage>(_connectionManager, queueName, o => o with { PrefetchCount = 10 }))
        {
            var pipe = sourceNode.OpenStream(new PipelineContext(), cts.Token);

            await foreach (var msg in pipe.WithCancellation(cts.Token))
            {
                msg.IsSettled.Should().BeFalse();
                await msg.AcknowledgeAsync(cts.Token);
                msg.IsSettled.Should().BeTrue();
                break;
            }
        }

        // Verify queue is empty (message was acked)
        var connection = await _connectionManager.GetConnectionAsync();
        await using var checkChannel = await connection.CreateChannelAsync();
        var result = await checkChannel.BasicGetAsync(queueName, true);
        result.Should().BeNull();
        await checkChannel.CloseAsync();
    }

    [Fact]
    public async Task SourceNode_Skips_A_Message_That_Does_Not_Deserialize()
    {
        var queueName = $"test-skip-{Guid.NewGuid():N}";
        var connection = await _connectionManager.GetConnectionAsync();

        await using (var channel = await connection.CreateChannelAsync(new CreateChannelOptions(true, true)))
        {
            _ = await channel.QueueDeclareAsync(queueName, true, false, false);
            await channel.BasicPublishAsync("", queueName, false, new BasicProperties(), "not json"u8.ToArray());
            await channel.BasicPublishAsync("", queueName, false, new BasicProperties(), Serialize(new TestMessage("good", 2)));
            await channel.CloseAsync();
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await using var sourceNode = RabbitMqConnector.Source<TestMessage>(_connectionManager, queueName,
            o => o with { RowErrorHandler = _ => NPipeline.Connectors.Errors.RowErrorAction.Skip });

        await foreach (var msg in sourceNode.OpenStream(new PipelineContext(), cts.Token).WithCancellation(cts.Token))
        {
            msg.Body.Name.Should().Be("good");
            await msg.AcknowledgeAsync(cts.Token);
            break;
        }

        await using var checkChannel = await connection.CreateChannelAsync();
        (await checkChannel.BasicGetAsync(queueName, true)).Should().BeNull("the undeserializable message was rejected and the good one acknowledged");
        await checkChannel.CloseAsync();
    }

    private async Task PublishAsync(string queueName, bool autoDelete, IEnumerable<TestMessage> messages)
    {
        var connection = await _connectionManager.GetConnectionAsync();
        await using var channel = await connection.CreateChannelAsync(new CreateChannelOptions(true, true));
        _ = await channel.QueueDeclareAsync(queueName, true, false, autoDelete);

        foreach (var message in messages)
        {
            await channel.BasicPublishAsync("", queueName, false, new BasicProperties(), Serialize(message));
        }

        await channel.CloseAsync();
    }

    private static byte[] Serialize(TestMessage message) => JsonMessageSerializer.Default.Serialize(message, new MessageContext("test"));

    private sealed record TestMessage(string Name, int Value);
}

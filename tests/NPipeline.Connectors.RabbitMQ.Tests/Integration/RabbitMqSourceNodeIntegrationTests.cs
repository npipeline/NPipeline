using Microsoft.Extensions.Logging.Abstractions;
using NPipeline.Connectors.RabbitMQ.Configuration;
using NPipeline.Connectors.RabbitMQ.Connection;
using NPipeline.Connectors.RabbitMQ.Models;
using NPipeline.Connectors.RabbitMQ.Nodes;
using NPipeline.Connectors.RabbitMQ.Serialization;
using NPipeline.Pipeline;
using RabbitMQ.Client;

namespace NPipeline.Connectors.RabbitMQ.Tests.Integration;

[Collection("RabbitMQ")]
public sealed class RabbitMqSourceNodeIntegrationTests : IAsyncDisposable
{
    private readonly RabbitMqConnectionManager _connectionManager;
    private readonly RabbitMqContainerFixture _fixture;

    public RabbitMqSourceNodeIntegrationTests(RabbitMqContainerFixture fixture)
    {
        _fixture = fixture;

        var connectionOptions = new RabbitMqConnectionOptions
        {
            HostName = _fixture.HostName,
            Port = _fixture.Port,
            UserName = RabbitMqContainerFixture.TestUsername,
            Password = RabbitMqContainerFixture.TestPassword,
        };

        _connectionManager = new RabbitMqConnectionManager(
            connectionOptions,
            NullLogger<RabbitMqConnectionManager>.Instance);
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
        var serializer = new RabbitMqJsonSerializer();

        var sourceOptions = new RabbitMqSourceOptions
        {
            QueueName = queueName,
            PrefetchCount = 10,
        };

        // Publish some messages first
        var connection = await _connectionManager.GetConnectionAsync();

        var pubChannel = await connection.CreateChannelAsync(
            new CreateChannelOptions(true, true));

        await pubChannel.QueueDeclareAsync(queueName, false, false, true);

        for (var i = 0; i < 5; i++)
        {
            var body = serializer.Serialize(new TestMessage($"Message-{i}", i));
            await pubChannel.BasicPublishAsync("", queueName, false, new BasicProperties(), body);
        }

        await pubChannel.CloseAsync();

        // Act - consume the messages
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var sourceNode = new RabbitMqSourceNode<TestMessage>(
            sourceOptions, _connectionManager, serializer,
            logger: NullLogger<RabbitMqSourceNode<TestMessage>>.Instance);

        var pipe = sourceNode.Initialize(new PipelineContext(), cts.Token);

        var consumed = new List<RabbitMqMessage<TestMessage>>();

        await foreach (var msg in pipe.WithCancellation(cts.Token))
        {
            consumed.Add(msg);
            await msg.AcknowledgeAsync(cts.Token);

            if (consumed.Count >= 5)
                break;
        }

        // Assert
        consumed.Should().HaveCount(5);

        consumed.Select(m => m.Body.Name).Should().BeEquivalentTo(
            Enumerable.Range(0, 5).Select(i => $"Message-{i}"));

        await sourceNode.DisposeAsync();
    }

    [Fact]
    public async Task SourceNode_Acknowledges_Messages()
    {
        // Arrange
        var queueName = $"test-ack-{Guid.NewGuid():N}";
        var serializer = new RabbitMqJsonSerializer();

        var sourceOptions = new RabbitMqSourceOptions
        {
            QueueName = queueName,
            PrefetchCount = 10,
        };

        // Publish a message
        var connection = await _connectionManager.GetConnectionAsync();

        var pubChannel = await connection.CreateChannelAsync(
            new CreateChannelOptions(true, true));

        await pubChannel.QueueDeclareAsync(queueName, false, false, false);

        var body = serializer.Serialize(new TestMessage("ack-test", 1));
        await pubChannel.BasicPublishAsync("", queueName, false, new BasicProperties(), body);
        await pubChannel.CloseAsync();

        // Consume and ack
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var sourceNode = new RabbitMqSourceNode<TestMessage>(
            sourceOptions, _connectionManager, serializer,
            logger: NullLogger<RabbitMqSourceNode<TestMessage>>.Instance);

        var pipe = sourceNode.Initialize(new PipelineContext(), cts.Token);

        await foreach (var msg in pipe.WithCancellation(cts.Token))
        {
            msg.IsAcknowledged.Should().BeFalse();
            await msg.AcknowledgeAsync(cts.Token);
            msg.IsAcknowledged.Should().BeTrue();
            break;
        }

        await sourceNode.DisposeAsync();

        // Verify queue is empty (message was acked)
        var checkChannel = await connection.CreateChannelAsync();
        var result = await checkChannel.BasicGetAsync(queueName, true);
        result.Should().BeNull();
        await checkChannel.CloseAsync();
    }

    private sealed record TestMessage(string Name, int Value);
}

using Microsoft.Extensions.Logging.Abstractions;
using NPipeline.Connectors.RabbitMQ.Configuration;
using NPipeline.Connectors.RabbitMQ.Connection;
using NPipeline.Connectors.RabbitMQ.Nodes;
using NPipeline.Connectors.RabbitMQ.Serialization;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Pipeline;
using RabbitMQ.Client;

namespace NPipeline.Connectors.RabbitMQ.Tests.Integration;

[Collection("RabbitMQ")]
public sealed class RabbitMqSinkNodeIntegrationTests : IAsyncDisposable
{
    private readonly RabbitMqConnectionManager _connectionManager;
    private readonly RabbitMqContainerFixture _fixture;

    public RabbitMqSinkNodeIntegrationTests(RabbitMqContainerFixture fixture)
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
    public async Task SinkNode_Publishes_Messages_To_Queue()
    {
        // Arrange
        var queueName = $"test-sink-{Guid.NewGuid():N}";
        var serializer = new RabbitMqJsonSerializer();

        // Declare queue
        var connection = await _connectionManager.GetConnectionAsync();
        var setupChannel = await connection.CreateChannelAsync();
        await setupChannel.QueueDeclareAsync(queueName, false, false, true);
        await setupChannel.CloseAsync();

        var sinkOptions = new RabbitMqSinkOptions
        {
            ExchangeName = "", // Default exchange routes to queue by routing key
            RoutingKey = queueName,
        };

        var items = Enumerable.Range(0, 3)
            .Select(i => new TestMessage($"Sink-{i}", i))
            .ToArray();

        var sinkNode = new RabbitMqSinkNode<TestMessage>(
            sinkOptions, _connectionManager, serializer,
            logger: NullLogger<RabbitMqSinkNode<TestMessage>>.Instance);

        // Act
        var pipe = CreateDataStream(items);
        await sinkNode.ConsumeAsync(pipe, new PipelineContext(), CancellationToken.None);

        // Assert - consume and verify
        var consumeChannel = await connection.CreateChannelAsync();

        for (var i = 0; i < 3; i++)
        {
            var result = await consumeChannel.BasicGetAsync(queueName, true);
            result.Should().NotBeNull();
            var msg = serializer.Deserialize<TestMessage>(result!.Body);
            msg.Name.Should().StartWith("Sink-");
        }

        await consumeChannel.CloseAsync();
    }

    [Fact]
    public async Task SinkNode_Publishes_With_Routing_Key_Selector()
    {
        // Arrange
        var queueName = $"test-sink-rk-{Guid.NewGuid():N}";
        var serializer = new RabbitMqJsonSerializer();

        var connection = await _connectionManager.GetConnectionAsync();
        var setupChannel = await connection.CreateChannelAsync();
        await setupChannel.QueueDeclareAsync(queueName, false, false, true);
        await setupChannel.CloseAsync();

        var sinkOptions = new RabbitMqSinkOptions
        {
            ExchangeName = "",
            RoutingKeySelector = obj =>
            {
                if (obj is TestMessage msg)
                    return queueName;

                return queueName;
            },
        };

        var items = new[] { new TestMessage("routed", 99) };

        var sinkNode = new RabbitMqSinkNode<TestMessage>(
            sinkOptions, _connectionManager, serializer,
            logger: NullLogger<RabbitMqSinkNode<TestMessage>>.Instance);

        // Act
        await sinkNode.ConsumeAsync(CreateDataStream(items), new PipelineContext(), CancellationToken.None);

        // Assert
        var consumeChannel = await connection.CreateChannelAsync();
        var result = await consumeChannel.BasicGetAsync(queueName, true);
        result.Should().NotBeNull();
        var msg = serializer.Deserialize<TestMessage>(result!.Body);
        msg.Name.Should().Be("routed");

        await consumeChannel.CloseAsync();
    }

    private static IDataStream<T> CreateDataStream<T>(IEnumerable<T> items)
    {
        async IAsyncEnumerable<T> Enumerate()
        {
            foreach (var item in items)
            {
                yield return item;

                await Task.Yield();
            }
        }

        return new DataStream<T>(Enumerate(), "test-pipe");
    }

    private sealed record TestMessage(string Name, int Value);
}

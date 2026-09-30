using System.Runtime.CompilerServices;
using NPipeline.Connectors.Messaging;
using NPipeline.Connectors.RabbitMQ.Connection;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Pipeline;
using RabbitMQ.Client;

namespace NPipeline.Connectors.RabbitMQ.Tests.Integration;

[Collection("RabbitMQ")]
public sealed class RabbitMqSinkNodeIntegrationTests : IAsyncDisposable
{
    private readonly IRabbitMqConnectionManager _connectionManager;

    public RabbitMqSinkNodeIntegrationTests(RabbitMqContainerFixture fixture)
    {
        _connectionManager = fixture.Connect();
    }

    public async ValueTask DisposeAsync()
    {
        await _connectionManager.DisposeAsync();
    }

    [Fact]
    public async Task ConnectionManager_PoolsChannelsByConfirmMode()
    {
        var unconfirmed = await _connectionManager.GetPooledChannelAsync(false);
        (await unconfirmed.GetNextPublishSequenceNumberAsync()).Should().Be(0, "a channel without confirms tracks no sequence numbers");
        _connectionManager.ReturnChannel(unconfirmed);

        var confirmed = await _connectionManager.GetPooledChannelAsync(true);
        confirmed.Should().NotBeSameAs(unconfirmed, "a node that wants confirms must never get a channel without them");
        (await confirmed.GetNextPublishSequenceNumberAsync()).Should().BeGreaterThan(0);
        _connectionManager.ReturnChannel(confirmed);

        (await _connectionManager.GetPooledChannelAsync(false)).Should().BeSameAs(unconfirmed);
        (await _connectionManager.GetPooledChannelAsync(true)).Should().BeSameAs(confirmed);
    }

    [Fact]
    public async Task SinkNode_WithConfirmsOff_Publishes_Messages_To_Queue()
    {
        var queueName = await DeclareQueueAsync("test-sink-noconfirm");
        var items = Enumerable.Range(0, 3).Select(i => new TestMessage($"Sink-{i}", i)).ToArray();

        var sinkNode = RabbitMqConnector.Sink<TestMessage>(_connectionManager, "", queueName, o => o with { PublisherConfirms = false });
        await sinkNode.ConsumeAsync(CreateDataStream(items), new PipelineContext(), CancellationToken.None);

        // Without confirms the publish returns before the broker routes the message, so allow it a moment.
        (await ReceiveAsync(queueName, 3)).Should().HaveCount(3);
    }

    [Fact]
    public async Task SinkNode_Publishes_Messages_To_Queue()
    {
        var queueName = await DeclareQueueAsync("test-sink");
        var items = Enumerable.Range(0, 3).Select(i => new TestMessage($"Sink-{i}", i)).ToArray();

        var sinkNode = RabbitMqConnector.Sink<TestMessage>(_connectionManager, "", queueName);
        await sinkNode.ConsumeAsync(CreateDataStream(items), new PipelineContext(), CancellationToken.None);

        var received = await ReceiveAsync(queueName, 3);
        received.Select(r => r.Name).Should().Equal("Sink-0", "Sink-1", "Sink-2");
    }

    [Fact]
    public async Task SinkNode_Publishes_With_Routing_Key_Selector()
    {
        var queueName = await DeclareQueueAsync("test-sink-rk");
        var items = new[] { new TestMessage("routed", 99) };

        var sinkNode = RabbitMqConnector.Sink<TestMessage>(_connectionManager, "", "unused", o => o with { RoutingKeySelector = _ => queueName });
        await sinkNode.ConsumeAsync(CreateDataStream(items), new PipelineContext(), CancellationToken.None);

        (await ReceiveAsync(queueName, 1)).Should().ContainSingle().Which.Name.Should().Be("routed");
    }

    [Fact]
    public async Task Source_Feeding_The_Sink_Through_Acknowledging_Leaves_The_Source_Queue_Empty()
    {
        const int count = 5;
        var sourceQueue = await DeclareQueueAsync("test-hop-in");
        var targetQueue = await DeclareQueueAsync("test-hop-out");

        await RabbitMqConnector.Sink<TestMessage>(_connectionManager, "", sourceQueue)
            .ConsumeAsync(CreateDataStream(Enumerable.Range(0, count).Select(i => new TestMessage($"Hop-{i}", i))), new PipelineContext(),
                CancellationToken.None);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var context = new PipelineContext();

        await using (var source = RabbitMqConnector.Source<TestMessage>(_connectionManager, sourceQueue))
        {
            var sink = RabbitMqConnector.Sink<TestMessage>(_connectionManager, "", targetQueue).Acknowledging();
            var messages = source.OpenStream(context, cts.Token);

            await sink.ConsumeAsync(new DataStream<IAcknowledgableMessage<TestMessage>>(TakeAsync(messages, count, cts.Token), "hop"), context, cts.Token);
        }

        (await ReceiveAsync(targetQueue, count)).Select(m => m.Name).Should().BeEquivalentTo(Enumerable.Range(0, count).Select(i => $"Hop-{i}"));
        (await ReceiveAsync(sourceQueue, 1, TimeSpan.FromMilliseconds(500))).Should().BeEmpty("the sink acknowledged every message it published");
    }

    private static async IAsyncEnumerable<IAcknowledgableMessage<T>> TakeAsync<T>(IAsyncEnumerable<IAcknowledgableMessage<T>> source, int count,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var taken = 0;

        await foreach (var message in source.WithCancellation(cancellationToken))
        {
            yield return message;

            if (++taken == count)
                yield break;
        }
    }

    private async Task<string> DeclareQueueAsync(string prefix)
    {
        var queueName = $"{prefix}-{Guid.NewGuid():N}";
        var connection = await _connectionManager.GetConnectionAsync();
        await using var channel = await connection.CreateChannelAsync();
        _ = await channel.QueueDeclareAsync(queueName, true, false, false);
        await channel.CloseAsync();
        return queueName;
    }

    /// <summary>Gets up to <paramref name="count" /> messages, waiting up to <paramref name="timeout" /> (10 seconds by default) for them.</summary>
    private async Task<List<TestMessage>> ReceiveAsync(string queueName, int count, TimeSpan? timeout = null)
    {
        var connection = await _connectionManager.GetConnectionAsync();
        await using var channel = await connection.CreateChannelAsync();
        var received = new List<TestMessage>();
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(10));

        while (received.Count < count && DateTime.UtcNow < deadline)
        {
            if (await channel.BasicGetAsync(queueName, true) is { } result)
                received.Add(JsonMessageSerializer.Default.Deserialize<TestMessage>(result.Body.Span, new MessageContext(queueName)));
            else
                await Task.Delay(50);
        }

        await channel.CloseAsync();
        return received;
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

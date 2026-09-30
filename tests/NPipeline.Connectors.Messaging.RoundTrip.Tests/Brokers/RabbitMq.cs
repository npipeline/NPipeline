using System.Text;
using NPipeline.Connectors.Messaging.RoundTrip.Tests.Harnesses;
using NPipeline.Connectors.Messaging.RoundTrip.Tests.Infrastructure;
using NPipeline.Connectors.RabbitMQ;
using NPipeline.Connectors.RabbitMQ.Configuration;
using NPipeline.Connectors.RabbitMQ.Connection;
using NPipeline.Connectors.RabbitMQ.Models;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using RabbitMQ.Client;

namespace NPipeline.Connectors.Messaging.RoundTrip.Tests.Brokers;

public sealed class RabbitMqHarness(string hostName, int port, string userName, string password) : MessagingHarness
{
    private readonly IRabbitMqConnectionManager _connection =
        RabbitMqConnector.Connect(new RabbitMqConnectionOptions { HostName = hostName, Port = port, UserName = userName, Password = password });

    public override string Name => "RabbitMQ";

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        await _connection.DisposeAsync();
    }

    public override async Task<string> CreateDestinationAsync()
    {
        var queue = $"rt-{Guid.NewGuid():N}";
        await using var channel = await _connection.CreateChannelAsync();
        _ = await channel.QueueDeclareAsync(queue, true, false, false);
        return queue;
    }

    public override async Task PublishRawAsync(string destination, params string[] bodies)
    {
        await using var channel = await _connection.CreateChannelAsync();

        foreach (var body in bodies)
        {
            await channel.BasicPublishAsync(string.Empty, destination, false, new BasicProperties(), Encoding.UTF8.GetBytes(body));
        }
    }

    public override async Task<List<string>> ReceiveRawAsync(string destination, int count, TimeSpan timeout)
    {
        await using var channel = await _connection.CreateChannelAsync();
        var bodies = new List<string>();
        var deadline = DateTime.UtcNow + timeout;

        while (bodies.Count < count && DateTime.UtcNow < deadline)
        {
            if (await channel.BasicGetAsync(destination, true) is { } result)
                bodies.Add(Encoding.UTF8.GetString(result.Body.Span));
            else
                await Task.Delay(100);
        }

        return bodies;
    }

    public override IAsyncEnumerable<IAcknowledgableMessage<T>> ReadAsync<T>(string destination, ReadSettings settings, PipelineContext context,
        CancellationToken cancellationToken) =>
        Enumerate<RabbitMqMessage<T>, T>(RabbitMqConnector.Source<T>(_connection, destination, o => o with { RowErrorHandler = settings.Handler }), context,
            cancellationToken);

    protected override SinkNode<T> Sink<T>(string destination) => RabbitMqConnector.Sink<T>(_connection, string.Empty, destination);
}

[Collection(RabbitMqBrokers.Name)]
public sealed class RabbitMqRoundTripTests(RabbitMqBrokerFixture broker)
    : MessagingRoundTripTests(new RabbitMqHarness(broker.HostName, broker.Port, RabbitMqBrokerFixture.UserName, RabbitMqBrokerFixture.Password));

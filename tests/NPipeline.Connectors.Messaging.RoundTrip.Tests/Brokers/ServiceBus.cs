using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using NPipeline.Connectors.Azure.ServiceBus;
using NPipeline.Connectors.Messaging.RoundTrip.Tests.Harnesses;
using NPipeline.Connectors.Messaging.RoundTrip.Tests.Infrastructure;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using SdkMessage = Azure.Messaging.ServiceBus.ServiceBusMessage;

namespace NPipeline.Connectors.Messaging.RoundTrip.Tests.Brokers;

public sealed class ServiceBusHarness(string connectionString, string administrationConnectionString) : MessagingHarness
{
    private readonly ServiceBusAdministrationClient _administration = new(administrationConnectionString);
    private readonly ServiceBusClient _client = new(connectionString);

    public override string Name => "Service Bus";

    public override TimeSpan RedeliveryDelay => TimeSpan.FromSeconds(1);

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        await _client.DisposeAsync();
    }

    public override async Task<string> CreateDestinationAsync()
    {
        var queue = $"rt-{Guid.NewGuid():N}";
        _ = await _administration.CreateQueueAsync(new CreateQueueOptions(queue) { LockDuration = TimeSpan.FromSeconds(5), MaxDeliveryCount = 10 });
        return queue;
    }

    public override async Task PublishRawAsync(string destination, params string[] bodies)
    {
        await using var sender = _client.CreateSender(destination);

        foreach (var body in bodies)
        {
            await sender.SendMessageAsync(new SdkMessage(body));
        }
    }

    public override async Task<List<string>> ReceiveRawAsync(string destination, int count, TimeSpan timeout)
    {
        await using var receiver = _client.CreateReceiver(destination);
        var bodies = new List<string>();
        var deadline = DateTime.UtcNow + timeout;

        while (bodies.Count < count && DateTime.UtcNow < deadline)
        {
            foreach (var message in await receiver.ReceiveMessagesAsync(count - bodies.Count, TimeSpan.FromSeconds(1)))
            {
                bodies.Add(message.Body.ToString());
                await receiver.CompleteMessageAsync(message);
            }
        }

        return bodies;
    }

    public override IAsyncEnumerable<IAcknowledgableMessage<T>> ReadAsync<T>(string destination, ReadSettings settings, PipelineContext context,
        CancellationToken cancellationToken) =>
        Enumerate<Azure.ServiceBus.Models.ServiceBusMessage<T>, T>(
            ServiceBusConnector.Source<T>(_client, destination, o => o with { RowErrorHandler = settings.Handler }), context, cancellationToken);

    protected override SinkNode<T> Sink<T>(string destination) => ServiceBusConnector.Sink<T>(_client, destination);

    public ServiceBusClient Client => _client;

    public async Task<string> CreateSessionQueueAsync()
    {
        var queue = $"rt-{Guid.NewGuid():N}";
        _ = await _administration.CreateQueueAsync(new CreateQueueOptions(queue) { RequiresSession = true, LockDuration = TimeSpan.FromSeconds(5) });
        return queue;
    }

    public IAsyncEnumerable<IAcknowledgableMessage<T>> ReadSessionsAsync<T>(string destination, CancellationToken cancellationToken) =>
        Enumerate<Azure.ServiceBus.Models.ServiceBusMessage<T>, T>(
            ServiceBusConnector.SessionSource<T>(_client, destination, configure: o => o with { MaxConcurrentSessions = 2, SessionIdleTimeout = TimeSpan.FromSeconds(1) }),
            new PipelineContext(), cancellationToken);
}

[Collection(ServiceBusBrokers.Name)]
public sealed class ServiceBusRoundTripTests(ServiceBusBrokerFixture broker)
    : MessagingRoundTripTests(new ServiceBusHarness(broker.ConnectionString, broker.AdministrationConnectionString))
{
    private static readonly System.Text.Json.JsonSerializerOptions Web = new(System.Text.Json.JsonSerializerDefaults.Web);

    [Fact]
    public async Task Sessions_are_read_in_order_and_keep_their_ids_through_a_sink()
    {
        var harness = (ServiceBusHarness)Harness;
        var from = await harness.CreateSessionQueueAsync();
        var to = await harness.CreateSessionQueueAsync();

        await using (var sender = harness.Client.CreateSender(from))
        {
            for (var i = 1; i <= 6; i++)
            {
                var body = System.Text.Json.JsonSerializer.Serialize(Order.Create(i), Web);
                await sender.SendMessageAsync(new SdkMessage(body) { SessionId = i % 2 == 0 ? "even" : "odd" });
            }
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var received = new List<(string Session, int Id)>();

        var messages = harness.ReadSessionsAsync<Order>(from, cts.Token).Limit(6, cts.Token);
        await harness.WriteMessagesAsync(to, Record(messages, received), new PipelineContext(), cts.Token);

        received.Where(r => r.Session == "odd").Select(r => r.Id).Should().Equal(1, 3, 5);
        received.Where(r => r.Session == "even").Select(r => r.Id).Should().Equal(2, 4, 6);

        // The sink kept each message's session, so the target queue's sessions hold the same messages.
        var copied = new List<string>();

        await using (var session = await harness.Client.AcceptSessionAsync(to, "even"))
        {
            foreach (var message in await session.ReceiveMessagesAsync(3, TimeSpan.FromSeconds(5)))
            {
                copied.Add(message.SessionId);
                await session.CompleteMessageAsync(message);
            }
        }

        copied.Should().Equal("even", "even", "even");
    }

    private static async IAsyncEnumerable<IAcknowledgableMessage<Order>> Record(IAsyncEnumerable<IAcknowledgableMessage<Order>> messages,
        List<(string Session, int Id)> received)
    {
        await foreach (var message in messages)
        {
            received.Add((((Azure.ServiceBus.Models.ServiceBusMessage<Order>)message).SessionId!, message.Body.Id));
            yield return message;
        }
    }
}

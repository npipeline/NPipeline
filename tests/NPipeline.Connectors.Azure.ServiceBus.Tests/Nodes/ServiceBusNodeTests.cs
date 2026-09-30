using System.Reflection;
using Azure.Messaging.ServiceBus;
using NPipeline.Connectors.Azure.ServiceBus.Configuration;
using NPipeline.Connectors.Azure.ServiceBus.Nodes;
using NPipeline.Connectors.Messaging;
using static NPipeline.Connectors.Azure.ServiceBus.Tests.Configuration.ServiceBusOptionsTests;

namespace NPipeline.Connectors.Azure.ServiceBus.Tests.Nodes;

internal static class NodeInternals
{
    public static TValue Field<TValue>(object node, string name) =>
        (TValue)node.GetType().GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(node)!;

    public static ServiceBusReadOptions ReadOptions<T>(ServiceBusSourceNode<T> node) => Field<ServiceBusReadOptions>(node, "_options");

    public static bool Sessions<T>(ServiceBusSourceNode<T> node) => Field<bool>(node, "_sessions");

    public static ServiceBusWriteOptions<T> WriteOptions<T>(ServiceBusSinkNode<T> node) => Field<ServiceBusWriteOptions<T>>(node, "_options");
}

public class ServiceBusSourceNodeTests
{
    [Fact]
    public void Constructor_WithNullOptions_Throws()
    {
        var create = () => new ServiceBusSourceNode<int>(null!);

        create.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithInvalidOptions_Throws()
    {
        var create = () => new ServiceBusSourceNode<int>(new ServiceBusReadOptions { ConnectionString = ConnectionString, Entity = "q", MaxInFlight = 0 });

        create.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Constructor_WithNoConnection_Throws()
    {
        var create = () => new ServiceBusSourceNode<int>(new ServiceBusReadOptions { Entity = "q" });

        create.Should().ThrowExactly<ArgumentException>();
    }

    [Fact]
    public async Task OpenStream_AfterDispose_Throws()
    {
        var source = new ServiceBusSourceNode<int>(new ServiceBusReadOptions { ConnectionString = ConnectionString, Entity = "q" });
        await source.DisposeAsync();

        var open = () => source.OpenStream(new NPipeline.Pipeline.PipelineContext(), CancellationToken.None);

        open.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public async Task DisposeAsync_Twice_DoesNotThrow()
    {
        var source = new ServiceBusSourceNode<int>(new ServiceBusReadOptions { ConnectionString = ConnectionString, Entity = "q" });
        await source.DisposeAsync();

        var again = async () => await source.DisposeAsync();

        await again.Should().NotThrowAsync();
    }
}

public class ServiceBusSinkNodeTests
{
    [Fact]
    public void Constructor_WithNullOptions_Throws()
    {
        var create = () => new ServiceBusSinkNode<int>(null!);

        create.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_WithInvalidOptions_Throws()
    {
        var create = () => new ServiceBusSinkNode<int>(new ServiceBusWriteOptions<int> { ConnectionString = ConnectionString, Entity = "q", BatchSize = 0 });

        create.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public async Task DisposeAsync_WithOwnClient_DisposesIt()
    {
        var sink = new ServiceBusSinkNode<int>(new ServiceBusWriteOptions<int> { ConnectionString = ConnectionString, Entity = "q" });
        var client = NodeInternals.Field<ServiceBusClient>(sink, "_client");

        await sink.DisposeAsync();

        client.IsClosed.Should().BeTrue();
        NodeInternals.Field<ServiceBusSender>(sink, "_sender").IsClosed.Should().BeTrue();
    }

    [Fact]
    public async Task DisposeAsync_WithSharedClient_LeavesItOpen()
    {
        await using var client = new ServiceBusClient(ConnectionString);
        var sink = ServiceBusConnector.Sink<int>(client, "q");

        await sink.DisposeAsync();

        client.IsClosed.Should().BeFalse();
        NodeInternals.Field<ServiceBusSender>(sink, "_sender").IsClosed.Should().BeTrue();
    }

    [Fact]
    public async Task Sinks_OnOneClient_HaveTheirOwnSenders()
    {
        await using var client = new ServiceBusClient(ConnectionString);
        await using var first = ServiceBusConnector.Sink<int>(client, "q");
        var second = ServiceBusConnector.Sink<int>(client, "q");

        await second.DisposeAsync();

        NodeInternals.Field<ServiceBusSender>(first, "_sender").IsClosed.Should().BeFalse();
    }

    [Fact]
    public async Task Sink_IsAMessageSink()
    {
        await using var client = new ServiceBusClient(ConnectionString);
        await using var sink = ServiceBusConnector.Sink<int>(client, "q");

        sink.Should().BeAssignableTo<IMessageSink<int>>();
        sink.Acknowledging().Should().BeOfType<AcknowledgingSink<int>>().Which.Inner.Should().BeSameAs(sink);
    }
}

public class ServiceBusConnectorTests
{
    [Fact]
    public async Task Source_ReadsTheQueue()
    {
        await using var client = new ServiceBusClient(ConnectionString);
        await using var source = ServiceBusConnector.Source<int>(client, "orders");

        var options = NodeInternals.ReadOptions(source);
        options.Client.Should().BeSameAs(client);
        options.Entity.Should().Be("orders");
        options.Subscription.Should().BeNull();
        NodeInternals.Sessions(source).Should().BeFalse();
    }

    [Fact]
    public async Task Source_AppliesConfigure()
    {
        await using var client = new ServiceBusClient(ConnectionString);
        await using var source = ServiceBusConnector.Source<int>(client, "orders", o => o with { MaxInFlight = 7, SubQueue = SubQueue.DeadLetter });

        var options = NodeInternals.ReadOptions(source);
        options.MaxInFlight.Should().Be(7);
        options.SubQueue.Should().Be(SubQueue.DeadLetter);
        options.Entity.Should().Be("orders");
    }

    [Fact]
    public async Task Source_WithInvalidConfigure_Throws()
    {
        await using var client = new ServiceBusClient(ConnectionString);

        var create = () => ServiceBusConnector.Source<int>(client, "orders", o => o with { MaxInFlight = 0 });

        create.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Source_WithNullClient_Throws()
    {
        var create = () => ServiceBusConnector.Source<int>(null!, "orders");

        create.Should().ThrowExactly<ArgumentException>();
    }

    [Fact]
    public async Task SubscriptionSource_ReadsTheSubscription()
    {
        await using var client = new ServiceBusClient(ConnectionString);
        await using var source = ServiceBusConnector.SubscriptionSource<int>(client, "orders-topic", "billing");

        var options = NodeInternals.ReadOptions(source);
        options.Entity.Should().Be("orders-topic");
        options.Subscription.Should().Be("billing");
        NodeInternals.Sessions(source).Should().BeFalse();
    }

    [Fact]
    public async Task SessionSource_ReadsSessions()
    {
        await using var client = new ServiceBusClient(ConnectionString);
        await using var queue = ServiceBusConnector.SessionSource<int>(client, "orders");
        await using var subscription = ServiceBusConnector.SessionSource<int>(client, "orders-topic", "billing", o => o with { MaxConcurrentSessions = 2 });

        NodeInternals.Sessions(queue).Should().BeTrue();
        NodeInternals.ReadOptions(queue).Subscription.Should().BeNull();

        NodeInternals.Sessions(subscription).Should().BeTrue();
        NodeInternals.ReadOptions(subscription).Subscription.Should().Be("billing");
        NodeInternals.ReadOptions(subscription).MaxConcurrentSessions.Should().Be(2);
    }

    [Fact]
    public async Task Sink_SendsToTheEntity()
    {
        await using var client = new ServiceBusClient(ConnectionString);
        await using var sink = ServiceBusConnector.Sink<int>(client, "invoices", o => o with { BatchSize = 5, FailedMessages = FailedMessageAction.DeadLetter });

        var options = NodeInternals.WriteOptions(sink);
        options.Client.Should().BeSameAs(client);
        options.Entity.Should().Be("invoices");
        options.BatchSize.Should().Be(5);
        options.FailedMessages.Should().Be(FailedMessageAction.DeadLetter);
        NodeInternals.Field<ServiceBusSender>(sink, "_sender").EntityPath.Should().Be("invoices");
    }
}

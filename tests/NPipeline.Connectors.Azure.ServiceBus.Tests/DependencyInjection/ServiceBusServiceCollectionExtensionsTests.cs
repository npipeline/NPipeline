using Azure.Core;
using Azure.Messaging.ServiceBus;
using FakeItEasy;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.Azure.ServiceBus.DependencyInjection;
using NPipeline.Connectors.Azure.ServiceBus.Tests.Nodes;
using static NPipeline.Connectors.Azure.ServiceBus.Tests.Configuration.ServiceBusOptionsTests;

namespace NPipeline.Connectors.Azure.ServiceBus.Tests.DependencyInjection;

public class ServiceBusServiceCollectionExtensionsTests
{
    [Fact]
    public async Task AddServiceBusConnector_WithConnectionString_RegistersASharedClientAndFactory()
    {
        await using var provider = new ServiceCollection().AddServiceBusConnector(ConnectionString).BuildServiceProvider();

        var client = provider.GetRequiredService<ServiceBusClient>();
        client.FullyQualifiedNamespace.Should().Be(Namespace);
        provider.GetRequiredService<ServiceBusClient>().Should().BeSameAs(client);
        provider.GetRequiredService<ServiceBusNodeFactory>().Should().BeSameAs(provider.GetRequiredService<ServiceBusNodeFactory>());
    }

    [Fact]
    public async Task AddServiceBusConnector_WithClientOptions_UsesThem()
    {
        var options = new ServiceBusClientOptions { Identifier = "npipeline-tests" };
        await using var provider = new ServiceCollection().AddServiceBusConnector(ConnectionString, options).BuildServiceProvider();

        provider.GetRequiredService<ServiceBusClient>().Identifier.Should().Be("npipeline-tests");
    }

    [Fact]
    public async Task AddServiceBusConnector_WithNamespaceAndCredential_RegistersAClient()
    {
        var credential = A.Fake<TokenCredential>();
        await using var provider = new ServiceCollection().AddServiceBusConnector(Namespace, credential).BuildServiceProvider();

        provider.GetRequiredService<ServiceBusClient>().FullyQualifiedNamespace.Should().Be(Namespace);
    }

    [Fact]
    public async Task AddServiceBusConnector_WithFactory_UsesIt()
    {
        await using var client = new ServiceBusClient(ConnectionString);
        await using var provider = new ServiceCollection().AddServiceBusConnector(_ => client).BuildServiceProvider();

        provider.GetRequiredService<ServiceBusClient>().Should().BeSameAs(client);
    }

    [Fact]
    public async Task AddServiceBusConnector_CalledTwice_KeepsTheFirstClient()
    {
        await using var first = new ServiceBusClient(ConnectionString);
        await using var second = new ServiceBusClient(ConnectionString);

        await using var provider = new ServiceCollection()
            .AddServiceBusConnector(_ => first)
            .AddServiceBusConnector(_ => second)
            .BuildServiceProvider();

        provider.GetRequiredService<ServiceBusClient>().Should().BeSameAs(first);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    public void AddServiceBusConnector_WithBlankConnectionString_Throws(string? connectionString)
    {
        var add = () => new ServiceCollection().AddServiceBusConnector(connectionString!);

        add.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void AddServiceBusConnector_WithBlankNamespace_Throws()
    {
        var add = () => new ServiceCollection().AddServiceBusConnector(" ", A.Fake<TokenCredential>());

        add.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void AddServiceBusConnector_WithNullFactory_Throws()
    {
        var add = () => new ServiceCollection().AddServiceBusConnector((Func<IServiceProvider, ServiceBusClient>)null!);

        add.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void AddServiceBusConnector_WithNullServices_Throws()
    {
        var add = () => ((IServiceCollection)null!).AddServiceBusConnector(ConnectionString);

        add.Should().Throw<ArgumentNullException>();
    }

    public class NodeFactory
    {
        [Fact]
        public async Task CreatesNodesOnTheRegisteredClient()
        {
            await using var client = new ServiceBusClient(ConnectionString);
            var factory = new ServiceBusNodeFactory(client);

            await using var source = factory.CreateSource<int>("orders", o => o with { MaxInFlight = 3 });
            await using var subscription = factory.CreateSubscriptionSource<int>("orders-topic", "billing");
            await using var sessions = factory.CreateSessionSource<int>("sessions", "sub");
            await using var sink = factory.CreateSink<int>("invoices", o => o with { BatchSize = 9 });

            NodeInternals.ReadOptions(source).Client.Should().BeSameAs(client);
            NodeInternals.ReadOptions(source).Entity.Should().Be("orders");
            NodeInternals.ReadOptions(source).MaxInFlight.Should().Be(3);
            NodeInternals.Sessions(source).Should().BeFalse();

            NodeInternals.ReadOptions(subscription).Entity.Should().Be("orders-topic");
            NodeInternals.ReadOptions(subscription).Subscription.Should().Be("billing");

            NodeInternals.ReadOptions(sessions).Entity.Should().Be("sessions");
            NodeInternals.ReadOptions(sessions).Subscription.Should().Be("sub");
            NodeInternals.Sessions(sessions).Should().BeTrue();

            NodeInternals.WriteOptions(sink).Client.Should().BeSameAs(client);
            NodeInternals.WriteOptions(sink).Entity.Should().Be("invoices");
            NodeInternals.WriteOptions(sink).BatchSize.Should().Be(9);
        }

        [Fact]
        public async Task IsResolvedWithTheRegisteredClient()
        {
            await using var client = new ServiceBusClient(ConnectionString);
            await using var provider = new ServiceCollection().AddServiceBusConnector(_ => client).BuildServiceProvider();

            await using var source = provider.GetRequiredService<ServiceBusNodeFactory>().CreateSource<int>("orders");

            NodeInternals.ReadOptions(source).Client.Should().BeSameAs(client);
        }
    }
}

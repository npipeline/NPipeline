using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.Messaging;
using NPipeline.Connectors.RabbitMQ.Configuration;
using NPipeline.Connectors.RabbitMQ.Connection;
using NPipeline.Connectors.RabbitMQ.DependencyInjection;

namespace NPipeline.Connectors.RabbitMQ.Tests.DependencyInjection;

public sealed class ServiceCollectionExtensionsTests
{
    [Fact]
    public async Task AddRabbitMq_Registers_ConnectionOptions()
    {
        var services = new ServiceCollection();
        _ = services.AddLogging();

        _ = services.AddRabbitMq(new RabbitMqConnectionOptions { HostName = "myhost", Port = 5673 });

        await using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<RabbitMqConnectionOptions>();

        options.HostName.Should().Be("myhost");
        options.Port.Should().Be(5673);
    }

    [Fact]
    public async Task AddRabbitMq_With_Configure_Registers_The_Configured_Options()
    {
        var services = new ServiceCollection();

        _ = services.AddRabbitMq(o => o with { HostName = "rabbit", Port = 5673 });

        await using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<RabbitMqConnectionOptions>().Should().Be(new RabbitMqConnectionOptions { HostName = "rabbit", Port = 5673 });
    }

    [Fact]
    public async Task AddRabbitMq_Registers_ConnectionManager_As_Singleton()
    {
        var services = new ServiceCollection();
        _ = services.AddLogging();
        _ = services.AddRabbitMq(new RabbitMqConnectionOptions());

        await using var provider = services.BuildServiceProvider();
        var manager = provider.GetService<IRabbitMqConnectionManager>();

        manager.Should().BeOfType<RabbitMqConnectionManager>();
        provider.GetService<IRabbitMqConnectionManager>().Should().BeSameAs(manager);
    }

    [Fact]
    public async Task AddRabbitMq_Works_Without_Logging()
    {
        var services = new ServiceCollection();
        _ = services.AddRabbitMq(new RabbitMqConnectionOptions());

        await using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IRabbitMqConnectionManager>().Should().BeOfType<RabbitMqConnectionManager>();
    }

    [Fact]
    public void AddRabbitMq_Throws_On_Invalid_Options()
    {
        var services = new ServiceCollection();
        var act = () => services.AddRabbitMq(new RabbitMqConnectionOptions { Port = -1 });
        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public async Task NodeFactory_CreateSource_Uses_The_Registered_Connection()
    {
        var services = new ServiceCollection();
        _ = services.AddRabbitMq(new RabbitMqConnectionOptions());

        await using var provider = services.BuildServiceProvider();
        var factory = provider.GetRequiredService<RabbitMqNodeFactory>();
        var captured = default(RabbitMqReadOptions);

        var source = factory.CreateSource<string>("test-q", o =>
        {
            captured = o with { PrefetchCount = 5 };
            return captured;
        });

        source.Should().NotBeNull();
        captured!.Queue.Should().Be("test-q");
        captured.Connection.Should().BeSameAs(provider.GetRequiredService<IRabbitMqConnectionManager>());
    }

    [Fact]
    public async Task NodeFactory_CreateSink_Uses_The_Registered_Connection()
    {
        var services = new ServiceCollection();
        _ = services.AddRabbitMq(new RabbitMqConnectionOptions());

        await using var provider = services.BuildServiceProvider();
        var factory = provider.GetRequiredService<RabbitMqNodeFactory>();
        var captured = default(RabbitMqWriteOptions<string>);

        var sink = factory.CreateSink<string>("test-ex", "rk", o =>
        {
            captured = o with { FailedMessages = FailedMessageAction.Requeue };
            return captured;
        });

        sink.Should().NotBeNull();
        captured!.Exchange.Should().Be("test-ex");
        captured.RoutingKey.Should().Be("rk");
        captured.Connection.Should().BeSameAs(provider.GetRequiredService<IRabbitMqConnectionManager>());
    }

    [Fact]
    public async Task NodeFactory_Validates_Node_Options()
    {
        var services = new ServiceCollection();
        _ = services.AddRabbitMq(new RabbitMqConnectionOptions());

        await using var provider = services.BuildServiceProvider();
        var factory = provider.GetRequiredService<RabbitMqNodeFactory>();

        var act = () => factory.CreateSource<string>(" ");
        act.Should().Throw<ArgumentException>();
    }
}

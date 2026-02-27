using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.RabbitMQ.Configuration;
using NPipeline.Connectors.RabbitMQ.Connection;
using NPipeline.Connectors.RabbitMQ.DependencyInjection;
using NPipeline.Connectors.RabbitMQ.Metrics;
using NPipeline.Connectors.Serialization;

namespace NPipeline.Connectors.RabbitMQ.Tests.DependencyInjection;

public sealed class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddRabbitMq_Registers_ConnectionOptions()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddRabbitMq(o =>
        {
            o.HostName = "myhost";
            o.Port = 5673;
        });

        var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<RabbitMqConnectionOptions>();

        options.HostName.Should().Be("myhost");
        options.Port.Should().Be(5673);
    }

    [Fact]
    public void AddRabbitMq_Registers_ConnectionManager()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRabbitMq(o => { });

        var provider = services.BuildServiceProvider();
        var manager = provider.GetService<IRabbitMqConnectionManager>();

        manager.Should().NotBeNull();
        manager.Should().BeOfType<RabbitMqConnectionManager>();
    }

    [Fact]
    public void AddRabbitMq_Registers_NullMetrics_By_Default()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRabbitMq(o => { });

        var provider = services.BuildServiceProvider();
        var metrics = provider.GetRequiredService<IRabbitMqMetrics>();

        metrics.Should().BeSameAs(NullRabbitMqMetrics.Instance);
    }

    [Fact]
    public void AddRabbitMq_Registers_Default_Serializer()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRabbitMq(o => { });

        var provider = services.BuildServiceProvider();
        var serializer = provider.GetRequiredService<IMessageSerializer>();

        serializer.Should().NotBeNull();
    }

    [Fact]
    public void AddRabbitMq_Throws_On_Invalid_Options()
    {
        var services = new ServiceCollection();
        var act = () => services.AddRabbitMq(o => o.Port = -1);
        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void AddRabbitMqSource_Registers_Options_And_Node()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRabbitMq(o => { });
        services.AddRabbitMqSource<string>(new RabbitMqSourceOptions { QueueName = "test-q" });

        var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<RabbitMqSourceOptions>();

        options.QueueName.Should().Be("test-q");
    }

    [Fact]
    public void AddRabbitMqSink_Registers_Options_And_Node()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRabbitMq(o => { });
        services.AddRabbitMqSink<string>(new RabbitMqSinkOptions { ExchangeName = "test-ex" });

        var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<RabbitMqSinkOptions>();

        options.ExchangeName.Should().Be("test-ex");
    }
}

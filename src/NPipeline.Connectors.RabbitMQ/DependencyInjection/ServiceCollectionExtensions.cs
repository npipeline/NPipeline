using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using NPipeline.Connectors.RabbitMQ.Configuration;
using NPipeline.Connectors.RabbitMQ.Connection;
using NPipeline.Connectors.RabbitMQ.Nodes;

namespace NPipeline.Connectors.RabbitMQ.DependencyInjection;

/// <summary>Registers the RabbitMQ connector.</summary>
public static class RabbitMqServiceCollectionExtensions
{
    /// <summary>Registers one connection, shared by every node, and <see cref="RabbitMqNodeFactory" />.</summary>
    public static IServiceCollection AddRabbitMq(this IServiceCollection services, Func<RabbitMqConnectionOptions, RabbitMqConnectionOptions> configureConnection)
    {
        ArgumentNullException.ThrowIfNull(configureConnection);
        return services.AddRabbitMq(configureConnection(new RabbitMqConnectionOptions()));
    }

    /// <summary>Registers one connection, shared by every node, and <see cref="RabbitMqNodeFactory" />.</summary>
    public static IServiceCollection AddRabbitMq(this IServiceCollection services, RabbitMqConnectionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();

        services.TryAddSingleton(options);
        services.TryAddSingleton<IRabbitMqConnectionManager>(sp =>
            new RabbitMqConnectionManager(sp.GetRequiredService<RabbitMqConnectionOptions>(), sp.GetService<ILogger<RabbitMqConnectionManager>>()));

        services.TryAddSingleton<RabbitMqNodeFactory>();
        return services;
    }
}

/// <summary>Creates RabbitMQ nodes on the registered connection.</summary>
public sealed class RabbitMqNodeFactory(IRabbitMqConnectionManager connection)
{
    /// <summary>A source that consumes <paramref name="queue" />.</summary>
    public RabbitMqSourceNode<T> CreateSource<T>(string queue, Func<RabbitMqReadOptions, RabbitMqReadOptions>? configure = null) =>
        RabbitMqConnector.Source<T>(connection, queue, configure);

    /// <summary>A sink that publishes to <paramref name="exchange" /> with <paramref name="routingKey" />.</summary>
    public RabbitMqSinkNode<T> CreateSink<T>(string exchange, string routingKey = "", Func<RabbitMqWriteOptions<T>, RabbitMqWriteOptions<T>>? configure = null) =>
        RabbitMqConnector.Sink<T>(connection, exchange, routingKey, configure);
}

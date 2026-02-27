using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NPipeline.Connectors.RabbitMQ.Configuration;
using NPipeline.Connectors.RabbitMQ.Connection;
using NPipeline.Connectors.RabbitMQ.Metrics;
using NPipeline.Connectors.RabbitMQ.Nodes;
using NPipeline.Connectors.RabbitMQ.Serialization;
using NPipeline.Connectors.Serialization;

namespace NPipeline.Connectors.RabbitMQ.DependencyInjection;

/// <summary>
///     Extension methods for registering RabbitMQ connector services.
/// </summary>
public static class RabbitMqServiceCollectionExtensions
{
    /// <summary>
    ///     Registers the RabbitMQ connection manager and shared services.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configureConnection">Action to configure connection options.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddRabbitMq(
        this IServiceCollection services,
        Action<RabbitMqConnectionOptions> configureConnection)
    {
        var connectionOptions = new RabbitMqConnectionOptions();
        configureConnection(connectionOptions);
        connectionOptions.Validate();

        services.AddSingleton(connectionOptions);
        services.AddSingleton<IRabbitMqConnectionManager, RabbitMqConnectionManager>();
        services.TryAddSingleton<IRabbitMqMetrics>(NullRabbitMqMetrics.Instance);
        services.TryAddSingleton<IMessageSerializer, RabbitMqJsonSerializer>();

        return services;
    }

    /// <summary>
    ///     Registers a <see cref="Nodes.RabbitMqSourceNode{T}" /> and its source options.
    /// </summary>
    /// <typeparam name="T">The message body type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="options">The source options. Must have <see cref="RabbitMqSourceOptions.QueueName" /> set.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddRabbitMqSource<T>(
        this IServiceCollection services,
        RabbitMqSourceOptions options)
    {
        options.Validate();
        services.AddSingleton(options);
        services.AddTransient<RabbitMqSourceNode<T>>();

        return services;
    }

    /// <summary>
    ///     Registers a <see cref="Nodes.RabbitMqSinkNode{T}" /> and its sink options.
    /// </summary>
    /// <typeparam name="T">The message body type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="options">The sink options. Must have <see cref="RabbitMqSinkOptions.ExchangeName" /> set.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddRabbitMqSink<T>(
        this IServiceCollection services,
        RabbitMqSinkOptions options)
    {
        options.Validate();
        services.AddSingleton(options);
        services.AddTransient<RabbitMqSinkNode<T>>();

        return services;
    }
}

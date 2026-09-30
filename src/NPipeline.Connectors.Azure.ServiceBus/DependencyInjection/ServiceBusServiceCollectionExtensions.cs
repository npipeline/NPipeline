using Azure.Core;
using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NPipeline.Connectors.Azure.ServiceBus.Configuration;
using NPipeline.Connectors.Azure.ServiceBus.Nodes;

namespace NPipeline.Connectors.Azure.ServiceBus.DependencyInjection;

/// <summary>Registers the Service Bus connector.</summary>
public static class ServiceBusServiceCollectionExtensions
{
    /// <summary>Registers one <see cref="ServiceBusClient" /> for <paramref name="connectionString" />, shared by every node, and <see cref="ServiceBusNodeFactory" />.</summary>
    public static IServiceCollection AddServiceBusConnector(this IServiceCollection services, string connectionString, ServiceBusClientOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        return services.AddServiceBusConnector(_ => new ServiceBusClient(connectionString, options ?? new ServiceBusClientOptions()));
    }

    /// <summary>Registers one <see cref="ServiceBusClient" /> for a namespace and credential (<c>DefaultAzureCredential</c> by default), and <see cref="ServiceBusNodeFactory" />.</summary>
    public static IServiceCollection AddServiceBusConnector(this IServiceCollection services, string fullyQualifiedNamespace, TokenCredential? credential,
        ServiceBusClientOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fullyQualifiedNamespace);

        return services.AddServiceBusConnector(_ =>
            new ServiceBusClient(fullyQualifiedNamespace, credential ?? new DefaultAzureCredential(), options ?? new ServiceBusClientOptions()));
    }

    /// <summary>Registers a <see cref="ServiceBusClient" /> made by <paramref name="client" />, shared by every node, and <see cref="ServiceBusNodeFactory" />.</summary>
    public static IServiceCollection AddServiceBusConnector(this IServiceCollection services, Func<IServiceProvider, ServiceBusClient> client)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(client);

        services.TryAddSingleton(client);
        services.TryAddSingleton<ServiceBusNodeFactory>();
        return services;
    }
}

/// <summary>Creates Service Bus nodes on the registered client.</summary>
public sealed class ServiceBusNodeFactory(ServiceBusClient client)
{
    /// <summary>A source that receives from <paramref name="queue" />.</summary>
    public ServiceBusSourceNode<T> CreateSource<T>(string queue, Func<ServiceBusReadOptions, ServiceBusReadOptions>? configure = null) =>
        ServiceBusConnector.Source<T>(client, queue, configure);

    /// <summary>A source that receives from <paramref name="subscription" /> of <paramref name="topic" />.</summary>
    public ServiceBusSourceNode<T> CreateSubscriptionSource<T>(string topic, string subscription, Func<ServiceBusReadOptions, ServiceBusReadOptions>? configure = null) =>
        ServiceBusConnector.SubscriptionSource<T>(client, topic, subscription, configure);

    /// <summary>A source for a session-enabled queue or subscription.</summary>
    public ServiceBusSourceNode<T> CreateSessionSource<T>(string entity, string? subscription = null, Func<ServiceBusReadOptions, ServiceBusReadOptions>? configure = null) =>
        ServiceBusConnector.SessionSource<T>(client, entity, subscription, configure);

    /// <summary>A sink that sends to <paramref name="entity" />.</summary>
    public ServiceBusSinkNode<T> CreateSink<T>(string entity, Func<ServiceBusWriteOptions<T>, ServiceBusWriteOptions<T>>? configure = null) =>
        ServiceBusConnector.Sink<T>(client, entity, configure);
}

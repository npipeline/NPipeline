using Azure.Messaging.ServiceBus;
using NPipeline.Connectors.Azure.ServiceBus.Configuration;
using NPipeline.Connectors.Azure.ServiceBus.Nodes;

namespace NPipeline.Connectors.Azure.ServiceBus;

/// <summary>Creates Service Bus sources and sinks over a shared <see cref="ServiceBusClient" />.</summary>
/// <example>
///     <code>
/// await using var client = new ServiceBusClient(connectionString);
/// var orders = ServiceBusConnector.Source&lt;Order&gt;(client, "orders");
/// var billing = ServiceBusConnector.SubscriptionSource&lt;Order&gt;(client, "orders-topic", "billing");
/// var invoices = ServiceBusConnector.Sink&lt;Invoice&gt;(client, "invoices");
///     </code>
/// </example>
public static class ServiceBusConnector
{
    internal const string Name = "servicebus";

    /// <summary>A source that receives from <paramref name="queue" />.</summary>
    public static ServiceBusSourceNode<T> Source<T>(ServiceBusClient client, string queue, Func<ServiceBusReadOptions, ServiceBusReadOptions>? configure = null) =>
        Create<T>(new ServiceBusReadOptions { Client = client, Entity = queue }, configure, false);

    /// <summary>A source that receives from <paramref name="subscription" /> of <paramref name="topic" />.</summary>
    public static ServiceBusSourceNode<T> SubscriptionSource<T>(ServiceBusClient client, string topic, string subscription,
        Func<ServiceBusReadOptions, ServiceBusReadOptions>? configure = null) =>
        Create<T>(new ServiceBusReadOptions { Client = client, Entity = topic, Subscription = subscription }, configure, false);

    /// <summary>
    ///     A source for a session-enabled queue, or a topic's session-enabled subscription: it accepts up to
    ///     <see cref="ServiceBusReadOptions.MaxConcurrentSessions" /> sessions at once and hands each session's messages on in order.
    /// </summary>
    public static ServiceBusSourceNode<T> SessionSource<T>(ServiceBusClient client, string entity, string? subscription = null,
        Func<ServiceBusReadOptions, ServiceBusReadOptions>? configure = null) =>
        Create<T>(new ServiceBusReadOptions { Client = client, Entity = entity, Subscription = subscription }, configure, true);

    /// <summary>A sink that sends to the queue or topic <paramref name="entity" />.</summary>
    public static ServiceBusSinkNode<T> Sink<T>(ServiceBusClient client, string entity, Func<ServiceBusWriteOptions<T>, ServiceBusWriteOptions<T>>? configure = null)
    {
        var options = new ServiceBusWriteOptions<T> { Client = client, Entity = entity };
        return new ServiceBusSinkNode<T>(configure?.Invoke(options) ?? options);
    }

    private static ServiceBusSourceNode<T> Create<T>(ServiceBusReadOptions options, Func<ServiceBusReadOptions, ServiceBusReadOptions>? configure, bool sessions) =>
        new(configure?.Invoke(options) ?? options, sessions);
}

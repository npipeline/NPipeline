using Azure.Messaging.ServiceBus;
using NPipeline.Connectors.Azure.ServiceBus.Configuration;

namespace NPipeline.Connectors.Azure.ServiceBus.Connection;

/// <summary>
///     Manages pooled <see cref="ServiceBusClient" /> instances and reusable <see cref="ServiceBusSender" />
///     instances, keyed by connection name and entity name.
/// </summary>
public interface IServiceBusConnectionPool : IAsyncDisposable
{
    /// <summary>
    ///     Returns an existing <see cref="ServiceBusClient" /> or creates a new one for
    ///     <paramref name="configuration" />.  Clients are keyed by connection string or namespace.
    /// </summary>
    ServiceBusClient GetOrCreateClient(ServiceBusConfiguration configuration);

    /// <summary>
    ///     Returns an existing <see cref="ServiceBusSender" /> or creates a new one for
    ///     <paramref name="queueOrTopicName" />.  Senders are long-lived and safe to share.
    /// </summary>
    ServiceBusSender GetOrCreateSender(
        ServiceBusConfiguration configuration,
        string queueOrTopicName);
}

using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;
using NPipeline.Connectors.Azure.Configuration;
using NPipeline.Connectors.Azure.ServiceBus.Configuration;

namespace NPipeline.Connectors.Azure.ServiceBus.Connection;

/// <summary>
///     Thread-safe pool of <see cref="ServiceBusClient" /> and <see cref="ServiceBusSender" /> instances.
/// </summary>
/// <remarks>
///     <para>
///         <see cref="ServiceBusClient" /> instances are keyed by the connection string or namespace so
///         that multiple nodes sharing the same logical connection share the same TCP links.
///     </para>
///     <para>
///         <see cref="ServiceBusSender" /> instances are long-lived and safe to share across nodes
///         sending to the same entity. They are keyed by <c>connectionKey:entityName</c>.
///     </para>
///     <para>
///         <strong>Note:</strong> <see cref="ServiceBusProcessor" /> instances are <em>not</em>
///         pooled — they are lifecycle-bound to their source node and are created and disposed per node.
///     </para>
/// </remarks>
public sealed class ServiceBusConnectionPool : IServiceBusConnectionPool
{
    private readonly AzureConnectionOptions? _azureConnections;
    private readonly ConcurrentDictionary<string, ServiceBusClient> _clients = new();
    private readonly ConcurrentDictionary<string, ServiceBusSender> _senders = new();
    private bool _disposed;

    /// <summary>Initializes a new <see cref="ServiceBusConnectionPool" /> without named connections.</summary>
    public ServiceBusConnectionPool()
    {
    }

    /// <summary>
    ///     Initializes a new <see cref="ServiceBusConnectionPool" /> with optional named Azure connections.
    /// </summary>
    public ServiceBusConnectionPool(AzureConnectionOptions azureConnections)
    {
        _azureConnections = azureConnections;
    }

    /// <inheritdoc />
    public ServiceBusClient GetOrCreateClient(ServiceBusConfiguration configuration)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var key = ServiceBusClientFactory.GetConnectionKey(configuration);
        return _clients.GetOrAdd(key, _ => ServiceBusClientFactory.Create(configuration, _azureConnections));
    }

    /// <inheritdoc />
    public ServiceBusSender GetOrCreateSender(
        ServiceBusConfiguration configuration,
        string queueOrTopicName)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var connectionKey = ServiceBusClientFactory.GetConnectionKey(configuration);
        var senderKey = $"{connectionKey}:{queueOrTopicName}";

        return _senders.GetOrAdd(senderKey, _ =>
        {
            var client = GetOrCreateClient(configuration);
            return client.CreateSender(queueOrTopicName);
        });
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        // Dispose all senders first
        var senderTasks = _senders.Values.Select(s => s.DisposeAsync().AsTask());
        await Task.WhenAll(senderTasks).ConfigureAwait(false);
        _senders.Clear();

        // Then dispose all clients
        var clientTasks = _clients.Values.Select(c => c.DisposeAsync().AsTask());
        await Task.WhenAll(clientTasks).ConfigureAwait(false);
        _clients.Clear();
    }
}

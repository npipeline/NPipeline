using Azure.Identity;
using Azure.Messaging.ServiceBus;
using NPipeline.Connectors.Azure.ServiceBus.Configuration;

namespace NPipeline.Connectors.Azure.ServiceBus.Internal;

/// <summary>The client a node uses: the one in its options, or one it creates and owns.</summary>
internal static class ServiceBusClients
{
    public static (ServiceBusClient Client, bool Owned) For(ServiceBusNodeOptions options)
    {
        if (options.Client is { } client)
            return (client, false);

        var clientOptions = new ServiceBusClientOptions();

        if (options.Retry is { } retry)
            clientOptions.RetryOptions = retry;

        return options.ConnectionString is { Length: > 0 } connectionString
            ? (new ServiceBusClient(connectionString, clientOptions), true)
            : (new ServiceBusClient(options.FullyQualifiedNamespace, options.Credential ?? new DefaultAzureCredential(), clientOptions), true);
    }
}

using Azure;
using Azure.Identity;
using Azure.Messaging.ServiceBus;
using NPipeline.Connectors.Azure.Configuration;
using NPipeline.Connectors.Azure.ServiceBus.Configuration;
using NPipeline.Connectors.Azure.ServiceBus.Exceptions;

namespace NPipeline.Connectors.Azure.ServiceBus.Connection;

/// <summary>
///     Creates <see cref="ServiceBusClient" /> instances from connector configuration.
///     Supports connection string, Azure AD credential, and named connection modes.
/// </summary>
public static class ServiceBusClientFactory
{
    /// <summary>
    ///     Creates a <see cref="ServiceBusClient" /> according to
    ///     <paramref name="configuration" />, optionally resolving named connections from
    ///     <paramref name="azureConnections" />.
    /// </summary>
    public static ServiceBusClient Create(
        ServiceBusConfiguration configuration,
        AzureConnectionOptions? azureConnections = null)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var retryOptions = configuration.Retry.ToRetryOptions();
        var clientOptions = new ServiceBusClientOptions { RetryOptions = retryOptions };

        // Named connection overrides inline settings
        if (!string.IsNullOrWhiteSpace(configuration.NamedConnection))
        {
            if (azureConnections == null)
            {
                throw new ServiceBusConnectionException(
                    $"Named connection '{configuration.NamedConnection}' requires an AzureConnectionOptions instance.");
            }

            // Try connection string
            var namedString = azureConnections.GetConnectionString(configuration.NamedConnection);

            if (namedString != null)
                return new ServiceBusClient(namedString, clientOptions);

            // Try endpoint
            var endpoint = azureConnections.GetEndpoint(configuration.NamedConnection);

            if (endpoint?.Endpoint != null)
            {
                var credential = endpoint.Credential ?? new DefaultAzureCredential();
                return new ServiceBusClient(endpoint.Endpoint.Host, credential, clientOptions);
            }

            throw new ServiceBusConnectionException(
                $"Named connection '{configuration.NamedConnection}' was not found in AzureConnectionOptions.");
        }

        return configuration.AuthenticationMode switch
        {
            AzureAuthenticationMode.ConnectionString =>
                CreateFromConnectionString(configuration, clientOptions),
            AzureAuthenticationMode.AzureAdCredential =>
                CreateFromAzureAd(configuration, clientOptions),
            AzureAuthenticationMode.EndpointWithKey =>
                CreateFromEndpointWithKey(configuration, clientOptions),
            _ => throw new ServiceBusConnectionException(
                $"Unsupported authentication mode: {configuration.AuthenticationMode}."),
        };
    }

    private static ServiceBusClient CreateFromConnectionString(
        ServiceBusConfiguration configuration,
        ServiceBusClientOptions clientOptions)
    {
        if (string.IsNullOrWhiteSpace(configuration.ConnectionString))
        {
            throw new ServiceBusConnectionException(
                "ConnectionString must be specified when AuthenticationMode is ConnectionString.");
        }

        return new ServiceBusClient(configuration.ConnectionString, clientOptions);
    }

    private static ServiceBusClient CreateFromAzureAd(
        ServiceBusConfiguration configuration,
        ServiceBusClientOptions clientOptions)
    {
        if (string.IsNullOrWhiteSpace(configuration.FullyQualifiedNamespace))
        {
            throw new ServiceBusConnectionException(
                "FullyQualifiedNamespace must be specified for Azure AD authentication.");
        }

        var credential = configuration.Credential ?? new DefaultAzureCredential();

        return new ServiceBusClient(
            configuration.FullyQualifiedNamespace,
            credential,
            clientOptions);
    }

    private static ServiceBusClient CreateFromEndpointWithKey(
        ServiceBusConfiguration configuration,
        ServiceBusClientOptions clientOptions)
    {
        if (string.IsNullOrWhiteSpace(configuration.FullyQualifiedNamespace))
        {
            throw new ServiceBusConnectionException(
                "FullyQualifiedNamespace must be specified for endpoint-with-key authentication.");
        }

        if (string.IsNullOrWhiteSpace(configuration.SharedAccessKeyName))
        {
            throw new ServiceBusConnectionException(
                "SharedAccessKeyName must be specified for endpoint-with-key authentication.");
        }

        if (string.IsNullOrWhiteSpace(configuration.SharedAccessKey))
        {
            throw new ServiceBusConnectionException(
                "SharedAccessKey must be specified for endpoint-with-key authentication.");
        }

        var keyCredential = new AzureNamedKeyCredential(
            configuration.SharedAccessKeyName,
            configuration.SharedAccessKey);

        return new ServiceBusClient(
            configuration.FullyQualifiedNamespace,
            keyCredential,
            clientOptions);
    }

    /// <summary>
    ///     Produces a string key that uniquely identifies a Service Bus connection (for pooling).
    /// </summary>
    public static string GetConnectionKey(ServiceBusConfiguration configuration)
    {
        return !string.IsNullOrWhiteSpace(configuration.NamedConnection)
            ? $"named:{configuration.NamedConnection}"
            : !string.IsNullOrWhiteSpace(configuration.ConnectionString)
                ? $"cs:{configuration.ConnectionString}"
                : configuration.AuthenticationMode == AzureAuthenticationMode.EndpointWithKey
                    ? $"nsk:{configuration.FullyQualifiedNamespace}:{configuration.SharedAccessKeyName}"
                    : $"ns:{configuration.FullyQualifiedNamespace}:{configuration.AuthenticationMode}";
    }
}

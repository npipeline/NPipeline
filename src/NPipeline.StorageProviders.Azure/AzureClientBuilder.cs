using Azure;
using Azure.Core;
using Azure.Storage;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Azure;

/// <summary>The routing data that identifies one Azure storage endpoint. It never holds a secret.</summary>
/// <param name="AccountName">The storage account name, when known.</param>
/// <param name="ServiceUrl">The absolute service URL, when one is set.</param>
internal readonly record struct AzureEndpointKey(string? AccountName, string? ServiceUrl);

/// <summary>How to construct one kind of Azure SDK service client from each kind of credential.</summary>
internal sealed class AzureClientConstructors<TClient>
{
    public required Func<string, TClient> FromConnectionString { get; init; }
    public required Func<Uri, AzureSasCredential, TClient> FromSas { get; init; }
    public required Func<Uri, StorageSharedKeyCredential, TClient> FromSharedKey { get; init; }
    public required Func<Uri, TokenCredential, TClient> FromToken { get; init; }
    public required Func<Uri, TClient> Anonymous { get; init; }
}

/// <summary>Resolves an endpoint from a URI and builds clients for it. Shared by the Blob and ADLS Gen2 providers.</summary>
internal static class AzureClientBuilder
{
    private static readonly (string Parameter, string Option)[] SecretParameters =
    [
        ("connectionString", nameof(AzureAccountOptions.DefaultConnectionString)),
        ("sasToken", nameof(AzureAccountOptions.DefaultSasToken)),
        ("accountKey", nameof(AzureAccountOptions.DefaultAccountKey)),
    ];

    /// <summary>Reads the routing data from <paramref name="uri" />, falling back to the options.</summary>
    /// <exception cref="ArgumentException">The URI carries a credential, or an invalid service URL.</exception>
    public static AzureEndpointKey GetEndpoint(StorageUri uri, AzureAccountOptions options)
    {
        ArgumentNullException.ThrowIfNull(uri);
        ArgumentNullException.ThrowIfNull(options);

        foreach (var (parameter, option) in SecretParameters)
        {
            if (uri.Parameters.ContainsKey(parameter))
            {
                throw new ArgumentException(
                    $"The '{parameter}' URI parameter is no longer supported, because URIs are logged and compared. Set {option} in the provider options instead.",
                    nameof(uri));
            }
        }

        var accountName = uri.Parameters.TryGetValue("accountName", out var name) && !string.IsNullOrWhiteSpace(name)
            ? name
            : options.AccountName;

        Uri? serviceUrl;

        if (uri.Parameters.TryGetValue("serviceUrl", out var text) && !string.IsNullOrEmpty(text))
        {
            serviceUrl = Uri.TryCreate(text, UriKind.Absolute, out var parsed)
                ? parsed
                : throw new ArgumentException($"Invalid service URL: {text}", nameof(uri));
        }
        else
        {
            serviceUrl = options.ServiceUrl;
        }

        return new AzureEndpointKey(accountName, serviceUrl?.AbsoluteUri);
    }

    /// <summary>Builds a client for <paramref name="endpoint" /> using the credentials in <paramref name="options" />.</summary>
    /// <param name="options">The provider options.</param>
    /// <param name="endpoint">The endpoint.</param>
    /// <param name="hostSuffix">The public host suffix, such as <c>blob.core.windows.net</c>, used when no service URL is set.</param>
    /// <param name="constructors">How to construct the client.</param>
    /// <param name="mapServiceUrl">Optional mapping applied to an explicit service URL.</param>
    public static TClient Build<TClient>(
        AzureAccountOptions options,
        AzureEndpointKey endpoint,
        string hostSuffix,
        AzureClientConstructors<TClient> constructors,
        Func<Uri, Uri>? mapServiceUrl = null)
    {
        // A connection string carries its own endpoint and credentials, so it cannot be combined with a service URL:
        // ignoring either one would connect somewhere, or as someone, the caller did not ask for.
        if (!string.IsNullOrEmpty(options.DefaultConnectionString))
        {
            if (endpoint.ServiceUrl is not null)
            {
                throw new ArgumentException(
                    "A service URL cannot be combined with a connection string. Remove 'serviceUrl' (or ServiceUrl) or the connection string.");
            }

            return constructors.FromConnectionString(options.DefaultConnectionString);
        }

        Uri url;

        if (endpoint.ServiceUrl is not null)
        {
            url = new Uri(endpoint.ServiceUrl);
            url = mapServiceUrl is null ? url : mapServiceUrl(url);
        }
        else if (!string.IsNullOrEmpty(endpoint.AccountName))
        {
            url = new Uri($"https://{endpoint.AccountName}.{hostSuffix}");
        }
        else
        {
            throw new InvalidOperationException("Account name must be provided when not using connection string or custom service URL.");
        }

        if (!string.IsNullOrWhiteSpace(options.DefaultSasToken))
            return constructors.FromSas(url, new AzureSasCredential(options.DefaultSasToken));

        if (!string.IsNullOrWhiteSpace(options.DefaultAccountKey))
        {
            if (string.IsNullOrWhiteSpace(endpoint.AccountName))
                throw new ArgumentException("An account name must be provided when using an account key.");

            return constructors.FromSharedKey(url, new StorageSharedKeyCredential(endpoint.AccountName, options.DefaultAccountKey));
        }

        var token = options.DefaultCredential ?? (options.UseDefaultCredentialChain ? options.DefaultCredentialChain : null);

        if (token is not null)
            return constructors.FromToken(url, token);

        if (!options.AllowAnonymousAccess)
        {
            throw new InvalidOperationException(
                "No Azure credentials are available. Set a connection string, SAS token, account key or token credential in the provider options, " +
                "enable UseDefaultCredentialChain, or set AllowAnonymousAccess to read public containers.");
        }

        return constructors.Anonymous(url);
    }
}

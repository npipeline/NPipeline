using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using Azure;
using Azure.Core;
using Azure.Storage;
using Azure.Storage.Blobs;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Azure;

/// <summary>
///     Factory for creating and caching Azure Blob Service clients with flexible authentication options.
/// </summary>
public class AzureBlobClientFactory
{
    private readonly ConcurrentDictionary<string, BlobServiceClient> _clientCache = new();
    private readonly ConcurrentQueue<string> _clientKeyQueue = new();
    private readonly AzureBlobStorageProviderOptions _options;

    /// <summary>
    ///     Initializes a new instance of the <see cref="AzureBlobClientFactory" /> class.
    /// </summary>
    /// <param name="options">The Azure storage provider options.</param>
    public AzureBlobClientFactory(AzureBlobStorageProviderOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>
    ///     Gets or creates an Azure Blob Service client for the specified storage URI.
    /// </summary>
    /// <param name="uri">The storage URI containing container, account, and optional credentials.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a <see cref="BlobServiceClient" />.</returns>
    public virtual Task<BlobServiceClient> GetClientAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var accountName = GetAccountName(uri);
        var connectionString = GetConnectionString(uri);
        var credentialInfo = GetCredentialInfo(uri, accountName);
        var serviceUrl = GetServiceUrl(uri);

        return GetClientAsync(connectionString, credentialInfo, serviceUrl, accountName, cancellationToken);
    }

    /// <summary>
    ///     Gets or creates an Azure Blob Service client with the specified configuration.
    /// </summary>
    /// <param name="connectionString">Optional connection string for Azure Storage.</param>
    /// <param name="credentialInfo">Credential information for authentication.</param>
    /// <param name="serviceUrl">Optional service URL for Azure Storage-compatible endpoints.</param>
    /// <param name="accountName">The storage account name.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a <see cref="BlobServiceClient" />.</returns>
    private Task<BlobServiceClient> GetClientAsync(
        string? connectionString,
        CredentialInfo? credentialInfo,
        Uri? serviceUrl,
        string? accountName,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var cacheKey = BuildCacheKey(connectionString, credentialInfo, serviceUrl, accountName);
        var clientOptions = CreateClientOptions();

        var client = _clientCache.GetOrAdd(cacheKey, _ =>
        {
            _clientKeyQueue.Enqueue(cacheKey);

            // A connection string carries its own endpoint and credentials, so it cannot be combined with a service URL:
            // ignoring either one would connect somewhere, or as someone, the caller did not ask for.
            if (!string.IsNullOrEmpty(connectionString))
            {
                if (serviceUrl is not null)
                {
                    throw new ArgumentException(
                        "A service URL cannot be combined with a connection string. Remove 'serviceUrl' (or ServiceUrl) or the connection string.");
                }

                return clientOptions is null
                    ? new BlobServiceClient(connectionString)
                    : new BlobServiceClient(connectionString, clientOptions);
            }

            // Build service URL if not provided
            var effectiveServiceUrl = serviceUrl ?? BuildDefaultServiceUrl(accountName ?? credentialInfo?.AccountName);

            // Handle SAS token credential
            if (credentialInfo?.SasToken is not null)
            {
                var sasCredential = new AzureSasCredential(credentialInfo.SasToken);

                return clientOptions is null
                    ? new BlobServiceClient(effectiveServiceUrl, sasCredential)
                    : new BlobServiceClient(effectiveServiceUrl, sasCredential, clientOptions);
            }

            // Handle account key credential
            if (credentialInfo?.AccountKey is not null && credentialInfo.AccountName is not null)
            {
                var keyCredential = new StorageSharedKeyCredential(credentialInfo.AccountName, credentialInfo.AccountKey);

                return clientOptions is null
                    ? new BlobServiceClient(effectiveServiceUrl, keyCredential)
                    : new BlobServiceClient(effectiveServiceUrl, keyCredential, clientOptions);
            }

            // Handle token credential (DefaultAzureCredential or custom TokenCredential)
            if (credentialInfo?.TokenCredential is not null)
            {
                return clientOptions is null
                    ? new BlobServiceClient(effectiveServiceUrl, credentialInfo.TokenCredential)
                    : new BlobServiceClient(effectiveServiceUrl, credentialInfo.TokenCredential, clientOptions);
            }

            if (!_options.AllowAnonymousAccess)
            {
                throw new InvalidOperationException(
                    "No Azure credentials are available. Provide a connection string, SAS token, account key or token credential, " +
                    "enable UseDefaultCredentialChain, or set AllowAnonymousAccess to read public containers.");
            }

            return clientOptions is null
                ? new BlobServiceClient(effectiveServiceUrl)
                : new BlobServiceClient(effectiveServiceUrl, clientOptions);
        });

        EnforceCacheLimit();

        return Task.FromResult(client);
    }

    /// <summary>
    ///     Extracts connection string from the storage URI or returns default connection string.
    /// </summary>
    /// <param name="uri">The storage URI.</param>
    /// <returns>The connection string, or null if not specified.</returns>
    private string? GetConnectionString(StorageUri uri)
    {
        if (uri.Parameters.TryGetValue("connectionString", out var connectionString) &&
            !string.IsNullOrWhiteSpace(connectionString))
            return connectionString;

        return _options.DefaultConnectionString;
    }

    /// <summary>
    ///     Extracts credential information from the storage URI or returns default credential.
    /// </summary>
    /// <param name="uri">The storage URI.</param>
    /// <param name="accountName">The storage account name.</param>
    /// <returns>The credential information, or null if using connection string.</returns>
    private CredentialInfo? GetCredentialInfo(StorageUri uri, string? accountName)
    {
        // Check for explicit connection string first (takes precedence) - must be non-empty to count
        if ((uri.Parameters.TryGetValue("connectionString", out var connectionString) && !string.IsNullOrWhiteSpace(connectionString))
            || !string.IsNullOrEmpty(_options.DefaultConnectionString))
            return null;

        // Check for SAS token
        if (uri.Parameters.TryGetValue("sasToken", out var sasToken) && !string.IsNullOrWhiteSpace(sasToken))
        {
            return new CredentialInfo
            {
                SasToken = sasToken,
                AccountName = accountName,
            };
        }

        // Check for account key
        if (uri.Parameters.TryGetValue("accountKey", out var accountKey) && !string.IsNullOrWhiteSpace(accountKey))
        {
            if (string.IsNullOrWhiteSpace(accountName))
                throw new ArgumentException("accountName must be provided when using accountKey.", nameof(uri));

            return new CredentialInfo
            {
                AccountKey = accountKey,
                AccountName = accountName,
            };
        }

        // Use default credential from options
        if (_options.DefaultCredential is not null)
        {
            return new CredentialInfo
            {
                TokenCredential = _options.DefaultCredential,
                AccountName = accountName,
            };
        }

        // Use default credential chain if enabled
        if (_options.UseDefaultCredentialChain)
        {
            return new CredentialInfo
            {
                TokenCredential = _options.DefaultCredentialChain,
                AccountName = accountName,
            };
        }

        return null;
    }

    /// <summary>
    ///     Extracts the storage account name from the URI or parameters.
    /// </summary>
    /// <param name="uri">The storage URI.</param>
    /// <returns>The storage account name.</returns>
    private string? GetAccountName(StorageUri uri)
    {
        if (uri.Parameters.TryGetValue("accountName", out var accountName) && !string.IsNullOrWhiteSpace(accountName))
            return accountName;

        return null;
    }

    /// <summary>
    ///     Extracts the service URL from the storage URI or returns default service URL.
    /// </summary>
    /// <param name="uri">The storage URI.</param>
    /// <returns>The service URL, or null if using default Azure endpoint.</returns>
    private Uri? GetServiceUrl(StorageUri uri)
    {
        if (uri.Parameters.TryGetValue("serviceUrl", out var serviceUrlString) &&
            !string.IsNullOrEmpty(serviceUrlString))
        {
            // StorageUri has already decoded the parameter; decoding again would corrupt values containing '%'.
            var decoded = serviceUrlString;

            if (Uri.TryCreate(decoded, UriKind.Absolute, out var serviceUrl))
                return serviceUrl;

            throw new ArgumentException($"Invalid service URL: {serviceUrlString}", nameof(uri));
        }

        return _options.ServiceUrl;
    }

    /// <summary>
    ///     Builds a default Azure Storage service URL for the specified account.
    /// </summary>
    /// <param name="accountName">The storage account name.</param>
    /// <returns>A URI for the Azure Blob Storage service.</returns>
    private static Uri BuildDefaultServiceUrl(string? accountName)
    {
        if (string.IsNullOrEmpty(accountName))
            throw new InvalidOperationException("Account name must be provided when not using connection string or custom service URL.");

        return new Uri($"https://{accountName}.blob.core.windows.net");
    }

    /// <summary>
    ///     Builds a cache key for the client configuration.
    /// </summary>
    private static string BuildCacheKey(
        string? connectionString,
        CredentialInfo? credentialInfo,
        Uri? serviceUrl,
        string? accountName)
    {
        // When a connection string is provided we ignore serviceUrl for caching purposes
        if (!string.IsNullOrEmpty(connectionString))
            return $"connection-string|{ComputeStableHash(connectionString)}";

        var endpointKey = serviceUrl?.ToString() ?? "default";

        if (credentialInfo is null)
        {
            var accountKey = string.IsNullOrEmpty(accountName)
                ? "default"
                : $"account:{accountName}";

            return string.Join("|", endpointKey, accountKey);
        }

        return string.Join("|", endpointKey, BuildCredentialKey(credentialInfo));
    }

    private void EnforceCacheLimit()
    {
        var limit = _options.ClientCacheSizeLimit;

        while (_clientCache.Count > limit && _clientKeyQueue.TryDequeue(out var staleKey))
        {
            _clientCache.TryRemove(staleKey, out _);
        }
    }

    private BlobClientOptions? CreateClientOptions()
    {
        if (_options.ServiceVersion is null)
            return null;

        return new BlobClientOptions(_options.ServiceVersion.Value);
    }

    private static string BuildCredentialKey(CredentialInfo credentialInfo)
    {
        var components = new List<string>();

        if (credentialInfo.TokenCredential is not null)
            components.Add($"token:{credentialInfo.TokenCredential.GetType().FullName}");

        if (credentialInfo.SasToken is not null)
            components.Add($"sas:{ComputeStableHash(credentialInfo.SasToken)}");

        if (credentialInfo.AccountKey is not null)
            components.Add($"key:{ComputeStableHash(credentialInfo.AccountKey)}");

        if (credentialInfo.AccountName is not null)
            components.Add($"account:{credentialInfo.AccountName}");

        return string.Join(";", components);
    }

    /// <summary>
    ///     Internal class to hold credential information.
    /// </summary>
    // The full secret is hashed: SAS tokens share long prefixes ("sv=2022-11-02&..."), so any prefix would collide.
    private static string ComputeStableHash(string value) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value)));

    private sealed class CredentialInfo
    {
        public TokenCredential? TokenCredential { get; init; }
        public string? SasToken { get; init; }
        public string? AccountKey { get; init; }
        public string? AccountName { get; init; }
    }
}

using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using Azure;
using Azure.Core;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Adls;

/// <summary>
///     Factory for creating and caching ADLS Gen2 Service clients with flexible authentication options.
/// </summary>
public class AdlsGen2ClientFactory
{
    private readonly ConcurrentDictionary<string, DataLakeServiceClient> _clientCache = new();
    private readonly ConcurrentDictionary<string, BlobServiceClient> _blobClientCache = new();
    private readonly ConcurrentQueue<string> _clientKeyQueue = new();
    private readonly AdlsGen2StorageProviderOptions _options;

    /// <summary>
    ///     Initializes a new instance of the <see cref="AdlsGen2ClientFactory" /> class.
    /// </summary>
    /// <param name="options">The ADLS Gen2 storage provider options.</param>
    public AdlsGen2ClientFactory(AdlsGen2StorageProviderOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>
    ///     Gets or creates an ADLS Gen2 Service client for the specified storage URI.
    /// </summary>
    /// <param name="uri">The storage URI containing filesystem, account, and optional credentials.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a <see cref="DataLakeServiceClient" />.</returns>
    public virtual Task<DataLakeServiceClient> GetClientAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var accountName = GetAccountName(uri);
        var connectionString = GetConnectionString(uri);
        var credentialInfo = GetCredentialInfo(uri, accountName);
        var serviceUrl = GetServiceUrl(uri);

        return GetClientAsync(connectionString, credentialInfo, serviceUrl, accountName, cancellationToken);
    }

    /// <summary>
    ///     Gets or creates a <see cref="BlobServiceClient" /> for the specified storage URI.
    ///     Uses the Blob API endpoint which is compatible with all storage configurations including
    ///     Azurite emulator.
    /// </summary>
    /// <param name="uri">The storage URI containing filesystem, account, and optional credentials.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a <see cref="BlobServiceClient" />.</returns>
    public virtual Task<BlobServiceClient> GetBlobServiceClientAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var accountName = GetAccountName(uri);
        var connectionString = GetConnectionString(uri);
        var credentialInfo = GetCredentialInfo(uri, accountName);
        var serviceUrl = GetServiceUrl(uri);

        return GetBlobServiceClientAsync(connectionString, credentialInfo, serviceUrl, accountName, cancellationToken);
    }

    private Task<BlobServiceClient> GetBlobServiceClientAsync(
        string? connectionString,
        CredentialInfo? credentialInfo,
        Uri? serviceUrl,
        string? accountName,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var cacheKey = "blob|" + BuildCacheKey(connectionString, credentialInfo, serviceUrl, accountName);

        var client = _blobClientCache.GetOrAdd(cacheKey, _ =>
        {
            if (serviceUrl is null && !string.IsNullOrEmpty(connectionString))
                return new BlobServiceClient(connectionString);

            var effectiveServiceUrl = serviceUrl is not null
                ? new Uri(serviceUrl.ToString().Replace(".dfs.core.windows.net", ".blob.core.windows.net"))
                : new Uri($"https://{accountName ?? credentialInfo?.AccountName}.blob.core.windows.net");

            if (credentialInfo?.SasToken is not null)
                return new BlobServiceClient(effectiveServiceUrl, new AzureSasCredential(credentialInfo.SasToken));

            if (credentialInfo?.AccountKey is not null && credentialInfo.AccountName is not null)
                return new BlobServiceClient(effectiveServiceUrl, new StorageSharedKeyCredential(credentialInfo.AccountName, credentialInfo.AccountKey));

            if (credentialInfo?.TokenCredential is not null)
                return new BlobServiceClient(effectiveServiceUrl, credentialInfo.TokenCredential);

            return new BlobServiceClient(effectiveServiceUrl);
        });

        return Task.FromResult(client);
    }

    /// <summary>
    ///     Gets or creates an ADLS Gen2 Service client with the specified configuration.
    /// </summary>
    /// <param name="connectionString">Optional connection string for Azure Storage.</param>
    /// <param name="credentialInfo">Credential information for authentication.</param>
    /// <param name="serviceUrl">Optional service URL for Azure Storage-compatible endpoints.</param>
    /// <param name="accountName">The storage account name.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a <see cref="DataLakeServiceClient" />.</returns>
    private Task<DataLakeServiceClient> GetClientAsync(
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

            // If a serviceUrl override is provided, prefer building the client with that endpoint and credentials,
            // even when a connection string exists. This mirrors S3 behavior where URI parameters can override defaults
            // and avoids parsing potentially placeholder connection strings in tests.
            if (serviceUrl is null && !string.IsNullOrEmpty(connectionString))
            {
                return clientOptions is null
                    ? new DataLakeServiceClient(connectionString)
                    : new DataLakeServiceClient(connectionString, clientOptions);
            }

            // Build service URL if not provided
            var effectiveServiceUrl = serviceUrl ?? BuildDefaultServiceUrl(accountName ?? credentialInfo?.AccountName);

            // Handle SAS token credential
            if (credentialInfo?.SasToken is not null)
            {
                var sasCredential = new AzureSasCredential(credentialInfo.SasToken);
                return clientOptions is null
                    ? new DataLakeServiceClient(effectiveServiceUrl, sasCredential)
                    : new DataLakeServiceClient(effectiveServiceUrl, sasCredential, clientOptions);
            }

            // Handle account key credential
            if (credentialInfo?.AccountKey is not null && credentialInfo.AccountName is not null)
            {
                var keyCredential = new StorageSharedKeyCredential(credentialInfo.AccountName, credentialInfo.AccountKey);
                return clientOptions is null
                    ? new DataLakeServiceClient(effectiveServiceUrl, keyCredential)
                    : new DataLakeServiceClient(effectiveServiceUrl, keyCredential, clientOptions);
            }

            // Handle token credential (DefaultAzureCredential or custom TokenCredential)
            if (credentialInfo?.TokenCredential is not null)
            {
                return clientOptions is null
                    ? new DataLakeServiceClient(effectiveServiceUrl, credentialInfo.TokenCredential)
                    : new DataLakeServiceClient(effectiveServiceUrl, credentialInfo.TokenCredential, clientOptions);
            }

            // No credentials provided - use anonymous access
            return clientOptions is null
                ? new DataLakeServiceClient(effectiveServiceUrl)
                : new DataLakeServiceClient(effectiveServiceUrl, clientOptions);
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
            return Uri.UnescapeDataString(connectionString);

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
                SasToken = Uri.UnescapeDataString(sasToken),
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
                AccountKey = Uri.UnescapeDataString(accountKey),
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
            return Uri.UnescapeDataString(accountName);

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
            var decoded = Uri.UnescapeDataString(serviceUrlString);

            if (Uri.TryCreate(decoded, UriKind.Absolute, out var serviceUrl))
                return serviceUrl;

            throw new ArgumentException($"Invalid service URL: {serviceUrlString}", nameof(uri));
        }

        return _options.ServiceUrl;
    }

    /// <summary>
    ///     Builds a default Azure Data Lake Storage service URL for the specified account.
    /// </summary>
    /// <param name="accountName">The storage account name.</param>
    /// <returns>A URI for the Azure Data Lake Storage service.</returns>
    private static Uri BuildDefaultServiceUrl(string? accountName)
    {
        if (string.IsNullOrEmpty(accountName))
            throw new InvalidOperationException("Account name must be provided when not using connection string or custom service URL.");

        return new Uri($"https://{accountName}.dfs.core.windows.net");
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
            return $"connection-string:{ComputeStableHash(connectionString)}";

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

    private DataLakeClientOptions? CreateClientOptions()
    {
        var options = _options.ServiceVersion is null
            ? new DataLakeClientOptions()
            : new DataLakeClientOptions(_options.ServiceVersion.Value);

        options.Retry.Mode = RetryMode.Exponential;
        options.Retry.MaxRetries = 5;
        options.Retry.Delay = TimeSpan.FromMilliseconds(800);
        options.Retry.MaxDelay = TimeSpan.FromSeconds(8);
        options.Retry.NetworkTimeout = TimeSpan.FromSeconds(100);

        return options;
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

    private static string ComputeStableHash(string value)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(value));
        return Convert.ToHexString(bytes);
    }

    /// <summary>
    ///     Internal class to hold credential information.
    /// </summary>
    private sealed class CredentialInfo
    {
        public TokenCredential? TokenCredential { get; init; }
        public string? SasToken { get; init; }
        public string? AccountKey { get; init; }
        public string? AccountName { get; init; }
    }
}

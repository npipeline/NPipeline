using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake;
using NPipeline.StorageProviders.Azure;
using NPipeline.StorageProviders.Models;
using NPipeline.StorageProviders.Utilities;

namespace NPipeline.StorageProviders.Adls;

/// <summary>
///     Creates and caches ADLS Gen2 clients, one Data Lake client and one Blob client per endpoint. Credentials come from
///     <see cref="AdlsGen2StorageProviderOptions" />; the URI only selects the endpoint.
/// </summary>
public class AdlsGen2ClientFactory : IDisposable
{
    private readonly ClientCache<AzureEndpointKey, BlobServiceClient> _blobClients;
    private readonly ClientCache<AzureEndpointKey, DataLakeServiceClient> _clients;
    private readonly AdlsGen2StorageProviderOptions _options;

    /// <summary>
    ///     Initializes a new instance of the <see cref="AdlsGen2ClientFactory" /> class.
    /// </summary>
    /// <param name="options">The ADLS Gen2 storage provider options.</param>
    public AdlsGen2ClientFactory(AdlsGen2StorageProviderOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _clients = new ClientCache<AzureEndpointKey, DataLakeServiceClient>(options.ClientCacheSizeLimit);
        _blobClients = new ClientCache<AzureEndpointKey, BlobServiceClient>(options.ClientCacheSizeLimit);
    }

    /// <summary>
    ///     Gets or creates an ADLS Gen2 Service client for the endpoint the storage URI selects.
    /// </summary>
    /// <param name="uri">The storage URI. It may carry <c>accountName</c> and <c>serviceUrl</c> parameters, never credentials.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a <see cref="DataLakeServiceClient" />.</returns>
    public virtual Task<DataLakeServiceClient> GetClientAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var endpoint = AzureClientBuilder.GetEndpoint(uri, _options);

        return Task.FromResult(_clients.GetOrCreate(endpoint, CreateDataLakeClient));
    }

    /// <summary>
    ///     Gets or creates a <see cref="BlobServiceClient" /> for the endpoint the storage URI selects. It uses the Blob API
    ///     endpoint, which is compatible with all storage configurations including the Azurite emulator.
    /// </summary>
    /// <param name="uri">The storage URI. It may carry <c>accountName</c> and <c>serviceUrl</c> parameters, never credentials.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a <see cref="BlobServiceClient" />.</returns>
    public virtual Task<BlobServiceClient> GetBlobServiceClientAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var endpoint = AzureClientBuilder.GetEndpoint(uri, _options);

        return Task.FromResult(_blobClients.GetOrCreate(endpoint, CreateBlobClient));
    }

    /// <summary>Releases the cached clients.</summary>
    public void Dispose()
    {
        _clients.Dispose();
        _blobClients.Dispose();
        GC.SuppressFinalize(this);
    }

    private DataLakeServiceClient CreateDataLakeClient(AzureEndpointKey endpoint)
    {
        var clientOptions = CreateClientOptions();

        return AzureClientBuilder.Build(
            _options,
            endpoint,
            "dfs.core.windows.net",
            new AzureClientConstructors<DataLakeServiceClient>
            {
                FromConnectionString = text => new DataLakeServiceClient(text, clientOptions),
                FromSas = (url, credential) => new DataLakeServiceClient(url, credential, clientOptions),
                FromSharedKey = (url, credential) => new DataLakeServiceClient(url, credential, clientOptions),
                FromToken = (url, credential) => new DataLakeServiceClient(url, credential, clientOptions),
                Anonymous = url => new DataLakeServiceClient(url, clientOptions),
            });
    }

    private BlobServiceClient CreateBlobClient(AzureEndpointKey endpoint)
    {
        var clientOptions = CreateBlobClientOptions();

        return AzureClientBuilder.Build(
            _options,
            endpoint,
            "blob.core.windows.net",
            new AzureClientConstructors<BlobServiceClient>
            {
                FromConnectionString = text => new BlobServiceClient(text, clientOptions),
                FromSas = (url, credential) => new BlobServiceClient(url, credential, clientOptions),
                FromSharedKey = (url, credential) => new BlobServiceClient(url, credential, clientOptions),
                FromToken = (url, credential) => new BlobServiceClient(url, credential, clientOptions),
                Anonymous = url => new BlobServiceClient(url, clientOptions),
            },
            static url => new Uri(url.ToString().Replace(".dfs.core.windows.net", ".blob.core.windows.net", StringComparison.OrdinalIgnoreCase)));
    }

    internal DataLakeClientOptions CreateClientOptions()
    {
        var options = _options.ServiceVersion is null
            ? new DataLakeClientOptions()
            : new DataLakeClientOptions(_options.ServiceVersion.Value);

        _options.Retry.ApplyTo(options.Retry);

        return options;
    }

    internal BlobClientOptions CreateBlobClientOptions()
    {
        // Match the Blob API version to the configured Data Lake version when there is an equivalent
        var options = _options.ServiceVersion is not null
                      && Enum.TryParse<BlobClientOptions.ServiceVersion>(_options.ServiceVersion.Value.ToString(), out var blobServiceVersion)
            ? new BlobClientOptions(blobServiceVersion)
            : new BlobClientOptions();

        _options.Retry.ApplyTo(options.Retry);

        return options;
    }
}

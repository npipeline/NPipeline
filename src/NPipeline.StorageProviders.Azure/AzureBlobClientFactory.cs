using Azure.Storage.Blobs;
using NPipeline.StorageProviders.Models;
using NPipeline.StorageProviders.Utilities;

namespace NPipeline.StorageProviders.Azure;

/// <summary>
///     Creates and caches Azure Blob Service clients, one per endpoint. Credentials come from
///     <see cref="AzureBlobStorageProviderOptions" />; the URI only selects the endpoint.
/// </summary>
public class AzureBlobClientFactory : IDisposable
{
    private readonly ClientCache<AzureEndpointKey, BlobServiceClient> _clients;
    private readonly AzureBlobStorageProviderOptions _options;

    /// <summary>
    ///     Initializes a new instance of the <see cref="AzureBlobClientFactory" /> class.
    /// </summary>
    /// <param name="options">The Azure storage provider options.</param>
    public AzureBlobClientFactory(AzureBlobStorageProviderOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _clients = new ClientCache<AzureEndpointKey, BlobServiceClient>(options.ClientCacheSizeLimit);
    }

    /// <summary>
    ///     Gets or creates an Azure Blob Service client for the endpoint the storage URI selects.
    /// </summary>
    /// <param name="uri">The storage URI. It may carry <c>accountName</c> and <c>serviceUrl</c> parameters, never credentials.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a <see cref="BlobServiceClient" />.</returns>
    public virtual Task<BlobServiceClient> GetClientAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var endpoint = AzureClientBuilder.GetEndpoint(uri, _options);

        return Task.FromResult(_clients.GetOrCreate(endpoint, Create));
    }

    /// <summary>Releases the cached clients.</summary>
    public void Dispose()
    {
        _clients.Dispose();
        GC.SuppressFinalize(this);
    }

    private BlobServiceClient Create(AzureEndpointKey endpoint)
    {
        var clientOptions = CreateClientOptions();

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
            });
    }

    private BlobClientOptions CreateClientOptions()
    {
        var options = _options.ServiceVersion is null
            ? new BlobClientOptions()
            : new BlobClientOptions(_options.ServiceVersion.Value);

        _options.Retry.ApplyTo(options.Retry);

        return options;
    }
}

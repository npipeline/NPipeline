using Amazon.S3;
using NPipeline.StorageProviders.Models;
using NPipeline.StorageProviders.Utilities;

namespace NPipeline.StorageProviders.S3;

/// <summary>The routing data that identifies one S3 endpoint. It never holds a secret.</summary>
/// <param name="Region">The AWS region system name, or <see langword="null" /> to let the SDK resolve it.</param>
/// <param name="ServiceUrl">The absolute service URL, or <see langword="null" /> for AWS.</param>
/// <param name="ForcePathStyle">Whether path-style addressing is forced.</param>
public readonly record struct S3EndpointKey(string? Region, string? ServiceUrl, bool ForcePathStyle);

/// <summary>
///     Abstract base class for creating and caching Amazon S3 clients, one per endpoint.
///     Subclasses are responsible for wiring credentials and configuration specific to their environment.
///     Credentials belong to the subclass's options, so the same ones apply to every client it creates.
/// </summary>
public abstract class S3ClientFactoryBase : IDisposable
{
    private readonly ClientCache<S3EndpointKey, IAmazonS3> _clients;

    /// <summary>Initializes a new instance of the <see cref="S3ClientFactoryBase" /> class.</summary>
    /// <param name="clientCacheLimit">The most endpoint clients kept in the cache.</param>
    protected S3ClientFactoryBase(int clientCacheLimit = 100)
    {
        _clients = new ClientCache<S3EndpointKey, IAmazonS3>(clientCacheLimit);
    }

    /// <summary>
    ///     Gets or creates an Amazon S3 client for the endpoint the storage URI selects.
    /// </summary>
    /// <param name="uri">The storage URI containing bucket and optional routing parameters.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing an <see cref="IAmazonS3" /> client.</returns>
    public virtual Task<IAmazonS3> GetClientAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        return Task.FromResult(_clients.GetOrCreate(GetEndpoint(uri), CreateClient));
    }

    /// <summary>
    ///     Reads the endpoint the storage URI selects. Throws for a URI that carries credentials or invalid routing data.
    /// </summary>
    /// <param name="uri">The storage URI.</param>
    /// <returns>The endpoint key.</returns>
    protected abstract S3EndpointKey GetEndpoint(StorageUri uri);

    /// <summary>
    ///     Creates an Amazon S3 client for an endpoint. Called once per endpoint.
    /// </summary>
    /// <param name="endpoint">The endpoint.</param>
    /// <returns>An <see cref="IAmazonS3" /> client.</returns>
    protected abstract IAmazonS3 CreateClient(S3EndpointKey endpoint);

    /// <summary>
    ///     Clears the client cache without disposing the clients. Useful for testing or when credentials change.
    /// </summary>
    public virtual void ClearCache()
    {
        _clients.Clear();
    }

    /// <summary>
    ///     Disposes every cached client and clears the cache.
    /// </summary>
    public void Dispose()
    {
        _clients.Dispose();
        GC.SuppressFinalize(this);
    }
}

using System.Collections.Concurrent;
using Amazon.S3;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.S3;

/// <summary>
///     Abstract base class for creating and caching Amazon S3 clients.
///     Subclasses are responsible for wiring credentials and configuration specific to their environment.
/// </summary>
public abstract class S3ClientFactoryBase
{
    private readonly ConcurrentDictionary<string, IAmazonS3> _clientCache = new();

    /// <summary>
    ///     Gets or creates an Amazon S3 client for the specified storage URI.
    /// </summary>
    /// <param name="uri">The storage URI containing bucket and optional configuration.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing an <see cref="IAmazonS3" /> client.</returns>
    public virtual Task<IAmazonS3> GetClientAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var cacheKey = BuildCacheKey(uri);
        var client = _clientCache.GetOrAdd(cacheKey, _ => CreateClient(uri));
        return Task.FromResult(client);
    }

    /// <summary>
    ///     Creates an Amazon S3 client for the specified storage URI.
    ///     Subclasses must implement this to provide environment-specific client configuration.
    /// </summary>
    /// <param name="uri">The storage URI.</param>
    /// <returns>An <see cref="IAmazonS3" /> client.</returns>
    protected abstract IAmazonS3 CreateClient(StorageUri uri);

    /// <summary>
    ///     Builds a cache key for the client based on the URI and configuration.
    ///     Subclasses can override this to include additional configuration in the cache key.
    /// </summary>
    /// <param name="uri">The storage URI.</param>
    /// <returns>A cache key string.</returns>
    protected virtual string BuildCacheKey(StorageUri uri)
    {
        return $"{GetType().FullName}|{uri.Host}";
    }

    /// <summary>
    ///     Clears the client cache. Useful for testing or when credentials change.
    /// </summary>
    public virtual void ClearCache()
    {
        _clientCache.Clear();
    }
}

using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Abstractions;

/// <summary>
///     Abstraction for storage backends (e.g., file system, S3, Azure Blob).
///     Data format connectors (CSV/Excel/etc.) use this interface to obtain streams
///     without taking a dependency on any specific storage implementation.
/// </summary>
/// <remarks>
///     Design goals:
///     - Decoupled from DI and concrete providers
///     - Async-first API
///     - Stream-based I/O for scalability
///     - Scheme-based routing via <see cref="Scheme" /> and <see cref="CanHandle(StorageUri)" />
/// </remarks>
public interface IStorageProvider
{
    /// <summary>
    ///     The primary URI scheme this provider targets (e.g. file, s3, azure).
    ///     Implementations may support additional schemes via <see cref="CanHandle(StorageUri)" />.
    /// </summary>
    StorageScheme Scheme { get; }

    /// <summary>
    ///     Indicates whether this provider can handle the specified <see cref="StorageUri" />.
    ///     Providers may support multiple schemes and conditional handling based on host/path/parameters.
    /// </summary>
    /// <param name="uri">The storage location to evaluate.</param>
    /// <returns>True if the provider can handle the given uri; otherwise false.</returns>
    bool CanHandle(StorageUri uri);

    /// <summary>
    ///     Opens a readable stream for the specified <see cref="StorageUri" />.
    ///     Caller is responsible for disposing the returned stream.
    /// </summary>
    /// <param name="uri">The storage location to read from.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a readable <see cref="System.IO.Stream" />.</returns>
    /// <exception cref="System.ArgumentNullException">If <paramref name="uri" /> is null.</exception>
    /// <exception cref="System.IO.FileNotFoundException">If the resource does not exist.</exception>
    /// <exception cref="System.UnauthorizedAccessException">If access is denied.</exception>
    /// <exception cref="System.IO.IOException">For other I/O related errors.</exception>
    Task<Stream> OpenReadAsync(
        StorageUri uri,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Opens a writable stream for the specified <see cref="StorageUri" />.
    ///     Caller is responsible for disposing and flushing the returned stream.
    /// </summary>
    /// <param name="uri">The storage location to write to.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a writable <see cref="System.IO.Stream" />.</returns>
    /// <exception cref="System.ArgumentNullException">If <paramref name="uri" /> is null.</exception>
    /// <exception cref="System.UnauthorizedAccessException">If access is denied.</exception>
    /// <exception cref="System.IO.IOException">For other I/O related errors.</exception>
    Task<Stream> OpenWriteAsync(
        StorageUri uri,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Checks whether a resource exists at the specified <see cref="StorageUri" />.
    /// </summary>
    /// <param name="uri">The storage location to check.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>True if the resource exists; otherwise false.</returns>
    Task<bool> ExistsAsync(
        StorageUri uri,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Deletes the resource at the specified <see cref="StorageUri" />.
    ///     If the resource does not exist, implementations may succeed silently or throw <see cref="FileNotFoundException" />;
    ///     check provider documentation for behavior.
    /// </summary>
    /// <param name="uri">The storage location to delete.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task representing the asynchronous delete operation.</returns>
    /// <exception cref="System.ArgumentNullException">If <paramref name="uri" /> is null.</exception>
    /// <exception cref="System.UnauthorizedAccessException">If access is denied.</exception>
    /// <exception cref="System.IO.IOException">For other I/O related errors.</exception>
    /// <exception cref="System.NotSupportedException">If the provider does not support delete operations.</exception>
    /// <remarks>
    ///     Default implementation throws <see cref="System.NotSupportedException" />.
    ///     Providers wishing to support delete must override this method.
    /// </remarks>
    Task DeleteAsync(
        StorageUri uri,
        CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException(
            $"Delete operation is not supported by {GetType().Name}. " +
            $"Check {nameof(IStorageProviderMetadataProvider)} or provider documentation.");
    }

    /// <summary>
    ///     Lists storage items at the specified prefix/directory.
    ///     Returns an async enumerable that yields <see cref="StorageItem" /> for each resource found.
    /// </summary>
    /// <param name="prefix">The URI prefix or directory to list.</param>
    /// <param name="recursive">If true, recursively lists all subdirectories/nested items; if false, lists only items in the specified directory. Default is false.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>
    ///     An async enumerable of <see cref="StorageItem" /> representing resources matching the prefix.
    ///     Returns an empty sequence if the prefix does not exist or contains no items.
    /// </returns>
    /// <exception cref="System.ArgumentNullException">If <paramref name="prefix" /> is null.</exception>
    /// <exception cref="System.UnauthorizedAccessException">If access is denied.</exception>
    /// <exception cref="System.IO.IOException">For other I/O related errors.</exception>
    /// <exception cref="System.NotSupportedException">If the provider does not support list operations.</exception>
    /// <remarks>
    ///     <para>
    ///         Default implementation throws <see cref="System.NotSupportedException" />.
    ///         Providers wishing to support listing must override this method and yield items asynchronously.
    ///     </para>
    ///     <para>
    ///         <strong>Cloud Provider Notes:</strong>
    ///         - <strong>S3/Azure:</strong> With <paramref name="recursive" /> = false, returns objects matching the prefix with "/" delimiter applied, similar to
    ///         filesystem directory listing.
    ///         - <strong>Database:</strong> Recursion semantics may differ; refer to provider documentation.
    ///         - <strong>Filesystem:</strong> recursive=false lists top-level items; recursive=true walks all subdirectories.
    ///     </para>
    ///     <para>
    ///         <strong>Permission and Resilience Behavior:</strong>
    ///         Providers should handle access restrictions gracefully when <paramref name="recursive" /> = true:
    ///         - If the root path is inaccessible, listing returns empty.
    ///         - If subdirectories become inaccessible during enumeration, they are skipped without aborting the listing.
    ///         - If items are deleted or modified during enumeration (concurrent mutations), they are skipped without aborting the listing.
    ///         This ensures robust listing behavior even in shared or changing environments. Providers unable to support partial results
    ///         (e.g., requiring all-or-nothing enumeration) should document this limitation.
    ///     </para>
    ///     <para>
    ///         Check <see cref="IStorageProviderMetadataProvider.GetMetadata" /> or provider documentation to determine if listing is supported
    ///         before calling this method.
    ///     </para>
    /// </remarks>
    IAsyncEnumerable<StorageItem> ListAsync(
        StorageUri prefix,
        bool recursive = false,
        CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException(
            $"List operation is not supported by {GetType().Name}. " +
            $"Check {nameof(IStorageProviderMetadataProvider)} or provider documentation.");
    }

    /// <summary>
    ///     Retrieves detailed metadata for the resource at the specified <see cref="StorageUri" />.
    ///     Returns null if the resource does not exist.
    /// </summary>
    /// <param name="uri">The storage location to query.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>
    ///     A task producing <see cref="StorageMetadata" /> if the resource exists; otherwise null.
    /// </returns>
    /// <exception cref="System.ArgumentNullException">If <paramref name="uri" /> is null.</exception>
    /// <exception cref="System.UnauthorizedAccessException">If access is denied.</exception>
    /// <exception cref="System.IO.IOException">For other I/O related errors.</exception>
    /// <remarks>
    ///     Default implementation returns null (metadata not available).
    ///     Providers supporting detailed metadata may override to return a populated <see cref="StorageMetadata" /> object.
    /// </remarks>
    Task<StorageMetadata?> GetMetadataAsync(
        StorageUri uri,
        CancellationToken cancellationToken = default)
    {
        return Task.FromResult<StorageMetadata?>(null);
    }
}

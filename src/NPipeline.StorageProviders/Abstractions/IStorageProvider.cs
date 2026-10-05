using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Abstractions;

/// <summary>
///     A storage backend (file system, S3, Azure Blob, ...) that connectors use to obtain streams without depending on
///     a specific implementation. Providers are routed to by URI scheme, and declare what they can do through
///     <see cref="Capabilities" />.
/// </summary>
/// <remarks>
///     <para>Derive from <see cref="StorageProvider" /> rather than implementing this interface directly: the base class validates arguments, checks capabilities and observes cancellation.</para>
///     <para>Every provider follows the same contract:</para>
///     <list type="bullet">
///         <item>A missing object: <see cref="FileNotFoundException" /> from read and move. <see cref="GetMetadataAsync" /> returns <see langword="null" /> and <see cref="ExistsAsync" /> returns <see langword="false" />.</item>
///         <item>Authentication or permission failure: <see cref="UnauthorizedAccessException" />.</item>
///         <item>An invalid bucket, container, key or path: <see cref="ArgumentException" />.</item>
///         <item>Any other service or I/O failure: <see cref="IOException" />, with the SDK exception as <see cref="Exception.InnerException" />.</item>
///         <item>A capability the provider does not declare: <see cref="Exceptions.UnsupportedStorageCapabilityException" />.</item>
///     </list>
/// </remarks>
public interface IStorageProvider
{
    /// <summary>A human-friendly name, for example "Amazon S3".</summary>
    string Name { get; }

    /// <summary>The URI schemes this provider serves. A resolver routes a URI to the provider that lists its scheme.</summary>
    IReadOnlyList<StorageScheme> Schemes { get; }

    /// <summary>The operations this provider supports.</summary>
    StorageCapabilities Capabilities { get; }

    /// <summary>Opens a readable stream. The caller disposes it.</summary>
    /// <param name="uri">The location to read.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <exception cref="FileNotFoundException">The object does not exist.</exception>
    Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken cancellationToken = default);

    /// <summary>Opens a writable stream, creating any missing parent directories. The caller disposes it.</summary>
    /// <param name="uri">The location to write.</param>
    /// <param name="options">Optional content type, metadata and write conditions.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    Task<StorageWriteStream> OpenWriteAsync(StorageUri uri, StorageWriteOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>Returns the object's metadata, or <see langword="null" /> when it does not exist.</summary>
    /// <param name="uri">The location to inspect.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    Task<StorageMetadata?> GetMetadataAsync(StorageUri uri, CancellationToken cancellationToken = default);

    /// <summary>Returns whether an object exists. A directory exists on providers that declare <see cref="StorageCapabilities.Hierarchy" />; a bare prefix on an object store does not.</summary>
    /// <param name="uri">The location to check.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    Task<bool> ExistsAsync(StorageUri uri, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Lists the objects under a directory. <paramref name="directory" /> is treated as a directory, so <c>logs</c> never matches <c>logs-archive/</c>.
    ///     With <paramref name="recursive" /> <see langword="false" /> it yields direct children, including directory entries; with <see langword="true" /> it yields every file below and no directory entries.
    ///     Each yielded URI keeps the caller's host, port, user and parameters. The order is unspecified, and a missing directory yields nothing.
    /// </summary>
    /// <param name="directory">The directory or prefix to list.</param>
    /// <param name="recursive">Whether to list nested items.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    IAsyncEnumerable<StorageItem> ListAsync(StorageUri directory, bool recursive = false, CancellationToken cancellationToken = default);

    /// <summary>Deletes an object. Deleting a missing object succeeds.</summary>
    /// <param name="uri">The location to delete.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    Task DeleteAsync(StorageUri uri, CancellationToken cancellationToken = default);

    /// <summary>Moves an object, overwriting the destination.</summary>
    /// <param name="source">The object to move.</param>
    /// <param name="destination">The new location.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <exception cref="FileNotFoundException">The source does not exist.</exception>
    Task MoveAsync(StorageUri source, StorageUri destination, CancellationToken cancellationToken = default);
}

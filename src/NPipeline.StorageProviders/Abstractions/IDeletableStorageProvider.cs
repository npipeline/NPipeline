using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Abstractions;

/// <summary>
///     Interface for storage providers that support delete operations.
/// </summary>
public interface IDeletableStorageProvider
{
    /// <summary>
    ///     Deletes a file at the specified URI.
    /// </summary>
    /// <param name="uri">The URI of the file to delete.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task DeleteAsync(StorageUri uri, CancellationToken cancellationToken = default);
}

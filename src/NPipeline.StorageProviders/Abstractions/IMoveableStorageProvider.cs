using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Abstractions;

/// <summary>
///     Interface for storage providers that support move/rename operations.
/// </summary>
public interface IMoveableStorageProvider
{
    /// <summary>
    ///     Moves a file from one location to another.
    /// </summary>
    /// <param name="sourceUri">The source URI.</param>
    /// <param name="destinationUri">The destination URI.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task MoveAsync(StorageUri sourceUri, StorageUri destinationUri, CancellationToken cancellationToken = default);
}

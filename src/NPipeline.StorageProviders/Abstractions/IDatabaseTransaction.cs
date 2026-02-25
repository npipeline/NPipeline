namespace NPipeline.StorageProviders.Abstractions;

/// <summary>
///     Database transaction abstraction for database-agnostic operations.
/// </summary>
public interface IDatabaseTransaction : IAsyncDisposable
{
    /// <summary>
    ///     Commits the transaction asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task CommitAsync(CancellationToken cancellationToken = default);

    /// <summary>
    ///     Rolls back the transaction asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task RollbackAsync(CancellationToken cancellationToken = default);
}

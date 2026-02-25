namespace NPipeline.StorageProviders.Abstractions;

/// <summary>
///     Database connection abstraction for database-agnostic operations.
/// </summary>
public interface IDatabaseConnection : IAsyncDisposable
{
    /// <summary>
    ///     Gets a value indicating whether the connection is currently open.
    /// </summary>
    bool IsOpen { get; }

    /// <summary>
    ///     Gets the current transaction, if one is active.
    /// </summary>
    IDatabaseTransaction? CurrentTransaction { get; }

    /// <summary>
    ///     Opens the database connection asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task OpenAsync(CancellationToken cancellationToken = default);

    /// <summary>
    ///     Closes the database connection asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task CloseAsync(CancellationToken cancellationToken = default);

    /// <summary>
    ///     Begins a new database transaction asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation that returns the transaction.</returns>
    Task<IDatabaseTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default);

    /// <summary>
    ///     Creates a database command for this connection.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task<IDatabaseCommand> CreateCommandAsync(CancellationToken cancellationToken = default);
}

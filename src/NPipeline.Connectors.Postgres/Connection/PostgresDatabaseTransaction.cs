using Npgsql;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Postgres.Connection;

/// <summary>
///     PostgreSQL implementation of IDatabaseTransaction.
///     Wraps NpgsqlTransaction for database-agnostic operations.
/// </summary>
internal sealed class PostgresDatabaseTransaction : IDatabaseTransaction
{
    private readonly PostgresDatabaseConnection _connection;
    private bool _disposed;

    /// <summary>
    ///     Initializes a new instance of <see cref="PostgresDatabaseTransaction" /> class.
    /// </summary>
    /// <param name="transaction">The Npgsql transaction.</param>
    /// <param name="connection">The connection that owns this transaction.</param>
    public PostgresDatabaseTransaction(NpgsqlTransaction transaction, PostgresDatabaseConnection connection)
    {
        UnderlyingTransaction = transaction ?? throw new ArgumentNullException(nameof(transaction));
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    }

    /// <summary>
    ///     Gets the underlying NpgsqlTransaction for PostgreSQL-specific operations.
    /// </summary>
    internal NpgsqlTransaction UnderlyingTransaction { get; }

    /// <summary>
    ///     Commits the transaction asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await UnderlyingTransaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        _connection.ClearTransaction();
    }

    /// <summary>
    ///     Rolls back the transaction asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await UnderlyingTransaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
        _connection.ClearTransaction();
    }

    /// <summary>
    ///     Disposes the transaction asynchronously.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        try
        {
            // DisposeAsync will roll back the transaction if not already committed
            await UnderlyingTransaction.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _connection.ClearTransaction();
            _disposed = true;
        }
    }
}

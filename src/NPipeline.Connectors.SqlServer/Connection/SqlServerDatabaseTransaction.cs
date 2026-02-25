using Microsoft.Data.SqlClient;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.SqlServer.Connection;

/// <summary>
///     SQL Server implementation of IDatabaseTransaction.
///     Wraps SqlTransaction for database-agnostic operations.
/// </summary>
internal sealed class SqlServerDatabaseTransaction : IDatabaseTransaction
{
    private readonly SqlServerDatabaseConnection _connection;
    private bool _disposed;

    /// <summary>
    ///     Initializes a new instance of <see cref="SqlServerDatabaseTransaction" /> class.
    /// </summary>
    /// <param name="transaction">The SQL Server transaction.</param>
    /// <param name="connection">The connection that owns this transaction.</param>
    public SqlServerDatabaseTransaction(SqlTransaction transaction, SqlServerDatabaseConnection connection)
    {
        UnderlyingTransaction = transaction ?? throw new ArgumentNullException(nameof(transaction));
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    }

    /// <summary>
    ///     Gets the underlying SqlTransaction for SQL Server-specific operations.
    /// </summary>
    internal SqlTransaction UnderlyingTransaction { get; }

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

using MySqlConnector;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.MySql.Connection;

/// <summary>
///     MySQL implementation of <see cref="IDatabaseTransaction" />.
///     Wraps <see cref="MySqlTransaction" /> for database-agnostic operations.
/// </summary>
internal sealed class MySqlDatabaseTransaction : IDatabaseTransaction
{
    private readonly MySqlDatabaseConnection _connection;
    private bool _disposed;

    /// <summary>
    ///     Initialises a new <see cref="MySqlDatabaseTransaction" />.
    /// </summary>
    /// <param name="transaction">The underlying MySQL transaction.</param>
    /// <param name="connection">The connection that owns this transaction.</param>
    public MySqlDatabaseTransaction(MySqlTransaction transaction,
        MySqlDatabaseConnection connection)
    {
        UnderlyingTransaction = transaction ?? throw new ArgumentNullException(nameof(transaction));
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    }

    /// <summary>
    ///     Gets the underlying <see cref="MySqlTransaction" /> for MySQL-specific operations.
    /// </summary>
    internal MySqlTransaction UnderlyingTransaction { get; }

    /// <inheritdoc />
    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await UnderlyingTransaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        _connection.ClearTransaction();
    }

    /// <inheritdoc />
    public async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await UnderlyingTransaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
        _connection.ClearTransaction();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        try
        {
            await UnderlyingTransaction.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _connection.ClearTransaction();
            _disposed = true;
        }
    }
}

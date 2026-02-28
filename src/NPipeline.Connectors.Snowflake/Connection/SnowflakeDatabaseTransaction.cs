using System.Data.Common;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Snowflake.Connection;

/// <summary>
///     Snowflake implementation of IDatabaseTransaction.
///     Wraps DbTransaction for database-agnostic operations.
/// </summary>
internal sealed class SnowflakeDatabaseTransaction : IDatabaseTransaction
{
    private readonly SnowflakeDatabaseConnection _connection;
    private bool _disposed;

    /// <summary>
    ///     Initializes a new instance of the <see cref="SnowflakeDatabaseTransaction" /> class.
    /// </summary>
    public SnowflakeDatabaseTransaction(DbTransaction transaction, SnowflakeDatabaseConnection connection)
    {
        UnderlyingTransaction = transaction ?? throw new ArgumentNullException(nameof(transaction));
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    }

    /// <summary>
    ///     Gets the underlying DbTransaction for Snowflake-specific operations.
    /// </summary>
    internal DbTransaction UnderlyingTransaction { get; }

    /// <summary>
    ///     Commits the transaction asynchronously.
    /// </summary>
    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await UnderlyingTransaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        _connection.ClearTransaction();
    }

    /// <summary>
    ///     Rolls back the transaction asynchronously.
    /// </summary>
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
            await UnderlyingTransaction.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _connection.ClearTransaction();
            _disposed = true;
        }
    }
}

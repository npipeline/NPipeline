using System.Data;
using System.Data.Common;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Snowflake.Connection;

/// <summary>
///     Snowflake implementation of IDatabaseConnection.
///     Wraps DbConnection for database-agnostic operations.
/// </summary>
internal sealed class SnowflakeDatabaseConnection(DbConnection connection) : IDatabaseConnection
{
    private SnowflakeDatabaseTransaction? _transactionWrapper;

    /// <summary>
    ///     Gets the underlying DbConnection for Snowflake-specific operations like PUT/COPY.
    /// </summary>
    internal DbConnection UnderlyingConnection { get; } = connection ?? throw new ArgumentNullException(nameof(connection));

    /// <summary>
    ///     Gets the underlying DbTransaction for Snowflake-specific operations.
    /// </summary>
    internal DbTransaction? UnderlyingTransaction { get; private set; }

    /// <summary>
    ///     Gets a value indicating whether the connection is currently open.
    /// </summary>
    public bool IsOpen => UnderlyingConnection.State == ConnectionState.Open;

    /// <summary>
    ///     Gets the current transaction, if one is active.
    /// </summary>
    public IDatabaseTransaction? CurrentTransaction => _transactionWrapper;

    /// <summary>
    ///     Opens the database connection asynchronously.
    /// </summary>
    public async Task OpenAsync(CancellationToken cancellationToken = default)
    {
        if (UnderlyingConnection.State == ConnectionState.Closed)
            await UnderlyingConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Closes the database connection asynchronously.
    /// </summary>
    public Task CloseAsync(CancellationToken cancellationToken = default)
    {
        UnderlyingConnection.Close();
        return Task.CompletedTask;
    }

    /// <summary>
    ///     Begins a new database transaction asynchronously.
    /// </summary>
    public async Task<IDatabaseTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        if (UnderlyingTransaction != null)
            throw new InvalidOperationException("A transaction is already in progress. Commit or rollback the current transaction before starting a new one.");

        UnderlyingTransaction = await UnderlyingConnection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        _transactionWrapper = new SnowflakeDatabaseTransaction(UnderlyingTransaction, this);
        return _transactionWrapper;
    }

    /// <summary>
    ///     Creates a database command for this connection.
    /// </summary>
    public Task<IDatabaseCommand> CreateCommandAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var command = UnderlyingConnection.CreateCommand();

        if (UnderlyingTransaction != null)
            command.Transaction = UnderlyingTransaction;

        return Task.FromResult<IDatabaseCommand>(new SnowflakeDatabaseCommand(command));
    }

    /// <summary>
    ///     Disposes the connection asynchronously.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (UnderlyingTransaction != null)
        {
            await UnderlyingTransaction.DisposeAsync().ConfigureAwait(false);
            UnderlyingTransaction = null;
            _transactionWrapper = null;
        }

        await UnderlyingConnection.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    ///     Clears the current transaction reference.
    /// </summary>
    internal void ClearTransaction()
    {
        UnderlyingTransaction = null;
        _transactionWrapper = null;
    }
}

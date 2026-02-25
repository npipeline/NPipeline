using System.Data;
using Microsoft.Data.SqlClient;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.SqlServer.Connection;

/// <summary>
///     SQL Server implementation of IDatabaseConnection.
///     Wraps SqlConnection for database-agnostic operations.
/// </summary>
internal sealed class SqlServerDatabaseConnection(SqlConnection connection) : IDatabaseConnection
{
    private SqlServerDatabaseTransaction? _transactionWrapper;

    /// <summary>
    ///     Gets the underlying SqlConnection for SQL Server-specific operations like SqlBulkCopy.
    /// </summary>
    internal SqlConnection UnderlyingConnection { get; } = connection ?? throw new ArgumentNullException(nameof(connection));

    /// <summary>
    ///     Gets the underlying SqlTransaction for SQL Server-specific operations.
    /// </summary>
    internal SqlTransaction? UnderlyingTransaction { get; private set; }

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
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task OpenAsync(CancellationToken cancellationToken = default)
    {
        if (UnderlyingConnection.State == ConnectionState.Closed)
            await UnderlyingConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Closes the database connection asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public Task CloseAsync(CancellationToken cancellationToken = default)
    {
        UnderlyingConnection.Close();
        return Task.CompletedTask;
    }

    /// <summary>
    ///     Begins a new database transaction asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation that returns the transaction.</returns>
    public async Task<IDatabaseTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        if (UnderlyingTransaction != null)
            throw new InvalidOperationException("A transaction is already in progress. Commit or rollback the current transaction before starting a new one.");

        UnderlyingTransaction = (SqlTransaction)await UnderlyingConnection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        _transactionWrapper = new SqlServerDatabaseTransaction(UnderlyingTransaction, this);
        return _transactionWrapper;
    }

    /// <summary>
    ///     Creates a database command for this connection.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public Task<IDatabaseCommand> CreateCommandAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var command = UnderlyingConnection.CreateCommand();

        // If there's an active transaction, enlist the command in it
        if (UnderlyingTransaction != null)
            command.Transaction = UnderlyingTransaction;

        return Task.FromResult<IDatabaseCommand>(new SqlServerDatabaseCommand(command));
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
    ///     Clears the current transaction reference (called by SqlServerDatabaseTransaction on commit/rollback).
    /// </summary>
    internal void ClearTransaction()
    {
        UnderlyingTransaction = null;
        _transactionWrapper = null;
    }
}

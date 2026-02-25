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
    private readonly SqlConnection _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    private SqlTransaction? _currentTransaction;
    private SqlServerDatabaseTransaction? _transactionWrapper;

    /// <summary>
    ///     Gets the underlying SqlConnection for SQL Server-specific operations like SqlBulkCopy.
    /// </summary>
    internal SqlConnection UnderlyingConnection => _connection;

    /// <summary>
    ///     Gets the underlying SqlTransaction for SQL Server-specific operations.
    /// </summary>
    internal SqlTransaction? UnderlyingTransaction => _currentTransaction;

    /// <summary>
    ///     Gets a value indicating whether the connection is currently open.
    /// </summary>
    public bool IsOpen => _connection.State == ConnectionState.Open;

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
        if (_connection.State == ConnectionState.Closed)
        {
            await _connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    ///     Closes the database connection asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public Task CloseAsync(CancellationToken cancellationToken = default)
    {
        _connection.Close();
        return Task.CompletedTask;
    }

    /// <summary>
    ///     Begins a new database transaction asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation that returns the transaction.</returns>
    public async Task<IDatabaseTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        if (_currentTransaction != null)
        {
            throw new InvalidOperationException("A transaction is already in progress. Commit or rollback the current transaction before starting a new one.");
        }

        _currentTransaction = (SqlTransaction)await _connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        _transactionWrapper = new SqlServerDatabaseTransaction(_currentTransaction, this);
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
        var command = _connection.CreateCommand();

        // If there's an active transaction, enlist the command in it
        if (_currentTransaction != null)
        {
            command.Transaction = _currentTransaction;
        }

        return Task.FromResult<IDatabaseCommand>(new SqlServerDatabaseCommand(command));
    }

    /// <summary>
    ///     Clears the current transaction reference (called by SqlServerDatabaseTransaction on commit/rollback).
    /// </summary>
    internal void ClearTransaction()
    {
        _currentTransaction = null;
        _transactionWrapper = null;
    }

    /// <summary>
    ///     Disposes the connection asynchronously.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_currentTransaction != null)
        {
            await _currentTransaction.DisposeAsync().ConfigureAwait(false);
            _currentTransaction = null;
            _transactionWrapper = null;
        }

        await _connection.DisposeAsync().ConfigureAwait(false);
    }
}

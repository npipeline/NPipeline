using System.Data;
using Npgsql;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.PostgreSQL.Connection;

/// <summary>
///     PostgreSQL implementation of IDatabaseConnection.
///     Wraps NpgsqlConnection for database-agnostic operations.
/// </summary>
internal sealed class PostgresDatabaseConnection(NpgsqlConnection connection) : IDatabaseConnection
{
    private PostgresDatabaseTransaction? _transactionWrapper;

    /// <summary>
    ///     Gets the underlying NpgsqlConnection for PostgreSQL-specific operations like COPY.
    /// </summary>
    internal NpgsqlConnection UnderlyingConnection { get; } = connection ?? throw new ArgumentNullException(nameof(connection));

    /// <summary>
    ///     Gets the underlying NpgsqlTransaction for PostgreSQL-specific operations.
    /// </summary>
    internal NpgsqlTransaction? UnderlyingTransaction { get; private set; }

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

        UnderlyingTransaction = await UnderlyingConnection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        _transactionWrapper = new PostgresDatabaseTransaction(UnderlyingTransaction, this);
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

        return Task.FromResult<IDatabaseCommand>(new PostgresDatabaseCommand(command));
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
    ///     Clears the current transaction reference (called by PostgresDatabaseTransaction on commit/rollback).
    /// </summary>
    internal void ClearTransaction()
    {
        UnderlyingTransaction = null;
        _transactionWrapper = null;
    }
}

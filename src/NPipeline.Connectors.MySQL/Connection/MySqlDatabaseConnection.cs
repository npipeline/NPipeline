using System.Data;
using MySqlConnector;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.MySql.Connection;

/// <summary>
///     MySQL implementation of <see cref="IDatabaseConnection" />.
///     Wraps <see cref="MySqlConnection" /> for database-agnostic operations.
/// </summary>
internal sealed class MySqlDatabaseConnection(MySqlConnection connection) : IDatabaseConnection
{
    private MySqlDatabaseTransaction? _transactionWrapper;

    /// <summary>
    ///     Gets the underlying <see cref="MySqlConnection" /> for MySQL-specific operations such as
    ///     <see cref="MySqlBulkLoader" />.
    /// </summary>
    internal MySqlConnection UnderlyingConnection { get; } =
        connection ?? throw new ArgumentNullException(nameof(connection));

    /// <summary>
    ///     Gets the underlying <see cref="MySqlTransaction" /> if one is active.
    /// </summary>
    internal MySqlTransaction? UnderlyingTransaction { get; private set; }

    /// <inheritdoc />
    public bool IsOpen => UnderlyingConnection.State == ConnectionState.Open;

    /// <inheritdoc />
    public IDatabaseTransaction? CurrentTransaction => _transactionWrapper;

    /// <inheritdoc />
    public async Task OpenAsync(CancellationToken cancellationToken = default)
    {
        if (UnderlyingConnection.State == ConnectionState.Closed)
            await UnderlyingConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task CloseAsync(CancellationToken cancellationToken = default)
    {
        UnderlyingConnection.Close();
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task<IDatabaseTransaction> BeginTransactionAsync(
        CancellationToken cancellationToken = default)
    {
        if (UnderlyingTransaction is not null)
            throw new InvalidOperationException(
                "A transaction is already in progress. Commit or rollback the current transaction before starting a new one.");

        UnderlyingTransaction = (MySqlTransaction)await UnderlyingConnection
            .BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        _transactionWrapper = new MySqlDatabaseTransaction(UnderlyingTransaction, this);
        return _transactionWrapper;
    }

    /// <inheritdoc />
    public Task<IDatabaseCommand> CreateCommandAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var command = UnderlyingConnection.CreateCommand();

        if (UnderlyingTransaction is not null)
            command.Transaction = UnderlyingTransaction;

        return Task.FromResult<IDatabaseCommand>(new MySqlDatabaseCommand(command));
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (UnderlyingTransaction is not null)
        {
            await UnderlyingTransaction.DisposeAsync().ConfigureAwait(false);
            UnderlyingTransaction = null;
            _transactionWrapper = null;
        }

        await UnderlyingConnection.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    ///     Clears the active transaction reference.
    ///     Called by <see cref="MySqlDatabaseTransaction" /> on commit or rollback.
    /// </summary>
    internal void ClearTransaction()
    {
        UnderlyingTransaction = null;
        _transactionWrapper = null;
    }
}

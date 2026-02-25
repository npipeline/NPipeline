using Microsoft.Azure.Cosmos;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Azure.CosmosDb.Connection;

/// <summary>
///     Cosmos DB implementation of IDatabaseConnection.
///     Wraps Container for database-agnostic operations.
/// </summary>
internal sealed class CosmosDatabaseConnection : IDatabaseConnection
{
    private readonly int _defaultFetchSize;
    private readonly int _defaultMaxConcurrency;
    private bool _disposed;

    /// <summary>
    ///     Initializes a new instance of the <see cref="CosmosDatabaseConnection" /> class.
    /// </summary>
    /// <param name="database">The Cosmos DB database.</param>
    /// <param name="container">The Cosmos DB container.</param>
    /// <param name="defaultFetchSize">Default fetch size for queries.</param>
    /// <param name="defaultMaxConcurrency">Default max concurrency for parallel queries.</param>
    public CosmosDatabaseConnection(
        Database database,
        Container container,
        int defaultFetchSize = 100,
        int defaultMaxConcurrency = 1)
    {
        Database = database;
        Container = container;
        _defaultFetchSize = defaultFetchSize;
        _defaultMaxConcurrency = defaultMaxConcurrency;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="CosmosDatabaseConnection" /> class with configuration.
    /// </summary>
    /// <param name="database">The Cosmos DB database.</param>
    /// <param name="container">The Cosmos DB container.</param>
    /// <param name="configuration">The Cosmos configuration.</param>
    public CosmosDatabaseConnection(
        Database database,
        Container container,
        CosmosConfiguration configuration)
    {
        Database = database;
        Container = container;
        _defaultFetchSize = configuration?.MaxItemCount ?? 100;
        _defaultMaxConcurrency = configuration?.MaxConcurrentOperations ?? 1;
    }

    /// <summary>
    ///     Gets the underlying Cosmos DB container.
    /// </summary>
    public Container Container { get; }

    /// <summary>
    ///     Gets the underlying Cosmos DB database.
    /// </summary>
    public Database Database { get; }

    /// <summary>
    ///     Gets a value indicating whether the connection is currently open.
    ///     Cosmos DB connections are stateless, so this always returns true.
    /// </summary>
    public bool IsOpen => !_disposed;

    /// <summary>
    ///     Gets the current transaction. Cosmos DB does not support connection-level transactions,
    ///     so this always returns null.
    /// </summary>
    public IDatabaseTransaction? CurrentTransaction => null;

    /// <summary>
    ///     Opens the database connection asynchronously.
    ///     Cosmos DB connections are stateless, so this is a no-op.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A completed task.</returns>
    public Task OpenAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return Task.CompletedTask;
    }

    /// <summary>
    ///     Begins a database transaction. Cosmos DB does not support connection-level transactions.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <exception cref="NotSupportedException">Always thrown. Use Cosmos DB transactional batch operations instead.</exception>
    public Task<IDatabaseTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException(
            "Cosmos DB does not support traditional database transactions. Use Cosmos DB transactional batch operations instead.");
    }

    /// <summary>
    ///     Closes the database connection asynchronously.
    ///     Cosmos DB connections are stateless, so this is a no-op.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A completed task.</returns>
    public Task CloseAsync(CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }

    /// <summary>
    ///     Creates a database command for this connection.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A Cosmos DB database command.</returns>
    public Task<IDatabaseCommand> CreateCommandAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        return Task.FromResult<IDatabaseCommand>(
            new CosmosDatabaseCommand(Container, _defaultFetchSize, _defaultMaxConcurrency));
    }

    /// <summary>
    ///     Disposes the connection asynchronously.
    /// </summary>
    public ValueTask DisposeAsync()
    {
        _disposed = true;
        return ValueTask.CompletedTask;
    }
}

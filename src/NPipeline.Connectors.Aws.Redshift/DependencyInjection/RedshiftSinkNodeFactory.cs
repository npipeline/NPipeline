using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Connection;
using NPipeline.Connectors.Aws.Redshift.Nodes;

namespace NPipeline.Connectors.Aws.Redshift.DependencyInjection;

/// <summary>
///     Factory for creating Redshift sink nodes with dependency injection support.
/// </summary>
public class RedshiftSinkNodeFactory : IRedshiftSinkNodeFactory
{
    private readonly IRedshiftConnectionPool _connectionPool;

    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftSinkNodeFactory" /> class.
    /// </summary>
    /// <param name="connectionPool">The connection pool.</param>
    public RedshiftSinkNodeFactory(IRedshiftConnectionPool connectionPool)
    {
        _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));
    }

    /// <summary>
    ///     Creates a Redshift sink node using a connection from the pool.
    /// </summary>
    /// <typeparam name="T">The type of objects to write.</typeparam>
    /// <param name="connectionName">The name of the connection to use.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="writeStrategy">The write strategy to use.</param>
    /// <param name="configuration">Optional configuration. If null, defaults are used.</param>
    /// <param name="schema">Optional schema name (default: from configuration or "public").</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A configured Redshift sink node.</returns>
    public Task<RedshiftSinkNode<T>> CreateSinkAsync<T>(
        string connectionName,
        string tableName,
        RedshiftWriteStrategy writeStrategy = RedshiftWriteStrategy.Batch,
        RedshiftConfiguration? configuration = null,
        string? schema = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(connectionName);
        ArgumentNullException.ThrowIfNull(tableName);
        cancellationToken.ThrowIfCancellationRequested();

        if (!_connectionPool.HasNamedConnection(connectionName))
            throw new InvalidOperationException($"Named connection '{connectionName}' not found.");

        var config = configuration ?? new RedshiftConfiguration();

        return Task.FromResult(new RedshiftSinkNode<T>(
            _connectionPool,
            tableName,
            writeStrategy,
            config,
            schema,
            connectionName));
    }

    /// <summary>
    ///     Creates a Redshift sink node using the default connection.
    /// </summary>
    /// <typeparam name="T">The type of objects to write.</typeparam>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="writeStrategy">The write strategy to use.</param>
    /// <param name="configuration">Optional configuration. If null, defaults are used.</param>
    /// <param name="schema">Optional schema name (default: from configuration or "public").</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A configured Redshift sink node.</returns>
    public Task<RedshiftSinkNode<T>> CreateSinkAsync<T>(
        string tableName,
        RedshiftWriteStrategy writeStrategy = RedshiftWriteStrategy.Batch,
        RedshiftConfiguration? configuration = null,
        string? schema = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(tableName);
        cancellationToken.ThrowIfCancellationRequested();

        var config = configuration ?? new RedshiftConfiguration();

        return Task.FromResult(new RedshiftSinkNode<T>(
            _connectionPool,
            tableName,
            writeStrategy,
            config,
            schema));
    }

    /// <summary>
    ///     Creates a Redshift sink node with explicit schema using a named connection.
    /// </summary>
    /// <typeparam name="T">The type of objects to write.</typeparam>
    /// <param name="connectionName">The name of the connection to use.</param>
    /// <param name="schema">The schema name.</param>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="writeStrategy">The write strategy to use.</param>
    /// <param name="configuration">Optional configuration. If null, defaults are used.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A configured Redshift sink node.</returns>
    public Task<RedshiftSinkNode<T>> CreateSinkAsync<T>(
        string connectionName,
        string schema,
        string tableName,
        RedshiftWriteStrategy writeStrategy = RedshiftWriteStrategy.Batch,
        RedshiftConfiguration? configuration = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        return CreateSinkAsync<T>(connectionName, tableName, writeStrategy, configuration, schema, cancellationToken);
    }
}

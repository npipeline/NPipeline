using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Connection;
using NPipeline.Connectors.Aws.Redshift.Mapping;
using NPipeline.Connectors.Aws.Redshift.Nodes;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Aws.Redshift.DependencyInjection;

/// <summary>
///     Factory for creating Redshift source nodes with dependency injection support.
/// </summary>
public class RedshiftSourceNodeFactory : IRedshiftSourceNodeFactory
{
    private readonly IRedshiftConnectionPool _connectionPool;

    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftSourceNodeFactory" /> class.
    /// </summary>
    /// <param name="connectionPool">The connection pool.</param>
    public RedshiftSourceNodeFactory(IRedshiftConnectionPool connectionPool)
    {
        _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));
    }

    /// <summary>
    ///     Creates a Redshift source node using a connection from the pool.
    /// </summary>
    /// <typeparam name="T">The type of objects to read.</typeparam>
    /// <param name="connectionName">The name of the connection to use.</param>
    /// <param name="query">The SQL query to execute.</param>
    /// <param name="rowMapper">Optional custom row mapper function.</param>
    /// <param name="configuration">Optional configuration. If null, defaults are used.</param>
    /// <param name="parameters">Optional query parameters.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A configured Redshift source node.</returns>
    public Task<RedshiftSourceNode<T>> CreateSourceAsync<T>(
        string connectionName,
        string query,
        Func<RedshiftRow, T>? rowMapper = null,
        RedshiftConfiguration? configuration = null,
        DatabaseParameter[]? parameters = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(connectionName);
        ArgumentNullException.ThrowIfNull(query);

        cancellationToken.ThrowIfCancellationRequested();

        if (!_connectionPool.HasNamedConnection(connectionName))
            throw new InvalidOperationException($"Named connection '{connectionName}' not found.");

        var config = configuration ?? new RedshiftConfiguration();

        return Task.FromResult(new RedshiftSourceNode<T>(
            _connectionPool,
            query,
            rowMapper,
            config,
            parameters,
            config.ContinueOnError,
            connectionName));
    }

    /// <summary>
    ///     Creates a Redshift source node using the default connection.
    /// </summary>
    /// <typeparam name="T">The type of objects to read.</typeparam>
    /// <param name="query">The SQL query to execute.</param>
    /// <param name="rowMapper">Optional custom row mapper function.</param>
    /// <param name="configuration">Optional configuration. If null, defaults are used.</param>
    /// <param name="parameters">Optional query parameters.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A configured Redshift source node.</returns>
    public Task<RedshiftSourceNode<T>> CreateSourceAsync<T>(
        string query,
        Func<RedshiftRow, T>? rowMapper = null,
        RedshiftConfiguration? configuration = null,
        DatabaseParameter[]? parameters = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(query);

        cancellationToken.ThrowIfCancellationRequested();

        var config = configuration ?? new RedshiftConfiguration();

        return Task.FromResult(new RedshiftSourceNode<T>(
            _connectionPool,
            query,
            rowMapper,
            config,
            parameters,
            config.ContinueOnError));
    }

    /// <summary>
    ///     Creates a Redshift source node using attribute-based mapping.
    /// </summary>
    /// <typeparam name="T">The type of objects to read (must have mapping attributes).</typeparam>
    /// <param name="connectionName">The name of the connection to use.</param>
    /// <param name="query">The SQL query to execute.</param>
    /// <param name="configuration">Optional configuration. If null, defaults are used.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A configured Redshift source node.</returns>
    public Task<RedshiftSourceNode<T>> CreateSourceWithAttributesAsync<T>(
        string connectionName,
        string query,
        RedshiftConfiguration? configuration = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        return CreateSourceAsync<T>(connectionName, query, null, configuration, null, cancellationToken);
    }
}

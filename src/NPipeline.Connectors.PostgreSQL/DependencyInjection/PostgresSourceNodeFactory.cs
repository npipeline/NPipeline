using NPipeline.Connectors.PostgreSQL.Configuration;
using NPipeline.Connectors.PostgreSQL.Connection;
using NPipeline.Connectors.PostgreSQL.Mapping;
using NPipeline.Connectors.PostgreSQL.Nodes;

namespace NPipeline.Connectors.PostgreSQL.DependencyInjection;

/// <summary>
///     Factory for creating PostgreSQL source nodes with dependency injection support.
/// </summary>
public class PostgresSourceNodeFactory
{
    private readonly IPostgresConnectionPool _connectionPool;

    /// <summary>
    ///     Initializes a new instance of the <see cref="PostgresSourceNodeFactory" /> class.
    /// </summary>
    /// <param name="connectionPool">The connection pool.</param>
    public PostgresSourceNodeFactory(IPostgresConnectionPool connectionPool)
    {
        _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));
    }

    /// <summary>
    ///     Creates a PostgreSQL source node using a connection from the pool.
    /// </summary>
    /// <typeparam name="T">The type of objects to read.</typeparam>
    /// <param name="connectionName">The name of the connection to use.</param>
    /// <param name="query">The SQL query to execute.</param>
    /// <param name="rowMapper">Optional custom row mapper function.</param>
    /// <param name="configuration">Optional configuration. If null, defaults are used.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A configured PostgreSQL source node.</returns>
    public Task<PostgresSourceNode<T>> CreateSourceAsync<T>(
        string connectionName,
        string query,
        Func<PostgresRow, T>? rowMapper = null,
        PostgresConfiguration? configuration = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(connectionName);
        ArgumentNullException.ThrowIfNull(query);

        cancellationToken.ThrowIfCancellationRequested();

        if (!_connectionPool.HasNamedConnection(connectionName))
            throw new InvalidOperationException($"Named connection '{connectionName}' not found.");

        var mapper = rowMapper ?? PostgresMapperBuilder.Build<T>();
        var config = configuration ?? new PostgresConfiguration();

        return Task.FromResult(new PostgresSourceNode<T>(_connectionPool, query, mapper, config, null, config.ContinueOnError, connectionName));
    }

    /// <summary>
    ///     Creates a PostgreSQL source node using attribute-based mapping.
    /// </summary>
    /// <typeparam name="T">The type of objects to read (must have mapping attributes).</typeparam>
    /// <param name="connectionName">The name of the connection to use.</param>
    /// <param name="query">The SQL query to execute.</param>
    /// <param name="configuration">Optional configuration. If null, defaults are used.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A configured PostgreSQL source node.</returns>
    public Task<PostgresSourceNode<T>> CreateSourceWithAttributesAsync<T>(
        string connectionName,
        string query,
        PostgresConfiguration? configuration = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        return CreateSourceAsync<T>(connectionName, query, null, configuration, cancellationToken);
    }
}

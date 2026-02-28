using NPipeline.Connectors.Snowflake.Configuration;
using NPipeline.Connectors.Snowflake.Connection;
using NPipeline.Connectors.Snowflake.Mapping;
using NPipeline.Connectors.Snowflake.Nodes;

namespace NPipeline.Connectors.Snowflake.DependencyInjection;

/// <summary>
///     Factory interface for creating Snowflake source nodes with dependency injection support.
/// </summary>
public interface ISnowflakeSourceNodeFactory
{
    /// <summary>
    ///     Creates a Snowflake source node using the default connection.
    /// </summary>
    /// <typeparam name="T">The type of objects to read.</typeparam>
    /// <param name="query">The SQL query to execute.</param>
    /// <param name="configuration">Optional configuration. If null, defaults are used.</param>
    /// <returns>A configured Snowflake source node.</returns>
    SnowflakeSourceNode<T> CreateSourceNode<T>(
        string query,
        SnowflakeConfiguration? configuration = null)
        where T : class;

    /// <summary>
    ///     Creates a Snowflake source node with a custom mapper using the default connection.
    /// </summary>
    /// <typeparam name="T">The type of objects to read.</typeparam>
    /// <param name="query">The SQL query to execute.</param>
    /// <param name="customMapper">Optional custom row mapper function.</param>
    /// <param name="configuration">Optional configuration. If null, defaults are used.</param>
    /// <returns>A configured Snowflake source node.</returns>
    SnowflakeSourceNode<T> CreateSourceNode<T>(
        string query,
        Func<SnowflakeRow, T>? customMapper,
        SnowflakeConfiguration? configuration = null)
        where T : class;

    /// <summary>
    ///     Creates a Snowflake source node using a named connection.
    /// </summary>
    /// <typeparam name="T">The type of objects to read.</typeparam>
    /// <param name="query">The SQL query to execute.</param>
    /// <param name="connectionName">The name of the connection to use.</param>
    /// <param name="configuration">Optional configuration. If null, defaults are used.</param>
    /// <returns>A configured Snowflake source node.</returns>
    SnowflakeSourceNode<T> CreateSourceNode<T>(
        string query,
        string? connectionName,
        SnowflakeConfiguration? configuration = null)
        where T : class;

    /// <summary>
    ///     Creates a Snowflake source node using a named connection with a custom mapper.
    /// </summary>
    /// <typeparam name="T">The type of objects to read.</typeparam>
    /// <param name="query">The SQL query to execute.</param>
    /// <param name="connectionName">The name of the connection to use.</param>
    /// <param name="customMapper">Optional custom row mapper function.</param>
    /// <param name="configuration">Optional configuration. If null, defaults are used.</param>
    /// <returns>A configured Snowflake source node.</returns>
    SnowflakeSourceNode<T> CreateSourceNode<T>(
        string query,
        string? connectionName,
        Func<SnowflakeRow, T>? customMapper,
        SnowflakeConfiguration? configuration = null)
        where T : class;
}

/// <summary>
///     Factory for creating Snowflake source nodes with dependency injection support.
/// </summary>
public class SnowflakeSourceNodeFactory : ISnowflakeSourceNodeFactory
{
    private readonly ISnowflakeConnectionPool _connectionPool;

    /// <summary>
    ///     Initializes a new instance of the <see cref="SnowflakeSourceNodeFactory" /> class.
    /// </summary>
    /// <param name="connectionPool">The connection pool.</param>
    public SnowflakeSourceNodeFactory(ISnowflakeConnectionPool connectionPool)
    {
        _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));
    }

    /// <inheritdoc />
    public SnowflakeSourceNode<T> CreateSourceNode<T>(
        string query,
        SnowflakeConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(query);

        var config = configuration ?? new SnowflakeConfiguration();
        return new SnowflakeSourceNode<T>(_connectionPool, query, config, null, config.ContinueOnError);
    }

    /// <inheritdoc />
    public SnowflakeSourceNode<T> CreateSourceNode<T>(
        string query,
        Func<SnowflakeRow, T>? customMapper,
        SnowflakeConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(query);

        var config = configuration ?? new SnowflakeConfiguration();
        return new SnowflakeSourceNode<T>(_connectionPool, query, customMapper, config, null, config.ContinueOnError);
    }

    /// <inheritdoc />
    public SnowflakeSourceNode<T> CreateSourceNode<T>(
        string query,
        string? connectionName,
        SnowflakeConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(query);

        if (!string.IsNullOrWhiteSpace(connectionName) && !_connectionPool.HasNamedConnection(connectionName))
            throw new InvalidOperationException($"Named connection '{connectionName}' not found.");

        var config = configuration ?? new SnowflakeConfiguration();
        return new SnowflakeSourceNode<T>(_connectionPool, query, config, null, config.ContinueOnError, connectionName);
    }

    /// <inheritdoc />
    public SnowflakeSourceNode<T> CreateSourceNode<T>(
        string query,
        string? connectionName,
        Func<SnowflakeRow, T>? customMapper,
        SnowflakeConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(query);

        if (!string.IsNullOrWhiteSpace(connectionName) && !_connectionPool.HasNamedConnection(connectionName))
            throw new InvalidOperationException($"Named connection '{connectionName}' not found.");

        var config = configuration ?? new SnowflakeConfiguration();
        return new SnowflakeSourceNode<T>(_connectionPool, query, customMapper, config, null, config.ContinueOnError, connectionName);
    }
}

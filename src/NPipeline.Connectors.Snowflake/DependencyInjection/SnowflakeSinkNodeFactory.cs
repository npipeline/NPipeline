using NPipeline.Connectors.Snowflake.Configuration;
using NPipeline.Connectors.Snowflake.Connection;
using NPipeline.Connectors.Snowflake.Nodes;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Snowflake.DependencyInjection;

/// <summary>
///     Factory interface for creating Snowflake sink nodes with dependency injection support.
/// </summary>
public interface ISnowflakeSinkNodeFactory
{
    /// <summary>
    ///     Creates a Snowflake sink node using the default connection.
    /// </summary>
    /// <typeparam name="T">The type of objects to write.</typeparam>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="configuration">Optional configuration. If null, defaults are used.</param>
    /// <returns>A configured Snowflake sink node.</returns>
    SnowflakeSinkNode<T> CreateSinkNode<T>(
        string tableName,
        SnowflakeConfiguration? configuration = null)
        where T : class;

    /// <summary>
    ///     Creates a Snowflake sink node with a custom mapper using the default connection.
    /// </summary>
    /// <typeparam name="T">The type of objects to write.</typeparam>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="customMapper">Optional custom parameter mapper function.</param>
    /// <param name="configuration">Optional configuration. If null, defaults are used.</param>
    /// <returns>A configured Snowflake sink node.</returns>
    SnowflakeSinkNode<T> CreateSinkNode<T>(
        string tableName,
        Func<T, IEnumerable<DatabaseParameter>>? customMapper,
        SnowflakeConfiguration? configuration = null)
        where T : class;

    /// <summary>
    ///     Creates a Snowflake sink node using a named connection.
    /// </summary>
    /// <typeparam name="T">The type of objects to write.</typeparam>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="connectionName">The name of the connection to use.</param>
    /// <param name="configuration">Optional configuration. If null, defaults are used.</param>
    /// <returns>A configured Snowflake sink node.</returns>
    SnowflakeSinkNode<T> CreateSinkNode<T>(
        string tableName,
        string? connectionName,
        SnowflakeConfiguration? configuration = null)
        where T : class;

    /// <summary>
    ///     Creates a Snowflake sink node using a named connection with a custom mapper.
    /// </summary>
    /// <typeparam name="T">The type of objects to write.</typeparam>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="connectionName">The name of the connection to use.</param>
    /// <param name="customMapper">Optional custom parameter mapper function.</param>
    /// <param name="configuration">Optional configuration. If null, defaults are used.</param>
    /// <returns>A configured Snowflake sink node.</returns>
    SnowflakeSinkNode<T> CreateSinkNode<T>(
        string tableName,
        string? connectionName,
        Func<T, IEnumerable<DatabaseParameter>>? customMapper,
        SnowflakeConfiguration? configuration = null)
        where T : class;
}

/// <summary>
///     Factory for creating Snowflake sink nodes with dependency injection support.
/// </summary>
public class SnowflakeSinkNodeFactory : ISnowflakeSinkNodeFactory
{
    private readonly ISnowflakeConnectionPool _connectionPool;

    /// <summary>
    ///     Initializes a new instance of the <see cref="SnowflakeSinkNodeFactory" /> class.
    /// </summary>
    /// <param name="connectionPool">The connection pool.</param>
    public SnowflakeSinkNodeFactory(ISnowflakeConnectionPool connectionPool)
    {
        _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));
    }

    /// <inheritdoc />
    public SnowflakeSinkNode<T> CreateSinkNode<T>(
        string tableName,
        SnowflakeConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(tableName);

        var config = configuration ?? new SnowflakeConfiguration();
        return new SnowflakeSinkNode<T>(_connectionPool, tableName, config);
    }

    /// <inheritdoc />
    public SnowflakeSinkNode<T> CreateSinkNode<T>(
        string tableName,
        Func<T, IEnumerable<DatabaseParameter>>? customMapper,
        SnowflakeConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(tableName);

        var config = configuration ?? new SnowflakeConfiguration();
        return new SnowflakeSinkNode<T>(_connectionPool, tableName, config, customMapper);
    }

    /// <inheritdoc />
    public SnowflakeSinkNode<T> CreateSinkNode<T>(
        string tableName,
        string? connectionName,
        SnowflakeConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(tableName);

        if (!string.IsNullOrWhiteSpace(connectionName) && !_connectionPool.HasNamedConnection(connectionName))
            throw new InvalidOperationException($"Named connection '{connectionName}' not found.");

        var config = configuration ?? new SnowflakeConfiguration();
        return new SnowflakeSinkNode<T>(_connectionPool, tableName, config, null, connectionName);
    }

    /// <inheritdoc />
    public SnowflakeSinkNode<T> CreateSinkNode<T>(
        string tableName,
        string? connectionName,
        Func<T, IEnumerable<DatabaseParameter>>? customMapper,
        SnowflakeConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(tableName);

        if (!string.IsNullOrWhiteSpace(connectionName) && !_connectionPool.HasNamedConnection(connectionName))
            throw new InvalidOperationException($"Named connection '{connectionName}' not found.");

        var config = configuration ?? new SnowflakeConfiguration();
        return new SnowflakeSinkNode<T>(_connectionPool, tableName, config, customMapper, connectionName);
    }
}

using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.Connectors.SqlServer.Connection;
using NPipeline.Connectors.SqlServer.Mapping;
using NPipeline.Connectors.SqlServer.Nodes;

namespace NPipeline.Connectors.SqlServer.DependencyInjection;

/// <summary>
///     Factory interface for creating SQL Server source nodes with dependency injection support.
/// </summary>
public interface ISqlServerSourceNodeFactory
{
    /// <summary>
    ///     Creates a SQL Server source node using the default connection.
    /// </summary>
    /// <typeparam name="T">The type of objects to read.</typeparam>
    /// <param name="query">The SQL query to execute.</param>
    /// <param name="configuration">Optional configuration. If null, defaults are used.</param>
    /// <returns>A configured SQL Server source node.</returns>
    SqlServerSourceNode<T> CreateSourceNode<T>(
        string query,
        SqlServerConfiguration? configuration = null)
        where T : class;

    /// <summary>
    ///     Creates a SQL Server source node with a custom mapper using the default connection.
    /// </summary>
    /// <typeparam name="T">The type of objects to read.</typeparam>
    /// <param name="query">The SQL query to execute.</param>
    /// <param name="customMapper">Optional custom row mapper function.</param>
    /// <param name="configuration">Optional configuration. If null, defaults are used.</param>
    /// <returns>A configured SQL Server source node.</returns>
    SqlServerSourceNode<T> CreateSourceNode<T>(
        string query,
        Func<SqlServerRow, T>? customMapper,
        SqlServerConfiguration? configuration = null)
        where T : class;

    /// <summary>
    ///     Creates a SQL Server source node using a named connection.
    /// </summary>
    /// <typeparam name="T">The type of objects to read.</typeparam>
    /// <param name="query">The SQL query to execute.</param>
    /// <param name="connectionName">The name of the connection to use.</param>
    /// <param name="configuration">Optional configuration. If null, defaults are used.</param>
    /// <returns>A configured SQL Server source node.</returns>
    SqlServerSourceNode<T> CreateSourceNode<T>(
        string query,
        string? connectionName,
        SqlServerConfiguration? configuration = null)
        where T : class;

    /// <summary>
    ///     Creates a SQL Server source node using a named connection with a custom mapper.
    /// </summary>
    /// <typeparam name="T">The type of objects to read.</typeparam>
    /// <param name="query">The SQL query to execute.</param>
    /// <param name="connectionName">The name of the connection to use.</param>
    /// <param name="customMapper">Optional custom row mapper function.</param>
    /// <param name="configuration">Optional configuration. If null, defaults are used.</param>
    /// <returns>A configured SQL Server source node.</returns>
    SqlServerSourceNode<T> CreateSourceNode<T>(
        string query,
        string? connectionName,
        Func<SqlServerRow, T>? customMapper,
        SqlServerConfiguration? configuration = null)
        where T : class;
}

/// <summary>
///     Factory for creating SQL Server source nodes with dependency injection support.
/// </summary>
public class SqlServerSourceNodeFactory : ISqlServerSourceNodeFactory
{
    private readonly ISqlServerConnectionPool _connectionPool;

    /// <summary>
    ///     Initializes a new instance of the <see cref="SqlServerSourceNodeFactory" /> class.
    /// </summary>
    /// <param name="connectionPool">The connection pool.</param>
    public SqlServerSourceNodeFactory(ISqlServerConnectionPool connectionPool)
    {
        _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));
    }

    /// <inheritdoc />
    public SqlServerSourceNode<T> CreateSourceNode<T>(
        string query,
        SqlServerConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(query);

        var config = configuration ?? new SqlServerConfiguration();
        return new SqlServerSourceNode<T>(_connectionPool, query, config, null, config.ContinueOnError);
    }

    /// <inheritdoc />
    public SqlServerSourceNode<T> CreateSourceNode<T>(
        string query,
        Func<SqlServerRow, T>? customMapper,
        SqlServerConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(query);

        var config = configuration ?? new SqlServerConfiguration();
        return new SqlServerSourceNode<T>(_connectionPool, query, customMapper, config, null, config.ContinueOnError);
    }

    /// <inheritdoc />
    public SqlServerSourceNode<T> CreateSourceNode<T>(
        string query,
        string? connectionName,
        SqlServerConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(query);

        if (!string.IsNullOrWhiteSpace(connectionName) && !_connectionPool.HasNamedConnection(connectionName))
            throw new InvalidOperationException($"Named connection '{connectionName}' not found.");

        var config = configuration ?? new SqlServerConfiguration();
        return new SqlServerSourceNode<T>(_connectionPool, query, config, null, config.ContinueOnError, connectionName);
    }

    /// <inheritdoc />
    public SqlServerSourceNode<T> CreateSourceNode<T>(
        string query,
        string? connectionName,
        Func<SqlServerRow, T>? customMapper,
        SqlServerConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(query);

        if (!string.IsNullOrWhiteSpace(connectionName) && !_connectionPool.HasNamedConnection(connectionName))
            throw new InvalidOperationException($"Named connection '{connectionName}' not found.");

        var config = configuration ?? new SqlServerConfiguration();
        return new SqlServerSourceNode<T>(_connectionPool, query, customMapper, config, null, config.ContinueOnError, connectionName);
    }
}

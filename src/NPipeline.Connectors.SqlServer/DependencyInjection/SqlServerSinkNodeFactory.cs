using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.Connectors.SqlServer.Connection;
using NPipeline.Connectors.SqlServer.Nodes;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.SqlServer.DependencyInjection;

/// <summary>
///     Factory interface for creating SQL Server sink nodes with dependency injection support.
/// </summary>
public interface ISqlServerSinkNodeFactory
{
    /// <summary>
    ///     Creates a SQL Server sink node using the default connection.
    /// </summary>
    /// <typeparam name="T">The type of objects to write.</typeparam>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="configuration">Optional configuration. If null, defaults are used.</param>
    /// <returns>A configured SQL Server sink node.</returns>
    SqlServerSinkNode<T> CreateSinkNode<T>(
        string tableName,
        SqlServerConfiguration? configuration = null)
        where T : class;

    /// <summary>
    ///     Creates a SQL Server sink node with a custom mapper using the default connection.
    /// </summary>
    /// <typeparam name="T">The type of objects to write.</typeparam>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="customMapper">Optional custom parameter mapper function.</param>
    /// <param name="configuration">Optional configuration. If null, defaults are used.</param>
    /// <returns>A configured SQL Server sink node.</returns>
    SqlServerSinkNode<T> CreateSinkNode<T>(
        string tableName,
        Func<T, IEnumerable<DatabaseParameter>>? customMapper,
        SqlServerConfiguration? configuration = null)
        where T : class;

    /// <summary>
    ///     Creates a SQL Server sink node using a named connection.
    /// </summary>
    /// <typeparam name="T">The type of objects to write.</typeparam>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="connectionName">The name of the connection to use.</param>
    /// <param name="configuration">Optional configuration. If null, defaults are used.</param>
    /// <returns>A configured SQL Server sink node.</returns>
    SqlServerSinkNode<T> CreateSinkNode<T>(
        string tableName,
        string? connectionName,
        SqlServerConfiguration? configuration = null)
        where T : class;

    /// <summary>
    ///     Creates a SQL Server sink node using a named connection with a custom mapper.
    /// </summary>
    /// <typeparam name="T">The type of objects to write.</typeparam>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="connectionName">The name of the connection to use.</param>
    /// <param name="customMapper">Optional custom parameter mapper function.</param>
    /// <param name="configuration">Optional configuration. If null, defaults are used.</param>
    /// <returns>A configured SQL Server sink node.</returns>
    SqlServerSinkNode<T> CreateSinkNode<T>(
        string tableName,
        string? connectionName,
        Func<T, IEnumerable<DatabaseParameter>>? customMapper,
        SqlServerConfiguration? configuration = null)
        where T : class;
}

/// <summary>
///     Factory for creating SQL Server sink nodes with dependency injection support.
/// </summary>
public class SqlServerSinkNodeFactory : ISqlServerSinkNodeFactory
{
    private readonly ISqlServerConnectionPool _connectionPool;

    /// <summary>
    ///     Initializes a new instance of the <see cref="SqlServerSinkNodeFactory" /> class.
    /// </summary>
    /// <param name="connectionPool">The connection pool.</param>
    public SqlServerSinkNodeFactory(ISqlServerConnectionPool connectionPool)
    {
        _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));
    }

    /// <inheritdoc />
    public SqlServerSinkNode<T> CreateSinkNode<T>(
        string tableName,
        SqlServerConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(tableName);

        var config = configuration ?? new SqlServerConfiguration();
        return new SqlServerSinkNode<T>(_connectionPool, tableName, config);
    }

    /// <inheritdoc />
    public SqlServerSinkNode<T> CreateSinkNode<T>(
        string tableName,
        Func<T, IEnumerable<DatabaseParameter>>? customMapper,
        SqlServerConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(tableName);

        var config = configuration ?? new SqlServerConfiguration();
        return new SqlServerSinkNode<T>(_connectionPool, tableName, config, customMapper);
    }

    /// <inheritdoc />
    public SqlServerSinkNode<T> CreateSinkNode<T>(
        string tableName,
        string? connectionName,
        SqlServerConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(tableName);

        if (!string.IsNullOrWhiteSpace(connectionName) && !_connectionPool.HasNamedConnection(connectionName))
            throw new InvalidOperationException($"Named connection '{connectionName}' not found.");

        var config = configuration ?? new SqlServerConfiguration();
        return new SqlServerSinkNode<T>(_connectionPool, tableName, config, null, connectionName);
    }

    /// <inheritdoc />
    public SqlServerSinkNode<T> CreateSinkNode<T>(
        string tableName,
        string? connectionName,
        Func<T, IEnumerable<DatabaseParameter>>? customMapper,
        SqlServerConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(tableName);

        if (!string.IsNullOrWhiteSpace(connectionName) && !_connectionPool.HasNamedConnection(connectionName))
            throw new InvalidOperationException($"Named connection '{connectionName}' not found.");

        var config = configuration ?? new SqlServerConfiguration();
        return new SqlServerSinkNode<T>(_connectionPool, tableName, config, customMapper, connectionName);
    }
}

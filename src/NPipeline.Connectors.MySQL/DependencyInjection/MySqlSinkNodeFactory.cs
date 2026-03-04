using NPipeline.Connectors.MySql.Configuration;
using NPipeline.Connectors.MySql.Connection;
using NPipeline.Connectors.MySql.Nodes;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.MySql.DependencyInjection;

/// <summary>
///     Factory interface for creating <see cref="MySqlSinkNode{T}" /> instances with DI support.
/// </summary>
public interface IMySqlSinkNodeFactory
{
    /// <summary>Creates a sink node using the default connection.</summary>
    MySqlSinkNode<T> CreateSinkNode<T>(
        string tableName,
        MySqlConfiguration? configuration = null)
        where T : class;

    /// <summary>Creates a sink node with a custom mapper using the default connection.</summary>
    MySqlSinkNode<T> CreateSinkNode<T>(
        string tableName,
        Func<T, IEnumerable<DatabaseParameter>>? customMapper,
        MySqlConfiguration? configuration = null)
        where T : class;

    /// <summary>Creates a sink node using a named connection.</summary>
    MySqlSinkNode<T> CreateSinkNode<T>(
        string tableName,
        string? connectionName,
        MySqlConfiguration? configuration = null)
        where T : class;

    /// <summary>Creates a sink node using a named connection with a custom mapper.</summary>
    MySqlSinkNode<T> CreateSinkNode<T>(
        string tableName,
        string? connectionName,
        Func<T, IEnumerable<DatabaseParameter>>? customMapper,
        MySqlConfiguration? configuration = null)
        where T : class;
}

/// <summary>
///     Default implementation of <see cref="IMySqlSinkNodeFactory" />.
/// </summary>
public class MySqlSinkNodeFactory : IMySqlSinkNodeFactory
{
    private readonly IMySqlConnectionPool _connectionPool;

    /// <summary>Initialises a new <see cref="MySqlSinkNodeFactory" />.</summary>
    public MySqlSinkNodeFactory(IMySqlConnectionPool connectionPool)
    {
        _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));
    }

    /// <inheritdoc />
    public MySqlSinkNode<T> CreateSinkNode<T>(
        string tableName,
        MySqlConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(tableName);
        var config = configuration ?? new MySqlConfiguration();
        return new MySqlSinkNode<T>(_connectionPool, tableName, config);
    }

    /// <inheritdoc />
    public MySqlSinkNode<T> CreateSinkNode<T>(
        string tableName,
        Func<T, IEnumerable<DatabaseParameter>>? customMapper,
        MySqlConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(tableName);
        var config = configuration ?? new MySqlConfiguration();
        return new MySqlSinkNode<T>(_connectionPool, tableName, config, customMapper);
    }

    /// <inheritdoc />
    public MySqlSinkNode<T> CreateSinkNode<T>(
        string tableName,
        string? connectionName,
        MySqlConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(tableName);

        if (!string.IsNullOrWhiteSpace(connectionName) && !_connectionPool.HasNamedConnection(connectionName))
            throw new InvalidOperationException($"Named connection '{connectionName}' not found.");

        var config = configuration ?? new MySqlConfiguration();
        return new MySqlSinkNode<T>(_connectionPool, tableName, config, null, connectionName);
    }

    /// <inheritdoc />
    public MySqlSinkNode<T> CreateSinkNode<T>(
        string tableName,
        string? connectionName,
        Func<T, IEnumerable<DatabaseParameter>>? customMapper,
        MySqlConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(tableName);

        if (!string.IsNullOrWhiteSpace(connectionName) && !_connectionPool.HasNamedConnection(connectionName))
            throw new InvalidOperationException($"Named connection '{connectionName}' not found.");

        var config = configuration ?? new MySqlConfiguration();
        return new MySqlSinkNode<T>(_connectionPool, tableName, config, customMapper, connectionName);
    }
}

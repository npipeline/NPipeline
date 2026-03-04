using NPipeline.Connectors.MySql.Configuration;
using NPipeline.Connectors.MySql.Connection;
using NPipeline.Connectors.MySql.Mapping;
using NPipeline.Connectors.MySql.Nodes;

namespace NPipeline.Connectors.MySql.DependencyInjection;

/// <summary>
///     Factory interface for creating <see cref="MySqlSourceNode{T}" /> instances with DI support.
/// </summary>
public interface IMySqlSourceNodeFactory
{
    /// <summary>Creates a source node using the default connection.</summary>
    MySqlSourceNode<T> CreateSourceNode<T>(
        string query,
        MySqlConfiguration? configuration = null)
        where T : class;

    /// <summary>Creates a source node with a custom mapper using the default connection.</summary>
    MySqlSourceNode<T> CreateSourceNode<T>(
        string query,
        Func<MySqlRow, T>? customMapper,
        MySqlConfiguration? configuration = null)
        where T : class;

    /// <summary>Creates a source node using a named connection.</summary>
    MySqlSourceNode<T> CreateSourceNode<T>(
        string query,
        string? connectionName,
        MySqlConfiguration? configuration = null)
        where T : class;

    /// <summary>Creates a source node using a named connection with a custom mapper.</summary>
    MySqlSourceNode<T> CreateSourceNode<T>(
        string query,
        string? connectionName,
        Func<MySqlRow, T>? customMapper,
        MySqlConfiguration? configuration = null)
        where T : class;
}

/// <summary>
///     Default implementation of <see cref="IMySqlSourceNodeFactory" />.
/// </summary>
public class MySqlSourceNodeFactory : IMySqlSourceNodeFactory
{
    private readonly IMySqlConnectionPool _connectionPool;

    /// <summary>Initialises a new <see cref="MySqlSourceNodeFactory" />.</summary>
    public MySqlSourceNodeFactory(IMySqlConnectionPool connectionPool)
    {
        _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));
    }

    /// <inheritdoc />
    public MySqlSourceNode<T> CreateSourceNode<T>(
        string query,
        MySqlConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        var config = configuration ?? new MySqlConfiguration();
        return new MySqlSourceNode<T>(_connectionPool, query, config, null, config.ContinueOnError);
    }

    /// <inheritdoc />
    public MySqlSourceNode<T> CreateSourceNode<T>(
        string query,
        Func<MySqlRow, T>? customMapper,
        MySqlConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        var config = configuration ?? new MySqlConfiguration();
        return new MySqlSourceNode<T>(_connectionPool, query, customMapper, config, null, config.ContinueOnError);
    }

    /// <inheritdoc />
    public MySqlSourceNode<T> CreateSourceNode<T>(
        string query,
        string? connectionName,
        MySqlConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(query);

        if (!string.IsNullOrWhiteSpace(connectionName) && !_connectionPool.HasNamedConnection(connectionName))
            throw new InvalidOperationException($"Named connection '{connectionName}' not found.");

        var config = configuration ?? new MySqlConfiguration();
        return new MySqlSourceNode<T>(_connectionPool, query, config, null, config.ContinueOnError, connectionName);
    }

    /// <inheritdoc />
    public MySqlSourceNode<T> CreateSourceNode<T>(
        string query,
        string? connectionName,
        Func<MySqlRow, T>? customMapper,
        MySqlConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(query);

        if (!string.IsNullOrWhiteSpace(connectionName) && !_connectionPool.HasNamedConnection(connectionName))
            throw new InvalidOperationException($"Named connection '{connectionName}' not found.");

        var config = configuration ?? new MySqlConfiguration();
        return new MySqlSourceNode<T>(_connectionPool, query, customMapper, config, null, config.ContinueOnError, connectionName);
    }
}

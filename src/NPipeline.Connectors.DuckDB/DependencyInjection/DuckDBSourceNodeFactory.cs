using NPipeline.Connectors.DuckDB.Configuration;
using NPipeline.Connectors.DuckDB.Connection;
using NPipeline.Connectors.DuckDB.Mapping;
using NPipeline.Connectors.DuckDB.Nodes;

namespace NPipeline.Connectors.DuckDB.DependencyInjection;

/// <summary>
///     Factory for creating DuckDB source nodes with dependency injection support.
/// </summary>
public class DuckDBSourceNodeFactory
{
    private readonly DuckDBOptions _options;

    /// <summary>
    ///     Initializes a new instance of the <see cref="DuckDBSourceNodeFactory" /> class.
    /// </summary>
    /// <param name="options">The DuckDB options.</param>
    public DuckDBSourceNodeFactory(DuckDBOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>
    ///     Creates a DuckDB source node for a named database configured via DI.
    /// </summary>
    /// <typeparam name="T">The type of objects to read.</typeparam>
    /// <param name="databaseName">The named database to use, or null for the default.</param>
    /// <param name="query">The SQL query to execute.</param>
    /// <param name="rowMapper">Optional custom row mapper function.</param>
    /// <param name="configuration">Optional overrides. If null, the named/default configuration is used.</param>
    /// <returns>A configured DuckDB source node.</returns>
    public DuckDBSourceNode<T> CreateSource<T>(
        string? databaseName,
        string query,
        Func<DuckDBRow, T>? rowMapper = null,
        DuckDBConfiguration? configuration = null) where T : class
    {
        var config = ResolveConfiguration(databaseName, configuration);

        var factory = new DuckDBConnectionFactory(
            config.DatabasePath,
            config.AccessMode);

        return rowMapper is not null
            ? new DuckDBSourceNode<T>(factory, query, rowMapper, config)
            : new DuckDBSourceNode<T>(factory, query, config);
    }

    /// <summary>
    ///     Creates a DuckDB source node using the default database.
    /// </summary>
    /// <typeparam name="T">The type of objects to read.</typeparam>
    /// <param name="query">The SQL query to execute.</param>
    /// <param name="rowMapper">Optional custom row mapper function.</param>
    /// <param name="configuration">Optional configuration overrides.</param>
    /// <returns>A configured DuckDB source node.</returns>
    public DuckDBSourceNode<T> CreateSource<T>(
        string query,
        Func<DuckDBRow, T>? rowMapper = null,
        DuckDBConfiguration? configuration = null) where T : class
    {
        return CreateSource<T>(null, query, rowMapper, configuration);
    }

    private DuckDBConfiguration ResolveConfiguration(string? databaseName, DuckDBConfiguration? overrides)
    {
        if (overrides is not null)
            return overrides;

        if (databaseName is not null && _options.NamedDatabases.TryGetValue(databaseName, out var namedConfig))
            return namedConfig;

        return _options.DefaultConfiguration;
    }
}

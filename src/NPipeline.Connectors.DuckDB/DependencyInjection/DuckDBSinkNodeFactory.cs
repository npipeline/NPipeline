using NPipeline.Connectors.DuckDB.Configuration;
using NPipeline.Connectors.DuckDB.Connection;
using NPipeline.Connectors.DuckDB.Nodes;

namespace NPipeline.Connectors.DuckDB.DependencyInjection;

/// <summary>
///     Factory for creating DuckDB sink nodes with dependency injection support.
/// </summary>
public class DuckDBSinkNodeFactory
{
    private readonly DuckDBOptions _options;

    /// <summary>
    ///     Initializes a new instance of the <see cref="DuckDBSinkNodeFactory" /> class.
    /// </summary>
    /// <param name="options">The DuckDB options.</param>
    public DuckDBSinkNodeFactory(DuckDBOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>
    ///     Creates a DuckDB sink node for a named database configured via DI.
    /// </summary>
    /// <typeparam name="T">The type of objects to write.</typeparam>
    /// <param name="databaseName">The named database to use, or null for the default.</param>
    /// <param name="tableName">The target table name.</param>
    /// <param name="configuration">Optional overrides. If null, the named/default configuration is used.</param>
    /// <returns>A configured DuckDB sink node.</returns>
    public DuckDBSinkNode<T> CreateSink<T>(
        string? databaseName,
        string tableName,
        DuckDBConfiguration? configuration = null) where T : class
    {
        var config = ResolveConfiguration(databaseName, configuration);

        var factory = new DuckDBConnectionFactory(
            config.DatabasePath,
            config.AccessMode);

        return new DuckDBSinkNode<T>(factory, tableName, config);
    }

    /// <summary>
    ///     Creates a DuckDB sink node using the default database.
    /// </summary>
    /// <typeparam name="T">The type of objects to write.</typeparam>
    /// <param name="tableName">The target table name.</param>
    /// <param name="configuration">Optional configuration overrides.</param>
    /// <returns>A configured DuckDB sink node.</returns>
    public DuckDBSinkNode<T> CreateSink<T>(
        string tableName,
        DuckDBConfiguration? configuration = null) where T : class
    {
        return CreateSink<T>(null, tableName, configuration);
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

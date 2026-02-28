using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NPipeline.Connectors.DuckDB.Configuration;

namespace NPipeline.Connectors.DuckDB.DependencyInjection;

/// <summary>
///     Extension methods for configuring DuckDB connector in dependency injection.
/// </summary>
public static class DuckDBServiceCollectionExtensions
{
    /// <summary>
    ///     Adds the DuckDB connector to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddDuckDBConnector(
        this IServiceCollection services,
        Action<DuckDBOptions>? configure = null)
    {
        var options = new DuckDBOptions();
        configure?.Invoke(options);

        services.TryAddSingleton(options);
        services.TryAddSingleton<DuckDBSourceNodeFactory>();
        services.TryAddSingleton<DuckDBSinkNodeFactory>();

        return services;
    }

    /// <summary>
    ///     Adds a named DuckDB database to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="name">The database name.</param>
    /// <param name="databasePath">Path to the .duckdb file (null/empty for in-memory).</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddDuckDBDatabase(
        this IServiceCollection services,
        string name,
        string? databasePath,
        Action<DuckDBConfiguration>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(name);

        var options = services.FirstOrDefault(sd => sd.ServiceType == typeof(DuckDBOptions))
            ?.ImplementationInstance as DuckDBOptions ?? new DuckDBOptions();

        var config = new DuckDBConfiguration { DatabasePath = databasePath };
        configure?.Invoke(config);
        options.NamedDatabases[name] = config;

        services.TryAddSingleton(options);

        return services;
    }

    /// <summary>
    ///     Configures the default DuckDB database.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="databasePath">Path to the .duckdb file (null/empty for in-memory).</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddDefaultDuckDBDatabase(
        this IServiceCollection services,
        string? databasePath,
        Action<DuckDBConfiguration>? configure = null)
    {
        var options = services.FirstOrDefault(sd => sd.ServiceType == typeof(DuckDBOptions))
            ?.ImplementationInstance as DuckDBOptions ?? new DuckDBOptions();

        options.DefaultConfiguration.DatabasePath = databasePath;
        configure?.Invoke(options.DefaultConfiguration);

        services.TryAddSingleton(options);

        return services;
    }

    /// <summary>
    ///     Configures the default DuckDB database as in-memory.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddDuckDBInMemory(
        this IServiceCollection services,
        Action<DuckDBConfiguration>? configure = null)
    {
        return services.AddDefaultDuckDBDatabase(null, configure);
    }
}

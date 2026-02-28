using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NPipeline.Connectors.Snowflake.Configuration;
using NPipeline.Connectors.Snowflake.Connection;

namespace NPipeline.Connectors.Snowflake.DependencyInjection;

/// <summary>
///     Extension methods for configuring Snowflake connector in dependency injection.
/// </summary>
public static class SnowflakeServiceCollectionExtensions
{
    /// <summary>
    ///     Adds the Snowflake connector to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSnowflakeConnector(
        this IServiceCollection services,
        Action<SnowflakeOptions>? configure = null)
    {
        var options = new SnowflakeOptions();
        configure?.Invoke(options);

        services.TryAddSingleton(options);

        services.TryAddSingleton<ISnowflakeConnectionPool>(sp =>
        {
            var opts = sp.GetRequiredService<SnowflakeOptions>();
            return new SnowflakeConnectionPool(opts);
        });

        services.TryAddSingleton<ISnowflakeSourceNodeFactory, SnowflakeSourceNodeFactory>();
        services.TryAddSingleton<ISnowflakeSinkNodeFactory, SnowflakeSinkNodeFactory>();

        return services;
    }

    /// <summary>
    ///     Adds the Snowflake connector to the service collection with custom options.
    /// </summary>
    /// <typeparam name="TOptions">The options type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSnowflakeConnector<TOptions>(
        this IServiceCollection services,
        Action<TOptions>? configure = null)
        where TOptions : SnowflakeOptions, new()
    {
        var options = new TOptions();
        configure?.Invoke(options);

        services.TryAddSingleton<SnowflakeOptions>(options);

        services.TryAddSingleton<ISnowflakeConnectionPool>(sp =>
        {
            var opts = sp.GetRequiredService<SnowflakeOptions>();
            return new SnowflakeConnectionPool(opts);
        });

        services.TryAddSingleton<ISnowflakeSourceNodeFactory, SnowflakeSourceNodeFactory>();
        services.TryAddSingleton<ISnowflakeSinkNodeFactory, SnowflakeSinkNodeFactory>();

        return services;
    }

    /// <summary>
    ///     Adds a named Snowflake connection to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="name">The connection name.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSnowflakeConnection(
        this IServiceCollection services,
        string name,
        string connectionString)
    {
        var options = services.FirstOrDefault(sd => sd.ServiceType == typeof(SnowflakeOptions))?.ImplementationInstance as SnowflakeOptions
                      ?? new SnowflakeOptions();

        options.AddOrUpdateConnection(name, connectionString);
        services.TryAddSingleton(options);

        return services;
    }

    /// <summary>
    ///     Adds the default Snowflake connection to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddDefaultSnowflakeConnection(
        this IServiceCollection services,
        string connectionString)
    {
        var options = services.FirstOrDefault(sd => sd.ServiceType == typeof(SnowflakeOptions))?.ImplementationInstance as SnowflakeOptions
                      ?? new SnowflakeOptions();

        options.DefaultConnectionString = connectionString;
        services.TryAddSingleton(options);

        return services;
    }

    /// <summary>
    ///     Adds a keyed Snowflake connection pool to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="name">The connection name/key.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddKeyedSnowflakeConnection(
        this IServiceCollection services,
        string name,
        string connectionString)
    {
        _ = services.AddKeyedSingleton<ISnowflakeConnectionPool>(name, (sp, key) => { return new SnowflakeConnectionPool(connectionString); });

        return services;
    }
}

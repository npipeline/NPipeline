using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NPipeline.Connectors.MySql.Configuration;
using NPipeline.Connectors.MySql.Connection;

namespace NPipeline.Connectors.MySql.DependencyInjection;

/// <summary>
///     Extension methods for configuring the MySQL connector in an <see cref="IServiceCollection" />.
/// </summary>
public static class MySqlServiceCollectionExtensions
{
    /// <summary>
    ///     Adds the MySQL connector to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional callback to configure <see cref="MySqlOptions" />.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddMySqlConnector(
        this IServiceCollection services,
        Action<MySqlOptions>? configure = null)
    {
        var options = new MySqlOptions();
        configure?.Invoke(options);

        services.TryAddSingleton(options);

        services.TryAddSingleton<IMySqlConnectionPool>(sp =>
        {
            var opts = sp.GetRequiredService<MySqlOptions>();
            return new MySqlConnectionPool(opts);
        });

        services.TryAddSingleton<IMySqlSourceNodeFactory, MySqlSourceNodeFactory>();
        services.TryAddSingleton<IMySqlSinkNodeFactory, MySqlSinkNodeFactory>();

        return services;
    }

    /// <summary>
    ///     Adds the MySQL connector to the service collection using a custom options subclass.
    /// </summary>
    public static IServiceCollection AddMySqlConnector<TOptions>(
        this IServiceCollection services,
        Action<TOptions>? configure = null)
        where TOptions : MySqlOptions, new()
    {
        var options = new TOptions();
        configure?.Invoke(options);

        services.TryAddSingleton<MySqlOptions>(options);

        services.TryAddSingleton<IMySqlConnectionPool>(sp =>
        {
            var opts = sp.GetRequiredService<MySqlOptions>();
            return new MySqlConnectionPool(opts);
        });

        services.TryAddSingleton<IMySqlSourceNodeFactory, MySqlSourceNodeFactory>();
        services.TryAddSingleton<IMySqlSinkNodeFactory, MySqlSinkNodeFactory>();

        return services;
    }

    /// <summary>
    ///     Registers a named connection with the MySQL options.
    /// </summary>
    public static IServiceCollection AddMySqlConnection(
        this IServiceCollection services,
        string name,
        string connectionString)
    {
        var options = services
            .FirstOrDefault(sd => sd.ServiceType == typeof(MySqlOptions))
            ?.ImplementationInstance as MySqlOptions
                      ?? new MySqlOptions();

        options.AddOrUpdateConnection(name, connectionString);
        services.TryAddSingleton(options);

        return services;
    }

    /// <summary>
    ///     Sets the default MySQL connection string.
    /// </summary>
    public static IServiceCollection AddDefaultMySqlConnection(
        this IServiceCollection services,
        string connectionString)
    {
        var options = services
            .FirstOrDefault(sd => sd.ServiceType == typeof(MySqlOptions))
            ?.ImplementationInstance as MySqlOptions
                      ?? new MySqlOptions();

        options.DefaultConnectionString = connectionString;
        services.TryAddSingleton(options);

        return services;
    }

    /// <summary>
    ///     Adds a keyed (named) MySQL connection pool.
    /// </summary>
    public static IServiceCollection AddKeyedMySqlConnection(
        this IServiceCollection services,
        string name,
        string connectionString)
    {
        _ = services.AddKeyedSingleton<IMySqlConnectionPool>(
            name,
            (_, _) => new MySqlConnectionPool(connectionString));

        return services;
    }
}

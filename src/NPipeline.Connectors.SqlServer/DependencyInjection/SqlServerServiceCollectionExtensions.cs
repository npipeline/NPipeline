using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.Connectors.SqlServer.Connection;

namespace NPipeline.Connectors.SqlServer.DependencyInjection;

/// <summary>
///     Extension methods for configuring SQL Server connector in dependency injection.
/// </summary>
public static class SqlServerServiceCollectionExtensions
{
    /// <summary>
    ///     Adds the SQL Server connector to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSqlServerConnector(
        this IServiceCollection services,
        Action<SqlServerOptions>? configure = null)
    {
        var options = new SqlServerOptions();
        configure?.Invoke(options);

        services.TryAddSingleton(options);

        services.TryAddSingleton<ISqlServerConnectionPool>(sp =>
        {
            var opts = sp.GetRequiredService<SqlServerOptions>();
            return new SqlServerConnectionPool(opts);
        });

        services.TryAddSingleton<ISqlServerSourceNodeFactory, SqlServerSourceNodeFactory>();
        services.TryAddSingleton<ISqlServerSinkNodeFactory, SqlServerSinkNodeFactory>();

        return services;
    }

    /// <summary>
    ///     Adds the SQL Server connector to the service collection with custom options.
    /// </summary>
    /// <typeparam name="TOptions">The options type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSqlServerConnector<TOptions>(
        this IServiceCollection services,
        Action<TOptions>? configure = null)
        where TOptions : SqlServerOptions, new()
    {
        var options = new TOptions();
        configure?.Invoke(options);

        services.TryAddSingleton<SqlServerOptions>(options);

        services.TryAddSingleton<ISqlServerConnectionPool>(sp =>
        {
            var opts = sp.GetRequiredService<SqlServerOptions>();
            return new SqlServerConnectionPool(opts);
        });

        services.TryAddSingleton<ISqlServerSourceNodeFactory, SqlServerSourceNodeFactory>();
        services.TryAddSingleton<ISqlServerSinkNodeFactory, SqlServerSinkNodeFactory>();

        return services;
    }

    /// <summary>
    ///     Adds a named SQL Server connection to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="name">The connection name.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSqlServerConnection(
        this IServiceCollection services,
        string name,
        string connectionString)
    {
        var options = services.FirstOrDefault(sd => sd.ServiceType == typeof(SqlServerOptions))?.ImplementationInstance as SqlServerOptions
                      ?? new SqlServerOptions();

        options.AddOrUpdateConnection(name, connectionString);
        services.TryAddSingleton(options);

        return services;
    }

    /// <summary>
    ///     Adds the default SQL Server connection to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddDefaultSqlServerConnection(
        this IServiceCollection services,
        string connectionString)
    {
        var options = services.FirstOrDefault(sd => sd.ServiceType == typeof(SqlServerOptions))?.ImplementationInstance as SqlServerOptions
                      ?? new SqlServerOptions();

        options.DefaultConnectionString = connectionString;
        services.TryAddSingleton(options);

        return services;
    }

    /// <summary>
    ///     Adds a keyed SQL Server connection pool to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="name">The connection name/key.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddKeyedSqlServerConnection(
        this IServiceCollection services,
        string name,
        string connectionString)
    {
        _ = services.AddKeyedSingleton<ISqlServerConnectionPool>(name, (sp, key) => { return new SqlServerConnectionPool(connectionString); });

        return services;
    }
}

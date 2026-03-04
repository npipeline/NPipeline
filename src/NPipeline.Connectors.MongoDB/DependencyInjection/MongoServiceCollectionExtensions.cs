using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NPipeline.Connectors.MongoDB.Connection;

namespace NPipeline.Connectors.MongoDB.DependencyInjection;

/// <summary>
///     Extension methods for configuring MongoDB connector in dependency injection.
/// </summary>
public static class MongoServiceCollectionExtensions
{
    /// <summary>
    ///     Adds the MongoDB connector to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddMongoConnector(
        this IServiceCollection services,
        Action<MongoConnectorOptions>? configure = null)
    {
        var options = new MongoConnectorOptions();
        configure?.Invoke(options);

        services.TryAddSingleton(options);

        services.TryAddSingleton<IMongoConnectionPool>(sp =>
        {
            var opts = sp.GetRequiredService<MongoConnectorOptions>();
            return new MongoConnectionPool(opts);
        });

        services.TryAddSingleton<MongoSourceNodeFactory>();
        services.TryAddSingleton<MongoSinkNodeFactory>();

        return services;
    }

    /// <summary>
    ///     Adds a named MongoDB connection to the service collection.
    ///     This method should be called after AddMongoConnector to add additional connections.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="name">The connection name.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    /// <remarks>
    ///     Note: This method retrieves and modifies the singleton options instance if already registered.
    ///     For thread-safe configuration, prefer using the configure callback in AddMongoConnector.
    /// </remarks>
    public static IServiceCollection AddMongoConnection(
        this IServiceCollection services,
        string name,
        string connectionString)
    {
        // Find existing options or create new
        var existingDescriptor = services.FirstOrDefault(sd => sd.ServiceType == typeof(MongoConnectorOptions));

        if (existingDescriptor?.ImplementationInstance is MongoConnectorOptions existingOptions)
        {
            // Modify the existing instance directly (it's a singleton)
            existingOptions.AddOrUpdateConnection(name, connectionString);
        }
        else
        {
            // No existing options, create new and register
            var newOptions = new MongoConnectorOptions();
            newOptions.AddOrUpdateConnection(name, connectionString);
            services.TryAddSingleton(newOptions);
        }

        return services;
    }

    /// <summary>
    ///     Adds the default MongoDB connection to the service collection.
    ///     This method should be called after AddMongoConnector to set the default connection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    /// <remarks>
    ///     Note: This method retrieves and modifies the singleton options instance if already registered.
    ///     For thread-safe configuration, prefer using the configure callback in AddMongoConnector.
    /// </remarks>
    public static IServiceCollection AddDefaultMongoConnection(
        this IServiceCollection services,
        string connectionString)
    {
        // Find existing options or create new
        var existingDescriptor = services.FirstOrDefault(sd => sd.ServiceType == typeof(MongoConnectorOptions));

        if (existingDescriptor?.ImplementationInstance is MongoConnectorOptions existingOptions)
        {
            // Modify the existing instance directly (it's a singleton)
            existingOptions.DefaultConnectionString = connectionString;
        }
        else
        {
            // No existing options, create new and register
            var newOptions = new MongoConnectorOptions
            {
                DefaultConnectionString = connectionString,
            };

            services.TryAddSingleton(newOptions);
        }

        return services;
    }

    /// <summary>
    ///     Adds a keyed MongoDB connection pool to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="name">The connection name/key.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddKeyedMongoConnection(
        this IServiceCollection services,
        string name,
        string connectionString)
    {
        _ = services.AddKeyedSingleton<IMongoConnectionPool>(name, (sp, key) => new MongoConnectionPool(connectionString));

        return services;
    }
}

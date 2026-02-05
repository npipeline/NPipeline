using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.Configuration;

namespace NPipeline.Connectors.DependencyInjection;

/// <summary>
///     Extension methods for configuring database services in the dependency injection container.
/// </summary>
public static class DatabaseServiceCollectionExtensions
{
    /// <summary>
    ///     Adds database options to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">An optional action to configure the database options.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddDatabaseOptions(
        this IServiceCollection services,
        Action<DatabaseOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = new DatabaseOptions();
        configure?.Invoke(options);

        services.AddSingleton(options);

        return services;
    }
}

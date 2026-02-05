using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Configuration;

namespace NPipeline.Connectors.DependencyInjection;

/// <summary>
///     Provides extension methods for registering NPipeline connectors and storage providers in the dependency injection container.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    ///     Registers a storage provider as a transient service.
    /// </summary>
    /// <typeparam name="TProvider">The concrete type of the storage provider, which must implement <see cref="IStorageProvider" />.</typeparam>
    /// <param name="services">The service collection to add the provider to.</param>
    /// <returns>The same service collection for chaining.</returns>
    public static IServiceCollection AddStorageProvider<TProvider>(this IServiceCollection services)
        where TProvider : class, IStorageProvider
    {
        ArgumentNullException.ThrowIfNull(services);
        return services.AddTransient<IStorageProvider, TProvider>();
    }

    /// <summary>
    ///     Registers a specific storage provider instance as a singleton.
    /// </summary>
    /// <param name="services">The service collection to add the provider to.</param>
    /// <param name="instance">The storage provider instance to register.</param>
    /// <returns>The same service collection for chaining.</returns>
    public static IServiceCollection AddStorageProvider(this IServiceCollection services, IStorageProvider instance)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(instance);
        return services.AddSingleton(instance);
    }

    /// <summary>
    ///     Registers the default FileSystemStorageProvider for handling 'file' schemes.
    /// </summary>
    /// <param name="services">The service collection to add the provider to.</param>
    /// <returns>The same service collection for chaining.</returns>
    public static IServiceCollection AddDefaultFileStorageProvider(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddTransient<IStorageProvider, FileSystemStorageProvider>();
        return services;
    }

    /// <summary>
    ///     Scans for and registers all public, concrete <see cref="IStorageProvider" /> implementations from the calling assembly.
    /// </summary>
    /// <param name="services">The service collection to add providers to.</param>
    /// <param name="configure">An action to configure connector settings, including provider types and schemes.</param>
    /// <returns>The same service collection for chaining.</returns>
    public static IServiceCollection AddStorageProvidersFromConfiguration(
        this IServiceCollection services,
        Action<ConnectorConfiguration> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var config = new ConnectorConfiguration();
        configure(config);

        foreach (var providerConfig in config.Providers)
        {
            var providerType = Type.GetType(providerConfig.Value.ProviderType);

            if (providerType is null)

                // Skip invalid provider types silently to allow configuration flexibility
                continue;

            services.AddTransient(typeof(IStorageProvider), providerType);
        }

        return services;
    }

    /// <summary>
    ///     Registers the default <see cref="StorageResolver" /> to resolve providers based on URI schemes.
    /// </summary>
    /// <param name="services">The service collection to add the resolver to.</param>
    /// <param name="includeFileSystem">If true, automatically includes the FileSystemStorageProvider.</param>
    /// <returns>The same service collection for chaining.</returns>
    public static IServiceCollection AddStorageResolver(this IServiceCollection services, bool includeFileSystem = true)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddSingleton<IStorageResolver, StorageResolver>();

        if (includeFileSystem)
            services.AddDefaultFileStorageProvider();

        return services;
    }

    /// <summary>
    ///     A convenience method to register both the storage resolver and providers from the specified assemblies.
    /// </summary>
    /// <param name="services">The service collection to configure.</param>
    /// <param name="configure">An action to configure connector settings, including provider types and schemes.</param>
    /// <returns>The same service collection for chaining.</returns>
    public static IServiceCollection AddConnectorsFromConfiguration(
        this IServiceCollection services,
        Action<ConnectorConfiguration> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        return services
            .AddStorageProvidersFromConfiguration(configure)
            .AddStorageResolver(false); // don't auto-add file system, should be configured
    }
}

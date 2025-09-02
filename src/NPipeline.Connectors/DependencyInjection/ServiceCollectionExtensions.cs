using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Configuration;

namespace NPipeline.Connectors.DependencyInjection;

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
        ArgumentNullException.ThrowIfNull(instance);
        return services.AddSingleton(instance);
    }

    /// <summary>
    ///     Registers the default <see cref="FileSystemStorageProvider" /> for handling 'file' schemes.
    /// </summary>
    /// <param name="services">The service collection to add the provider to.</param>
    /// <returns>The same service collection for chaining.</returns>
    public static IServiceCollection AddDefaultFileStorageProvider(this IServiceCollection services)
    {
        return services.AddStorageProvider<FileSystemStorageProvider>();
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
    /// <param name="includeFileSystem">If true, automatically includes the <see cref="FileSystemStorageProvider" />.</param>
    /// <returns>The same service collection for chaining.</returns>
    public static IServiceCollection AddStorageResolver(this IServiceCollection services, bool includeFileSystem = true)
    {
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
        return services
            .AddStorageProvidersFromConfiguration(configure)
            .AddStorageResolver(false); // don't auto-add file system, should be configured
    }
}

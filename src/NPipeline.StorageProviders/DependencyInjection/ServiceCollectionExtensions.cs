using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.StorageProviders.DependencyInjection;

/// <summary>Registers storage providers and the storage resolver in the dependency injection container.</summary>
public static class ServiceCollectionExtensions
{
    /// <summary>Registers a storage provider as a singleton. Registering the same provider type twice has no effect.</summary>
    /// <typeparam name="TProvider">The provider type.</typeparam>
    /// <param name="services">The service collection to add the provider to.</param>
    /// <returns>The same service collection for chaining.</returns>
    public static IServiceCollection AddStorageProvider<TProvider>(this IServiceCollection services)
        where TProvider : class, IStorageProvider
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IStorageProvider, TProvider>());
        return services;
    }

    /// <summary>
    ///     Registers a storage provider instance. Unlike <see cref="AddStorageProvider{TProvider}" />, several instances of one
    ///     provider type can be registered (for example two S3 providers on different schemes); registering the same instance twice has no effect.
    /// </summary>
    /// <param name="services">The service collection to add the provider to.</param>
    /// <param name="instance">The provider instance.</param>
    /// <returns>The same service collection for chaining.</returns>
    public static IServiceCollection AddStorageProvider(this IServiceCollection services, IStorageProvider instance)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(instance);

        if (!services.Any(d => d.ServiceType == typeof(IStorageProvider) && ReferenceEquals(d.ImplementationInstance, instance)))
            services.AddSingleton(instance);

        return services;
    }

    /// <summary>Registers the file system provider, which serves <c>file://</c> URIs.</summary>
    /// <param name="services">The service collection to add the provider to.</param>
    /// <returns>The same service collection for chaining.</returns>
    public static IServiceCollection AddFileSystemStorageProvider(this IServiceCollection services) =>
        services.AddStorageProvider<FileSystemStorageProvider>();

    /// <summary>
    ///     Registers an <see cref="IStorageResolver" /> built from every <see cref="IStorageProvider" /> in the container,
    ///     including providers registered after this call. Two providers that serve the same scheme fail when the resolver is first resolved.
    /// </summary>
    /// <param name="services">The service collection to add the resolver to.</param>
    /// <param name="includeFileSystem">When <see langword="true" />, also registers the file system provider.</param>
    /// <returns>The same service collection for chaining.</returns>
    public static IServiceCollection AddStorageResolver(this IServiceCollection services, bool includeFileSystem = true)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddSingleton<IStorageResolver>(static sp => new StorageResolver(sp.GetServices<IStorageProvider>()));

        return includeFileSystem ? services.AddFileSystemStorageProvider() : services;
    }
}

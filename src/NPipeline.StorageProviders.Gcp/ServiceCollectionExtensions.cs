using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.StorageProviders.Gcp;

/// <summary>
///     Extension methods for configuring GCS storage provider in dependency injection.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    ///     Adds GCS storage provider to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action for GCS storage provider options.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddGcsStorageProvider(
        this IServiceCollection services,
        Action<GcsStorageProviderOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = new GcsStorageProviderOptions();
        configure?.Invoke(options);

        return Register(services, options);
    }

    /// <summary>
    ///     Adds GCS storage provider to the service collection with pre-configured options.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="options">The GCS storage provider options.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddGcsStorageProvider(
        this IServiceCollection services,
        GcsStorageProviderOptions options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);

        return Register(services, options);
    }

    private static IServiceCollection Register(IServiceCollection services, GcsStorageProviderOptions options)
    {
        options.Validate();

        services.TryAddSingleton(options);
        services.TryAddSingleton<GcsClientFactory>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IStorageProvider, GcsStorageProvider>());

        return services;
    }
}

using Microsoft.Extensions.DependencyInjection;
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

        // Configure options
        var options = new GcsStorageProviderOptions();
        configure?.Invoke(options);
        options.Validate();

        // Register options as singleton
        _ = services.AddSingleton(options);

        // Register GcsClientFactory as singleton
        _ = services.AddSingleton<GcsClientFactory>();

        // Register GcsStorageProvider as singleton
        _ = services.AddSingleton<GcsStorageProvider>();
        _ = services.AddSingleton<IStorageProvider>(sp => sp.GetRequiredService<GcsStorageProvider>());
        _ = services.AddSingleton<IStorageProviderMetadataProvider>(sp => sp.GetRequiredService<GcsStorageProvider>());

        return services;
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
        options.Validate();

        // Register options as singleton
        _ = services.AddSingleton(options);

        // Register GcsClientFactory as singleton
        _ = services.AddSingleton<GcsClientFactory>();

        // Register GcsStorageProvider as singleton
        _ = services.AddSingleton<GcsStorageProvider>();
        _ = services.AddSingleton<IStorageProvider>(sp => sp.GetRequiredService<GcsStorageProvider>());
        _ = services.AddSingleton<IStorageProviderMetadataProvider>(sp => sp.GetRequiredService<GcsStorageProvider>());

        return services;
    }
}

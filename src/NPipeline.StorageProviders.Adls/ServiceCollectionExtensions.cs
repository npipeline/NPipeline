using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.StorageProviders.Adls;

/// <summary>
///     Extension methods for configuring ADLS Gen2 storage provider in dependency injection.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    ///     Adds ADLS Gen2 storage provider to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action for ADLS Gen2 storage provider options.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddAdlsGen2StorageProvider(
        this IServiceCollection services,
        Action<AdlsGen2StorageProviderOptions>? configure = null)
    {
        // Configure options
        var options = new AdlsGen2StorageProviderOptions();
        configure?.Invoke(options);

        // Register options as singleton
        _ = services.AddSingleton(options);

        // Register AdlsGen2ClientFactory as singleton
        _ = services.AddSingleton<AdlsGen2ClientFactory>();

        // Register AdlsGen2StorageProvider as singleton
        _ = services.AddSingleton<AdlsGen2StorageProvider>();
        _ = services.AddSingleton<IStorageProvider>(sp => sp.GetRequiredService<AdlsGen2StorageProvider>());
        _ = services.AddSingleton<IDeletableStorageProvider>(sp => sp.GetRequiredService<AdlsGen2StorageProvider>());
        _ = services.AddSingleton<IMoveableStorageProvider>(sp => sp.GetRequiredService<AdlsGen2StorageProvider>());
        _ = services.AddSingleton<IStorageProviderMetadataProvider>(sp => sp.GetRequiredService<AdlsGen2StorageProvider>());

        return services;
    }

    /// <summary>
    ///     Adds ADLS Gen2 storage provider to the service collection using a pre-built options instance.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="options">Pre-configured ADLS Gen2 storage provider options.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddAdlsGen2StorageProvider(
        this IServiceCollection services,
        AdlsGen2StorageProviderOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        _ = services.AddSingleton(options);
        _ = services.AddSingleton<AdlsGen2ClientFactory>();
        _ = services.AddSingleton<AdlsGen2StorageProvider>();
        _ = services.AddSingleton<IStorageProvider>(sp => sp.GetRequiredService<AdlsGen2StorageProvider>());
        _ = services.AddSingleton<IDeletableStorageProvider>(sp => sp.GetRequiredService<AdlsGen2StorageProvider>());
        _ = services.AddSingleton<IMoveableStorageProvider>(sp => sp.GetRequiredService<AdlsGen2StorageProvider>());
        _ = services.AddSingleton<IStorageProviderMetadataProvider>(sp => sp.GetRequiredService<AdlsGen2StorageProvider>());

        return services;
    }

}

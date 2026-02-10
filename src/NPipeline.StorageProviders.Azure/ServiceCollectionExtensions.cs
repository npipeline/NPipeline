using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.StorageProviders.Azure;

/// <summary>
///     Extension methods for configuring Azure Blob storage provider in dependency injection.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    ///     Adds Azure Blob storage provider to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action for Azure storage provider options.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddAzureBlobStorageProvider(
        this IServiceCollection services,
        Action<AzureBlobStorageProviderOptions>? configure = null)
    {
        // Configure options
        var options = new AzureBlobStorageProviderOptions();
        configure?.Invoke(options);

        // Register options as singleton
        _ = services.AddSingleton(options);

        // Register AzureBlobClientFactory as singleton
        _ = services.AddSingleton<AzureBlobClientFactory>();

        // Register AzureBlobStorageProvider as singleton
        _ = services.AddSingleton<AzureBlobStorageProvider>();
        _ = services.AddSingleton<IStorageProvider>(sp => sp.GetRequiredService<AzureBlobStorageProvider>());
        _ = services.AddSingleton<IStorageProviderMetadataProvider>(sp => sp.GetRequiredService<AzureBlobStorageProvider>());

        return services;
    }

    /// <summary>
    ///     Adds Azure Blob storage provider to the service collection with pre-configured options.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="options">The Azure storage provider options.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddAzureBlobStorageProvider(
        this IServiceCollection services,
        AzureBlobStorageProviderOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        // Register options as singleton
        _ = services.AddSingleton(options);

        // Register AzureBlobClientFactory as singleton
        _ = services.AddSingleton<AzureBlobClientFactory>();

        // Register AzureBlobStorageProvider as singleton
        _ = services.AddSingleton<AzureBlobStorageProvider>();
        _ = services.AddSingleton<IStorageProvider>(sp => sp.GetRequiredService<AzureBlobStorageProvider>());
        _ = services.AddSingleton<IStorageProviderMetadataProvider>(sp => sp.GetRequiredService<AzureBlobStorageProvider>());

        return services;
    }
}

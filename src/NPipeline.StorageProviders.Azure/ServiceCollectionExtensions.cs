using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
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
        ArgumentNullException.ThrowIfNull(services);

        var options = new AzureBlobStorageProviderOptions();
        configure?.Invoke(options);

        return Register(services, options);
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
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);

        return Register(services, options);
    }

    private static IServiceCollection Register(IServiceCollection services, AzureBlobStorageProviderOptions options)
    {
        services.TryAddSingleton(options);
        services.TryAddSingleton<AzureBlobClientFactory>();
        services.TryAddSingleton<AzureBlobStorageProvider>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IStorageProvider, AzureBlobStorageProvider>());

        return services;
    }
}

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
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
        ArgumentNullException.ThrowIfNull(services);

        var options = new AdlsGen2StorageProviderOptions();
        configure?.Invoke(options);

        return Register(services, options);
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
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);

        return Register(services, options);
    }

    private static IServiceCollection Register(IServiceCollection services, AdlsGen2StorageProviderOptions options)
    {
        services.TryAddSingleton(options);
        services.TryAddSingleton<AdlsGen2ClientFactory>();
        services.TryAddSingleton<AdlsGen2StorageProvider>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IStorageProvider, AdlsGen2StorageProvider>());

        return services;
    }
}

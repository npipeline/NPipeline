using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.StorageProviders.Sftp;

/// <summary>
///     Extension methods for configuring SFTP storage provider in dependency injection.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    ///     Adds SFTP storage provider to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action for SFTP storage provider options.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSftpStorageProvider(
        this IServiceCollection services,
        Action<SftpStorageProviderOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = new SftpStorageProviderOptions();
        configure?.Invoke(options);

        return Register(services, options);
    }

    /// <summary>
    ///     Adds SFTP storage provider to the service collection with pre-configured options.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="options">The SFTP storage provider options.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSftpStorageProvider(
        this IServiceCollection services,
        SftpStorageProviderOptions options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);

        return Register(services, options);
    }

    private static IServiceCollection Register(IServiceCollection services, SftpStorageProviderOptions options)
    {
        services.TryAddSingleton(options);
        services.TryAddSingleton<SftpClientFactory>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IStorageProvider, SftpStorageProvider>());

        return services;
    }
}

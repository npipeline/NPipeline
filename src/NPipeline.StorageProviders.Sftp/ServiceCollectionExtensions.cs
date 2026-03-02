using Microsoft.Extensions.DependencyInjection;
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
        // Configure options
        var options = new SftpStorageProviderOptions();
        configure?.Invoke(options);

        // Register options as singleton
        _ = services.AddSingleton(options);

        // Register SftpClientFactory as singleton
        _ = services.AddSingleton<SftpClientFactory>();

        // Register SftpStorageProvider as singleton
        _ = services.AddSingleton<SftpStorageProvider>();
        _ = services.AddSingleton<IStorageProvider>(sp => sp.GetRequiredService<SftpStorageProvider>());
        _ = services.AddSingleton<IStorageProviderMetadataProvider>(sp => sp.GetRequiredService<SftpStorageProvider>());

        return services;
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
        ArgumentNullException.ThrowIfNull(options);

        // Register options as singleton
        _ = services.AddSingleton(options);

        // Register SftpClientFactory as singleton
        _ = services.AddSingleton<SftpClientFactory>();

        // Register SftpStorageProvider as singleton
        _ = services.AddSingleton<SftpStorageProvider>();
        _ = services.AddSingleton<IStorageProvider>(sp => sp.GetRequiredService<SftpStorageProvider>());
        _ = services.AddSingleton<IStorageProviderMetadataProvider>(sp => sp.GetRequiredService<SftpStorageProvider>());

        return services;
    }
}

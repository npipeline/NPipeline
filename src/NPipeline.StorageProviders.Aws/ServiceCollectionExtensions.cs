using Microsoft.Extensions.DependencyInjection;

namespace NPipeline.StorageProviders.Aws;

/// <summary>
///     Extension methods for configuring S3 storage provider in dependency injection.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    ///     Adds S3 storage provider to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action for S3 storage provider options.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddS3StorageProvider(
        this IServiceCollection services,
        Action<S3StorageProviderOptions>? configure = null)
    {
        // Configure options
        var options = new S3StorageProviderOptions();
        configure?.Invoke(options);

        // Register options as singleton
        _ = services.AddSingleton(options);

        // Register S3ClientFactory as singleton
        _ = services.AddSingleton<S3ClientFactory>();

        // Register S3StorageProvider as singleton
        _ = services.AddSingleton<S3StorageProvider>();

        return services;
    }

    /// <summary>
    ///     Adds S3 storage provider to the service collection with pre-configured options.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="options">The S3 storage provider options.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddS3StorageProvider(
        this IServiceCollection services,
        S3StorageProviderOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        // Register options as singleton
        _ = services.AddSingleton(options);

        // Register S3ClientFactory as singleton
        _ = services.AddSingleton<S3ClientFactory>();

        // Register S3StorageProvider as singleton
        _ = services.AddSingleton<S3StorageProvider>();

        return services;
    }
}

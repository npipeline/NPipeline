using Microsoft.Extensions.DependencyInjection;

namespace NPipeline.StorageProviders.S3.Compatible;

/// <summary>
///     Extension methods for configuring S3-compatible storage provider in dependency injection.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    ///     Adds S3-compatible storage provider to the service collection with pre-configured options.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="options">The S3-compatible storage provider options.</param>
    /// <returns>The service collection for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when required options are not provided.</exception>
    public static IServiceCollection AddS3CompatibleStorageProvider(
        this IServiceCollection services,
        S3CompatibleStorageProviderOptions options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);

        // Validate required fields
        ValidateOptions(options);

        // Register options as singleton
        _ = services.AddSingleton(options);

        // Register S3CompatibleClientFactory as singleton
        _ = services.AddSingleton<S3CompatibleClientFactory>();

        // Register S3CompatibleStorageProvider as singleton
        _ = services.AddSingleton<S3CompatibleStorageProvider>();

        return services;
    }

    private static void ValidateOptions(S3CompatibleStorageProviderOptions options)
    {
        if (options.ServiceUrl is null)
            throw new ArgumentException("ServiceUrl is required for S3-compatible storage provider.", nameof(options));

        if (string.IsNullOrWhiteSpace(options.AccessKey))
            throw new ArgumentException("AccessKey is required for S3-compatible storage provider.", nameof(options));

        if (string.IsNullOrWhiteSpace(options.SecretKey))
            throw new ArgumentException("SecretKey is required for S3-compatible storage provider.", nameof(options));
    }
}

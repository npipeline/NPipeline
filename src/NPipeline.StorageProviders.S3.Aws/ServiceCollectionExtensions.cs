using Microsoft.Extensions.DependencyInjection;

namespace NPipeline.StorageProviders.S3.Aws;

/// <summary>
///     Extension methods for configuring AWS S3 storage provider in dependency injection.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    ///     Adds AWS S3 storage provider to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action for AWS S3 storage provider options.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddAwsS3StorageProvider(
        this IServiceCollection services,
        Action<AwsS3StorageProviderOptions>? configure = null)
    {
        // Configure options
        var options = new AwsS3StorageProviderOptions();
        configure?.Invoke(options);

        // Register options as singleton
        _ = services.AddSingleton(options);

        // Register AwsS3ClientFactory as singleton
        _ = services.AddSingleton<AwsS3ClientFactory>();

        // Register AwsS3StorageProvider as singleton
        _ = services.AddSingleton<AwsS3StorageProvider>();

        return services;
    }

    /// <summary>
    ///     Adds AWS S3 storage provider to the service collection with pre-configured options.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="options">The AWS S3 storage provider options.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddAwsS3StorageProvider(
        this IServiceCollection services,
        AwsS3StorageProviderOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        // Register options as singleton
        _ = services.AddSingleton(options);

        // Register AwsS3ClientFactory as singleton
        _ = services.AddSingleton<AwsS3ClientFactory>();

        // Register AwsS3StorageProvider as singleton
        _ = services.AddSingleton<AwsS3StorageProvider>();

        return services;
    }
}

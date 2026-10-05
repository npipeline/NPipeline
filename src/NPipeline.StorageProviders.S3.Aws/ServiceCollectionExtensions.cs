using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NPipeline.StorageProviders.Abstractions;

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
        ArgumentNullException.ThrowIfNull(services);

        var options = new AwsS3StorageProviderOptions();
        configure?.Invoke(options);

        return Register(services, options);
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
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);

        return Register(services, options);
    }

    private static IServiceCollection Register(IServiceCollection services, AwsS3StorageProviderOptions options)
    {
        services.TryAddSingleton(options);
        services.TryAddSingleton<AwsS3ClientFactory>();
        services.TryAddSingleton<AwsS3StorageProvider>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IStorageProvider, AwsS3StorageProvider>());

        return services;
    }
}

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

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

        services.TryAddSingleton(options);
        services.TryAddSingleton<S3CompatibleClientFactory>();
        services.TryAddSingleton<S3CompatibleStorageProvider>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IStorageProvider, S3CompatibleStorageProvider>());

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

        if (options.Schemes is null || options.Schemes.Count == 0)
            throw new ArgumentException("At least one scheme is required for S3-compatible storage provider.", nameof(options));

        foreach (var scheme in options.Schemes)
        {
            if (!StorageScheme.IsValid(scheme))
                throw new ArgumentException($"'{scheme}' is not a valid storage scheme.", nameof(options));
        }
    }
}

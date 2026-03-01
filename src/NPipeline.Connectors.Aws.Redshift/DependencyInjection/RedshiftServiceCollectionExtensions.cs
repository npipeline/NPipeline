using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Connection;

namespace NPipeline.Connectors.Aws.Redshift.DependencyInjection;

/// <summary>
///     Extension methods for configuring Redshift connector in dependency injection.
/// </summary>
public static class RedshiftServiceCollectionExtensions
{
    /// <summary>
    ///     Adds the Redshift connector to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddRedshiftConnector(
        this IServiceCollection services,
        Action<RedshiftOptions>? configure = null)
    {
        var options = new RedshiftOptions();
        configure?.Invoke(options);

        services.TryAddSingleton(options);

        services.TryAddSingleton<IRedshiftConnectionPool>(sp =>
        {
            var opts = sp.GetRequiredService<RedshiftOptions>();
            return new RedshiftConnectionPool(opts);
        });

        services.TryAddSingleton<IRedshiftSourceNodeFactory, RedshiftSourceNodeFactory>();
        services.TryAddSingleton<IRedshiftSinkNodeFactory, RedshiftSinkNodeFactory>();

        services.TryAddSingleton<IAmazonS3>(sp =>
        {
            var opts = sp.GetRequiredService<RedshiftOptions>();
            var config = opts.DefaultConfiguration;

            var region = string.IsNullOrWhiteSpace(config.AwsRegion)
                ? null
                : RegionEndpoint.GetBySystemName(config.AwsRegion);

            if (!string.IsNullOrWhiteSpace(config.AwsAccessKeyId)
                && !string.IsNullOrWhiteSpace(config.AwsSecretAccessKey))
            {
                var credentials = new BasicAWSCredentials(config.AwsAccessKeyId, config.AwsSecretAccessKey);

                return region is null
                    ? new AmazonS3Client(credentials)
                    : new AmazonS3Client(credentials, region);
            }

            return region is null
                ? new AmazonS3Client()
                : new AmazonS3Client(region);
        });

        return services;
    }

    /// <summary>
    ///     Adds a named Redshift connection to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="name">The connection name.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddRedshiftConnection(
        this IServiceCollection services,
        string name,
        string connectionString)
    {
        var options = services.FirstOrDefault(sd => sd.ServiceType == typeof(RedshiftOptions))?.ImplementationInstance as RedshiftOptions
                      ?? new RedshiftOptions();

        options.AddOrUpdateConnection(name, connectionString);
        services.TryAddSingleton(options);

        return services;
    }

    /// <summary>
    ///     Adds the default Redshift connection to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddDefaultRedshiftConnection(
        this IServiceCollection services,
        string connectionString)
    {
        var options = services.FirstOrDefault(sd => sd.ServiceType == typeof(RedshiftOptions))?.ImplementationInstance as RedshiftOptions
                      ?? new RedshiftOptions();

        options.DefaultConnectionString = connectionString;
        services.TryAddSingleton(options);

        return services;
    }

    /// <summary>
    ///     Adds a keyed Redshift connection pool to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="name">The connection name/key.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddKeyedRedshiftConnection(
        this IServiceCollection services,
        string name,
        string connectionString)
    {
        _ = services.AddKeyedSingleton<IRedshiftConnectionPool>(name, (sp, key) => { return new RedshiftConnectionPool(connectionString); });

        return services;
    }
}

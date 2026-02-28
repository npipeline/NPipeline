using NPipeline.StorageProviders;

namespace NPipeline.Connectors.Snowflake;

/// <summary>
///     Factory class for creating storage resolvers with Snowflake support.
/// </summary>
/// <remarks>
///     This factory provides a convenient way to create a <see cref="StorageResolver" />
///     pre-configured with the <see cref="SnowflakeDatabaseStorageProvider" />.
/// </remarks>
public static class SnowflakeStorageResolverFactory
{
    /// <summary>
    ///     Creates a new <see cref="StorageResolver" /> with the Snowflake provider registered.
    /// </summary>
    /// <returns>
    ///     A <see cref="StorageResolver" /> instance with <see cref="SnowflakeDatabaseStorageProvider" /> registered.
    /// </returns>
    public static StorageResolver CreateResolver()
    {
        var resolver = new StorageResolver();
        resolver.RegisterProvider(new SnowflakeDatabaseStorageProvider());
        return resolver;
    }
}

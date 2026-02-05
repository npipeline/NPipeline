using NPipeline.StorageProviders;

namespace NPipeline.Connectors.PostgreSQL;

/// <summary>
///     Factory class for creating storage resolvers with PostgreSQL support.
/// </summary>
/// <remarks>
///     This factory provides a convenient way to create a <see cref="StorageResolver" />
///     pre-configured with the <see cref="PostgresDatabaseStorageProvider" />.
///     This allows PostgreSQL connector nodes to use a default resolver that includes
///     the PostgreSQL provider without requiring manual registration.
/// </remarks>
public static class PostgresStorageResolverFactory
{
    /// <summary>
    ///     Creates a new <see cref="StorageResolver" /> with the PostgreSQL provider registered.
    /// </summary>
    /// <returns>
    ///     A <see cref="StorageResolver" /> instance with <see cref="PostgresDatabaseStorageProvider" /> registered.
    /// </returns>
    /// <remarks>
    ///     The returned resolver can be used to resolve PostgreSQL storage URIs to
    ///     <see cref="PostgresDatabaseStorageProvider" /> instances. Additional providers
    ///     can be registered on the returned resolver if needed.
    /// </remarks>
    public static StorageResolver CreateResolver()
    {
        var resolver = new StorageResolver();
        resolver.RegisterProvider(new PostgresDatabaseStorageProvider());
        return resolver;
    }
}

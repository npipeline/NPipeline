using NPipeline.StorageProviders;

namespace NPipeline.Connectors.MongoDB;

/// <summary>
///     Factory class for creating storage resolvers with MongoDB support.
/// </summary>
/// <remarks>
///     This factory provides a convenient way to create a <see cref="StorageResolver" />
///     pre-configured with the <see cref="MongoDatabaseStorageProvider" />.
///     This allows MongoDB connector nodes to use a default resolver that includes
///     the MongoDB provider without requiring manual registration.
/// </remarks>
public static class MongoStorageResolverFactory
{
    /// <summary>
    ///     Creates a new <see cref="StorageResolver" /> with the MongoDB provider registered.
    /// </summary>
    /// <returns>
    ///     A <see cref="StorageResolver" /> instance with <see cref="MongoDatabaseStorageProvider" /> registered.
    /// </returns>
    /// <remarks>
    ///     The returned resolver can be used to resolve MongoDB storage URIs to
    ///     <see cref="MongoDatabaseStorageProvider" /> instances. Additional providers
    ///     can be registered on the returned resolver if needed.
    /// </remarks>
    public static StorageResolver CreateResolver()
    {
        var resolver = new StorageResolver();
        resolver.RegisterProvider(new MongoDatabaseStorageProvider());
        return resolver;
    }
}

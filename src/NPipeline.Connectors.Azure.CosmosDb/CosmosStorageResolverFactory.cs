using NPipeline.Connectors.Azure.CosmosDb.StorageProvider;
using NPipeline.StorageProviders;

namespace NPipeline.Connectors.Azure.CosmosDb;

/// <summary>
///     Factory class for creating storage resolvers with Cosmos DB support.
/// </summary>
/// <remarks>
///     This factory provides a convenient way to create a <see cref="StorageResolver" />
///     pre-configured with the <see cref="CosmosDatabaseStorageProvider" />.
///     This allows Cosmos DB connector nodes to use a default resolver that includes
///     the Cosmos DB provider without requiring manual registration.
/// </remarks>
public static class CosmosStorageResolverFactory
{
    /// <summary>
    ///     Creates a new <see cref="StorageResolver" /> with the Cosmos DB provider registered.
    /// </summary>
    /// <returns>
    ///     A <see cref="StorageResolver" /> instance with <see cref="CosmosDatabaseStorageProvider" /> registered.
    /// </returns>
    /// <remarks>
    ///     The returned resolver can be used to resolve Cosmos DB storage URIs to
    ///     <see cref="CosmosDatabaseStorageProvider" /> instances. Additional providers
    ///     can be registered on the returned resolver if needed.
    /// </remarks>
    public static StorageResolver CreateResolver()
    {
        var resolver = new StorageResolver();
        resolver.RegisterProvider(new CosmosDatabaseStorageProvider());
        return resolver;
    }
}

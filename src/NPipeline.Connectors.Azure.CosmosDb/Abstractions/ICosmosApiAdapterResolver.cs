using NPipeline.Connectors.Azure.CosmosDb.Configuration;

namespace NPipeline.Connectors.Azure.CosmosDb.Abstractions;

/// <summary>
///     Resolves API adapters for Cosmos SQL, Mongo, and Cassandra.
/// </summary>
public interface ICosmosApiAdapterResolver
{
    /// <summary>
    ///     Resolves an adapter by API type.
    /// </summary>
    ICosmosApiAdapter GetAdapter(CosmosApiType apiType);

    /// <summary>
    ///     Resolves an adapter by URI scheme.
    /// </summary>
    ICosmosApiAdapter GetAdapter(string scheme);
}

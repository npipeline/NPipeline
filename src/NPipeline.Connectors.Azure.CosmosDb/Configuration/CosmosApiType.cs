namespace NPipeline.Connectors.Azure.CosmosDb.Configuration;

/// <summary>
///     Supported Cosmos APIs in this connector.
/// </summary>
public enum CosmosApiType
{
    /// <summary>
    ///     Cosmos SQL (Core) API.
    /// </summary>
    Sql = 0,

    /// <summary>
    ///     Cosmos Mongo API.
    /// </summary>
    Mongo = 1,

    /// <summary>
    ///     Cosmos Cassandra API.
    /// </summary>
    Cassandra = 2,
}

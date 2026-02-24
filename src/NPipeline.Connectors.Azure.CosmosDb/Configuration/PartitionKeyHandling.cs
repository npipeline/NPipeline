namespace NPipeline.Connectors.Azure.CosmosDb.Configuration;

/// <summary>
///     Defines how partition keys are handled for Cosmos DB write operations.
/// </summary>
public enum PartitionKeyHandling
{
    /// <summary>
    ///     Automatically extract partition key from <see cref="Mapping.CosmosPartitionKeyAttribute" /> or by convention.
    ///     Looks for properties named "PartitionKey", "partitionKey", or properties decorated with the attribute.
    /// </summary>
    Auto,

    /// <summary>
    ///     Use an explicit partition key selector function provided in the sink node constructor.
    ///     Required when partition key cannot be determined by attribute or convention.
    /// </summary>
    Explicit,

    /// <summary>
    ///     Use <see cref="Microsoft.Azure.Cosmos.PartitionKey.None" /> for non-partitioned containers.
    ///     Only for legacy containers without partitioning.
    /// </summary>
    None,
}

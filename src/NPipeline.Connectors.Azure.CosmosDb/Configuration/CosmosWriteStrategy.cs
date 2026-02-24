namespace NPipeline.Connectors.Azure.CosmosDb.Configuration;

/// <summary>
///     Defines the write strategy for Cosmos DB sink operations.
/// </summary>
public enum CosmosWriteStrategy
{
    /// <summary>
    ///     Insert documents individually. Fails on duplicate IDs with 409 Conflict.
    ///     Best for known-new data imports where duplicates should cause errors.
    /// </summary>
    Insert = 0,

    /// <summary>
    ///     Per-row insert strategy. Alias for Insert.
    ///     Writes documents one at a time using CreateItemAsync.
    /// </summary>
    PerRow = Insert,

    /// <summary>
    ///     Upsert documents individually. Creates new or replaces existing documents.
    ///     This is the default strategy and is idempotent when ID and partition key are deterministic.
    /// </summary>
    Upsert = 1,

    /// <summary>
    ///     Use parallel batch writes for improved throughput.
    ///     Documents are grouped and written concurrently. Non-transactional but performant.
    /// </summary>
    Batch = 2,

    /// <summary>
    ///     Use transactional batch for atomic operations within the same partition.
    ///     All operations succeed or fail together. Best for related data requiring atomicity.
    ///     Note: Maximum 100 operations per batch.
    /// </summary>
    TransactionalBatch = 3,

    /// <summary>
    ///     Use bulk execution for high-throughput scenarios.
    ///     Optimized for large-scale data ingestion with concurrent operations.
    ///     Non-transactional but highly performant.
    /// </summary>
    Bulk = 4,
}

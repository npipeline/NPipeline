namespace NPipeline.Connectors.MongoDB.Configuration;

/// <summary>
///     Defines the write strategy for MongoDB write operations.
/// </summary>
public enum MongoWriteStrategy
{
    /// <summary>
    ///     Uses InsertMany for batch inserts. Fastest for new documents but fails on duplicate keys.
    /// </summary>
    InsertMany,

    /// <summary>
    ///     Uses ReplaceOne with upsert enabled. Updates existing documents or inserts new ones.
    /// </summary>
    Upsert,

    /// <summary>
    ///     Uses BulkWrite for maximum flexibility with mixed operations.
    /// </summary>
    BulkWrite,
}

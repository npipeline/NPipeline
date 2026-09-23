using MongoDB.Bson;
using MongoDB.Driver;
using NPipeline.Connectors.MongoDB.Configuration;

namespace NPipeline.Connectors.MongoDB.Writers;

/// <summary>
///     A writer that separates mapping a batch from sending it, so the sink can map once and retry only the send. A
///     retry then re-sends the same documents, with the same <c>_id</c> values, instead of freshly mapped ones.
/// </summary>
/// <typeparam name="T">The type of objects to write.</typeparam>
internal interface IPreparedMongoWriter<in T>
{
    /// <summary>Maps the items to the write models to send. Mapping failures surface here, before any write.</summary>
    IReadOnlyList<WriteModel<BsonDocument>> Prepare(IEnumerable<T> items, MongoConfiguration configuration);

    /// <summary>Sends prepared write models. Safe to call again with the same models after a failure.</summary>
    Task SendAsync(
        IMongoCollection<BsonDocument> collection,
        IReadOnlyList<WriteModel<BsonDocument>> models,
        MongoConfiguration configuration,
        CancellationToken cancellationToken);
}

internal static class MongoWriteModels
{
    /// <summary>
    ///     Gives a document to be inserted an <c>_id</c> if it has none, so every attempt to insert it carries the same
    ///     one. Without it the driver generates a new <c>_id</c> for each freshly mapped copy, and a retry after a
    ///     partly applied insert writes the applied documents a second time.
    /// </summary>
    public static BsonDocument WithId(BsonDocument document)
    {
        if (!document.Contains("_id"))
            document.InsertAt(0, new BsonElement("_id", ObjectId.GenerateNewId()));

        return document;
    }
}

using MongoDB.Bson;
using MongoDB.Driver;
using NPipeline.Connectors.MongoDB.Configuration;

namespace NPipeline.Connectors.MongoDB.Writers;

/// <summary>
///     Interface for MongoDB write strategies.
/// </summary>
/// <typeparam name="T">The type of objects to write.</typeparam>
public interface IMongoWriter<T>
{
    /// <summary>
    ///     Writes a batch of documents to MongoDB.
    /// </summary>
    /// <param name="collection">The MongoDB collection to write to.</param>
    /// <param name="items">The items to write.</param>
    /// <param name="configuration">The MongoDB configuration.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task WriteBatchAsync(
        IMongoCollection<BsonDocument> collection,
        IEnumerable<T> items,
        MongoConfiguration configuration,
        CancellationToken cancellationToken = default);
}

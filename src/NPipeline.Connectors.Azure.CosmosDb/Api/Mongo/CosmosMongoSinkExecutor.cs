using MongoDB.Bson;
using MongoDB.Driver;
using NPipeline.Connectors.Azure.CosmosDb.Abstractions;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;

namespace NPipeline.Connectors.Azure.CosmosDb.Api.Mongo;

/// <summary>
///     Mongo sink executor for Cosmos Mongo API.
/// </summary>
internal sealed class CosmosMongoSinkExecutor<T> : ICosmosSinkExecutor<T>
{
    private readonly IMongoCollection<BsonDocument> _collection;
    private readonly Func<T, string>? _idSelector;

    /// <summary>
    ///     Initializes a new instance of <see cref="CosmosMongoSinkExecutor{T}" />.
    /// </summary>
    public CosmosMongoSinkExecutor(MongoClient client, CosmosConfiguration configuration, Func<T, string>? idSelector)
    {
        if (string.IsNullOrWhiteSpace(configuration.DatabaseId))
            throw new InvalidOperationException("DatabaseId is required for Mongo sink execution.");

        if (string.IsNullOrWhiteSpace(configuration.ContainerId))
            throw new InvalidOperationException("ContainerId is required for Mongo sink execution.");

        var database = client.GetDatabase(configuration.DatabaseId);
        _collection = database.GetCollection<BsonDocument>(configuration.ContainerId);
        _idSelector = idSelector;
    }

    /// <inheritdoc />
    public async Task WriteAsync(IEnumerable<T> items, CosmosWriteStrategy strategy, CancellationToken cancellationToken = default)
    {
        var documents = items.Select(ToDocument).ToList();

        if (documents.Count == 0)
            return;

        switch (strategy)
        {
            case CosmosWriteStrategy.PerRow:
                await _collection.InsertManyAsync(documents, cancellationToken: cancellationToken);
                break;

            case CosmosWriteStrategy.Upsert:
            {
                var models = documents.Select(d =>
                {
                    var id = d.GetValue("_id", BsonNull.Value);
                    var filter = Builders<BsonDocument>.Filter.Eq("_id", id);
                    return new ReplaceOneModel<BsonDocument>(filter, d) { IsUpsert = true };
                });

                await _collection.BulkWriteAsync(models, new BulkWriteOptions { IsOrdered = false }, cancellationToken);
                break;
            }

            case CosmosWriteStrategy.Batch:
            case CosmosWriteStrategy.Bulk:
            {
                var models = documents.Select(d => new ReplaceOneModel<BsonDocument>(
                    Builders<BsonDocument>.Filter.Eq("_id", d.GetValue("_id", BsonNull.Value)),
                    d)
                {
                    IsUpsert = true,
                });

                await _collection.BulkWriteAsync(models, new BulkWriteOptions { IsOrdered = false }, cancellationToken);
                break;
            }

            case CosmosWriteStrategy.TransactionalBatch:
                throw new NotSupportedException("TransactionalBatch is not supported for Mongo adapter in this connector.");

            default:
                throw new NotSupportedException($"Write strategy '{strategy}' is not supported.");
        }
    }

    private BsonDocument ToDocument(T item)
    {
        var document = item switch
        {
            BsonDocument bson => bson,
            _ => item == null
                ? new BsonDocument()
                : item.ToBsonDocument(),
        };

        if (_idSelector != null)
        {
            var id = _idSelector(item);
            document["_id"] = id;
        }
        else if (document.Contains("id") && !document.Contains("_id"))
            document["_id"] = document["id"];

        return document;
    }
}

using MongoDB.Bson;
using MongoDB.Driver;
using NPipeline.Connectors.Azure.CosmosDb.Abstractions;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;

namespace NPipeline.Connectors.Azure.CosmosDb.Api.Mongo;

/// <summary>
///     Mongo source executor for Cosmos Mongo API.
/// </summary>
internal sealed class CosmosMongoSourceExecutor : ICosmosSourceExecutor
{
    private readonly IMongoCollection<BsonDocument> _collection;

    /// <summary>
    ///     Initializes a new instance of <see cref="CosmosMongoSourceExecutor" />.
    /// </summary>
    public CosmosMongoSourceExecutor(MongoClient client, CosmosConfiguration configuration)
    {
        if (string.IsNullOrWhiteSpace(configuration.DatabaseId))
            throw new InvalidOperationException("DatabaseId is required for Mongo source execution.");

        if (string.IsNullOrWhiteSpace(configuration.ContainerId))
            throw new InvalidOperationException("ContainerId is required for Mongo source execution.");

        var database = client.GetDatabase(configuration.DatabaseId);
        _collection = database.GetCollection<BsonDocument>(configuration.ContainerId);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<IDictionary<string, object?>>> QueryAsync(
        string query,
        CancellationToken cancellationToken = default)
    {
        var filterDocument = string.IsNullOrWhiteSpace(query)
            ? new BsonDocument()
            : BsonDocument.Parse(query);

        var filter = new BsonDocumentFilterDefinition<BsonDocument>(filterDocument);
        var documents = await _collection.Find(filter).ToListAsync(cancellationToken);

        var result = new List<IDictionary<string, object?>>(documents.Count);

        foreach (var document in documents)
        {
            var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

            foreach (var element in document.Elements)
            {
                row[element.Name] = BsonTypeMapper.MapToDotNetValue(element.Value);
            }

            result.Add(row);
        }

        return result;
    }
}

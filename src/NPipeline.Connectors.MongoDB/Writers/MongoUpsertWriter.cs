using MongoDB.Bson;
using MongoDB.Driver;
using NPipeline.Connectors.MongoDB.Configuration;
using NPipeline.Connectors.MongoDB.Mapping;
using OurMongoWriteException = NPipeline.Connectors.MongoDB.Exceptions.MongoWriteException;

namespace NPipeline.Connectors.MongoDB.Writers;

/// <summary>
///     Writer that uses BulkWrite with ReplaceOneModel for upsert operations.
///     Updates existing documents or inserts new ones based on key fields.
/// </summary>
/// <typeparam name="T">The type of objects to write.</typeparam>
public class MongoUpsertWriter<T> : IMongoWriter<T>
{
    private readonly Func<T, BsonDocument>? _documentMapper;
    private readonly Func<T, FilterDefinition<BsonDocument>>? _upsertFilterBuilder;
    private readonly string[]? _upsertKeyFields;

    /// <summary>
    ///     Initializes a new instance of the <see cref="MongoUpsertWriter{T}" /> class.
    /// </summary>
    /// <param name="upsertKeyFields">The key fields to use for upsert matching. Default is "_id".</param>
    /// <param name="documentMapper">Optional custom document mapper function.</param>
    /// <param name="upsertFilterBuilder">Optional custom filter builder for upsert operations.</param>
    public MongoUpsertWriter(
        string[]? upsertKeyFields = null,
        Func<T, BsonDocument>? documentMapper = null,
        Func<T, FilterDefinition<BsonDocument>>? upsertFilterBuilder = null)
    {
        _upsertKeyFields = upsertKeyFields;
        _documentMapper = documentMapper;
        _upsertFilterBuilder = upsertFilterBuilder;
    }

    /// <summary>
    ///     Writes a batch of documents to MongoDB using upsert operations.
    /// </summary>
    /// <param name="collection">The MongoDB collection to write to.</param>
    /// <param name="items">The items to write.</param>
    /// <param name="configuration">The MongoDB configuration.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task WriteBatchAsync(
        IMongoCollection<BsonDocument> collection,
        IEnumerable<T> items,
        MongoConfiguration configuration,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(collection);
        ArgumentNullException.ThrowIfNull(configuration);

        var itemList = items.ToList();

        if (itemList.Count == 0)
            return;

        var mapper = _documentMapper ?? MongoWriteDocumentMapper.GetOrCreateMapper<T>();
        var keyFields = _upsertKeyFields ?? configuration.UpsertKeyFields;

        var writeModels = new List<WriteModel<BsonDocument>>(itemList.Count);

        foreach (var item in itemList)
        {
            try
            {
                var document = mapper(item);

                // Use custom filter builder if provided, otherwise build filter from the already-mapped document
                var filter = _upsertFilterBuilder != null
                    ? _upsertFilterBuilder(item)
                    : BuildFilterFromDocument(keyFields, document);

                var replaceOne = new ReplaceOneModel<BsonDocument>(filter, document)
                {
                    IsUpsert = true,
                };

                writeModels.Add(replaceOne);
            }
            catch (Exception ex)
            {
                if (configuration.ContinueOnError)
                {
                    configuration.DocumentErrorHandler?.Invoke(ex, null);
                    continue;
                }

                throw new OurMongoWriteException(
                    $"Failed to prepare upsert document: {ex.Message}",
                    configuration.CollectionName,
                    itemList.Count,
                    ex);
            }
        }

        if (writeModels.Count == 0)
            return;

        var options = new BulkWriteOptions
        {
            IsOrdered = configuration.OrderedWrites,
        };

        try
        {
            await collection.BulkWriteAsync(writeModels, options, cancellationToken);
        }
        catch (MongoBulkWriteException<BsonDocument> ex)
        {
            HandleBulkWriteException(ex, configuration, writeModels.Count);
        }
    }

    /// <summary>
    ///     Builds a filter from an already-mapped BSON document based on key fields.
    /// </summary>
    /// <param name="keyFields">The key fields to match on.</param>
    /// <param name="document">The already-mapped BSON document.</param>
    /// <returns>A filter definition for upsert matching.</returns>
    private static FilterDefinition<BsonDocument> BuildFilterFromDocument(
        string[] keyFields,
        BsonDocument document)
    {
        var filters = new List<FilterDefinition<BsonDocument>>();

        foreach (var keyField in keyFields)
        {
            if (document.TryGetValue(keyField, out var value))
                filters.Add(Builders<BsonDocument>.Filter.Eq(keyField, value));
        }

        return filters.Count > 0
            ? Builders<BsonDocument>.Filter.And(filters)
            : Builders<BsonDocument>.Filter.Empty;
    }

    private static void HandleBulkWriteException(
        MongoBulkWriteException<BsonDocument> exception,
        MongoConfiguration configuration,
        int batchCount)
    {
        var upsertsCount = exception.Result?.Upserts?.Count ?? 0;
        var modifiedCount = exception.Result?.ModifiedCount ?? 0;

        throw new OurMongoWriteException(
            $"Bulk write error during upsert to {configuration.CollectionName}: {exception.Message}",
            configuration.CollectionName,
            batchCount,
            exception)
        {
            SuccessfullyWrittenCount = (int)(upsertsCount + modifiedCount),
            WriteErrorCode = exception.WriteErrors?.Count > 0
                ? exception.WriteErrors[0].Code
                : null,
        };
    }
}

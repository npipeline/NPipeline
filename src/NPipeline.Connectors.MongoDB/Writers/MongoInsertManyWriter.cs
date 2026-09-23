using MongoDB.Bson;
using MongoDB.Driver;
using NPipeline.Connectors.MongoDB.Configuration;
using NPipeline.Connectors.MongoDB.Mapping;
using OurMongoWriteException = NPipeline.Connectors.MongoDB.Exceptions.MongoWriteException;

namespace NPipeline.Connectors.MongoDB.Writers;

/// <summary>
///     Writer that uses InsertMany for batch inserts.
///     Fastest for new documents but fails on duplicate keys unless configured to ignore.
/// </summary>
/// <typeparam name="T">The type of objects to write.</typeparam>
public class MongoInsertManyWriter<T> : IMongoWriter<T>, IPreparedMongoWriter<T>
{
    private readonly Func<T, BsonDocument>? _documentMapper;

    /// <summary>
    ///     Initializes a new instance of the <see cref="MongoInsertManyWriter{T}" /> class.
    /// </summary>
    /// <param name="documentMapper">Optional custom document mapper function.</param>
    public MongoInsertManyWriter(Func<T, BsonDocument>? documentMapper = null)
    {
        _documentMapper = documentMapper;
    }

    /// <summary>
    ///     Writes a batch of documents to MongoDB using InsertMany.
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

        var models = Prepare(items, configuration);

        if (models.Count > 0)
            await SendAsync(collection, models, configuration, cancellationToken).ConfigureAwait(false);
    }

    IReadOnlyList<WriteModel<BsonDocument>> IPreparedMongoWriter<T>.Prepare(IEnumerable<T> items, MongoConfiguration configuration)
    {
        return Prepare(items, configuration);
    }

    Task IPreparedMongoWriter<T>.SendAsync(
        IMongoCollection<BsonDocument> collection,
        IReadOnlyList<WriteModel<BsonDocument>> models,
        MongoConfiguration configuration,
        CancellationToken cancellationToken)
    {
        return SendAsync(collection, models, configuration, cancellationToken);
    }

    private List<WriteModel<BsonDocument>> Prepare(IEnumerable<T> items, MongoConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var itemList = items.ToList();
        var mapper = _documentMapper ?? MongoWriteDocumentMapper.GetOrCreateMapper<T>();
        var models = new List<WriteModel<BsonDocument>>(itemList.Count);

        foreach (var item in itemList)
        {
            try
            {
                models.Add(new InsertOneModel<BsonDocument>(MongoWriteModels.WithId(mapper(item))));
            }
            catch (Exception ex)
            {
                if (configuration.ContinueOnError)
                {
                    configuration.DocumentErrorHandler?.Invoke(ex, null);
                    continue;
                }

                throw new OurMongoWriteException(
                    $"Failed to map document for insert: {ex.Message}",
                    configuration.CollectionName,
                    itemList.Count,
                    ex);
            }
        }

        return models;
    }

    private static async Task SendAsync(
        IMongoCollection<BsonDocument> collection,
        IReadOnlyList<WriteModel<BsonDocument>> models,
        MongoConfiguration configuration,
        CancellationToken cancellationToken)
    {
        var documents = models.Select(static m => ((InsertOneModel<BsonDocument>)m).Document).ToList();

        var options = new InsertManyOptions
        {
            IsOrdered = configuration.OrderedWrites,
        };

        try
        {
            await collection.InsertManyAsync(documents, options, cancellationToken).ConfigureAwait(false);
        }
        catch (MongoBulkWriteException<BsonDocument> ex)
        {
            HandleBulkWriteException(ex, configuration, documents.Count);
        }
    }

    private static void HandleBulkWriteException(
        MongoBulkWriteException<BsonDocument> exception,
        MongoConfiguration configuration,
        int batchCount)
    {
        // Check for duplicate key errors
        var hasDuplicateKeyError = exception.WriteErrors?.Any(e => e.Category == ServerErrorCategory.DuplicateKey) == true;

        if (hasDuplicateKeyError)
        {
            switch (configuration.OnDuplicate)
            {
                case OnDuplicateAction.Ignore:
                    // Swallow the exception - duplicates are ignored
                    return;

                case OnDuplicateAction.Fail:
                default:
                    throw new OurMongoWriteException(
                        $"Duplicate key error during insert to {configuration.CollectionName}",
                        configuration.CollectionName,
                        batchCount,
                        exception)
                    {
                        WriteErrorCode = exception.WriteErrors?.Count > 0
                            ? exception.WriteErrors[0].Code
                            : null,
                        SuccessfullyWrittenCount = (int)(exception.Result?.InsertedCount ?? 0),
                    };
            }
        }

        // Re-throw for non-duplicate errors
        throw new OurMongoWriteException(
            $"Bulk write error during insert to {configuration.CollectionName}: {exception.Message}",
            configuration.CollectionName,
            batchCount,
            exception)
        {
            SuccessfullyWrittenCount = (int)(exception.Result?.InsertedCount ?? 0),
        };
    }
}

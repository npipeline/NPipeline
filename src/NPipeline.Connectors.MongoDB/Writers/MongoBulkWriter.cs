using MongoDB.Bson;
using MongoDB.Driver;
using NPipeline.Connectors.MongoDB.Configuration;
using NPipeline.Connectors.MongoDB.Mapping;
using OurMongoWriteException = NPipeline.Connectors.MongoDB.Exceptions.MongoWriteException;

namespace NPipeline.Connectors.MongoDB.Writers;

/// <summary>
///     Maximally flexible writer that supports custom write models.
///     Default behavior uses InsertOneModel with IsOrdered = false.
///     Can produce any mix of InsertOneModel, ReplaceOneModel, UpdateOneModel, DeleteOneModel.
/// </summary>
/// <typeparam name="T">The type of objects to write.</typeparam>
public class MongoBulkWriter<T> : IMongoWriter<T>
{
    private readonly Func<T, BsonDocument>? _documentMapper;
    private readonly Func<T, WriteModel<BsonDocument>>? _writeModelBuilder;

    /// <summary>
    ///     Initializes a new instance of the <see cref="MongoBulkWriter{T}" /> class with default insert behavior.
    /// </summary>
    /// <param name="documentMapper">Optional custom document mapper function.</param>
    public MongoBulkWriter(Func<T, BsonDocument>? documentMapper = null)
    {
        _documentMapper = documentMapper;
        _writeModelBuilder = null;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="MongoBulkWriter{T}" /> class with custom write model builder.
    /// </summary>
    /// <param name="writeModelBuilder">
    ///     Custom function to build write models for each item.
    ///     This allows mixing InsertOneModel, ReplaceOneModel, UpdateOneModel, and DeleteOneModel.
    /// </param>
    /// <param name="documentMapper">Optional custom document mapper function.</param>
    public MongoBulkWriter(
        Func<T, WriteModel<BsonDocument>> writeModelBuilder,
        Func<T, BsonDocument>? documentMapper = null)
    {
        _writeModelBuilder = writeModelBuilder ?? throw new ArgumentNullException(nameof(writeModelBuilder));
        _documentMapper = documentMapper;
    }

    /// <summary>
    ///     Writes a batch of documents to MongoDB using BulkWrite.
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
        var writeModels = new List<WriteModel<BsonDocument>>(itemList.Count);

        foreach (var item in itemList)
        {
            try
            {
                WriteModel<BsonDocument> writeModel;

                if (_writeModelBuilder != null)
                {
                    // Use custom write model builder
                    writeModel = _writeModelBuilder(item);
                }
                else
                {
                    // Default: InsertOneModel per document
                    var document = mapper(item);
                    writeModel = new InsertOneModel<BsonDocument>(document);
                }

                writeModels.Add(writeModel);
            }
            catch (Exception ex)
            {
                if (configuration.ContinueOnError)
                {
                    configuration.DocumentErrorHandler?.Invoke(ex, null);
                    continue;
                }

                throw new OurMongoWriteException(
                    $"Failed to prepare bulk write model: {ex.Message}",
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

    private static void HandleBulkWriteException(
        MongoBulkWriteException<BsonDocument> exception,
        MongoConfiguration configuration,
        int batchCount)
    {
        // Check for duplicate key errors
        var hasDuplicateKeyError = exception.WriteErrors?.Any(e => e.Category == ServerErrorCategory.DuplicateKey) == true;

        if (hasDuplicateKeyError && configuration.OnDuplicate == OnDuplicateAction.Ignore)
        {
            // Swallow the exception - duplicates are ignored
            return;
        }

        var result = exception.Result;
        var upsertsCount = result?.Upserts?.Count ?? 0;

        var successCount = (result?.InsertedCount ?? 0) +
                           (result?.ModifiedCount ?? 0) +
                           upsertsCount +
                           (result?.DeletedCount ?? 0);

        throw new OurMongoWriteException(
            $"Bulk write error to {configuration.CollectionName}: {exception.Message}",
            configuration.CollectionName,
            batchCount,
            exception)
        {
            SuccessfullyWrittenCount = (int)successCount,
            WriteErrorCode = exception.WriteErrors?.Count > 0
                ? exception.WriteErrors[0].Code
                : null,
        };
    }
}

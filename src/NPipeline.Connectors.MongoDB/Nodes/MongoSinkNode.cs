using MongoDB.Bson;
using MongoDB.Driver;
using NPipeline.Connectors.MongoDB.Configuration;
using NPipeline.Connectors.MongoDB.Writers;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;
using OurMongoWriteException = NPipeline.Connectors.MongoDB.Exceptions.MongoWriteException;

namespace NPipeline.Connectors.MongoDB.Nodes;

/// <summary>
///     A MongoDB sink node that writes documents to a MongoDB collection.
/// </summary>
/// <typeparam name="T">The type of objects consumed by this sink.</typeparam>
public class MongoSinkNode<T> : SinkNode<T>, IAsyncDisposable
    where T : class
{
    private readonly IMongoClient? _client;
    private readonly MongoConfiguration _configuration;
    private readonly string? _connectionString;
    private readonly Func<T, BsonDocument>? _documentMapper;
    private readonly IStorageProvider? _storageProvider;
    private readonly StorageUri? _storageUri;
    private readonly Func<T, FilterDefinition<BsonDocument>>? _upsertFilterBuilder;

    private IMongoClient? _ownedClient;

    /// <summary>
    ///     Initializes a new instance of the <see cref="MongoSinkNode{T}" /> class using a connection string.
    /// </summary>
    /// <param name="connectionString">The MongoDB connection string.</param>
    /// <param name="configuration">The MongoDB configuration.</param>
    /// <param name="documentMapper">Optional custom document mapper function.</param>
    /// <param name="upsertFilterBuilder">Optional custom filter builder for upsert operations.</param>
    public MongoSinkNode(
        string connectionString,
        MongoConfiguration configuration,
        Func<T, BsonDocument>? documentMapper = null,
        Func<T, FilterDefinition<BsonDocument>>? upsertFilterBuilder = null)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.Validate();
        _documentMapper = documentMapper;
        _upsertFilterBuilder = upsertFilterBuilder;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="MongoSinkNode{T}" /> class using an existing MongoDB client.
    /// </summary>
    /// <param name="client">The MongoDB client.</param>
    /// <param name="configuration">The MongoDB configuration.</param>
    /// <param name="documentMapper">Optional custom document mapper function.</param>
    /// <param name="upsertFilterBuilder">Optional custom filter builder for upsert operations.</param>
    public MongoSinkNode(
        IMongoClient client,
        MongoConfiguration configuration,
        Func<T, BsonDocument>? documentMapper = null,
        Func<T, FilterDefinition<BsonDocument>>? upsertFilterBuilder = null)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.Validate();
        _documentMapper = documentMapper;
        _upsertFilterBuilder = upsertFilterBuilder;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="MongoSinkNode{T}" /> class using a storage URI.
    /// </summary>
    /// <param name="uri">The storage URI.</param>
    /// <param name="configuration">The MongoDB configuration.</param>
    /// <param name="documentMapper">Optional custom document mapper function.</param>
    /// <param name="upsertFilterBuilder">Optional custom filter builder for upsert operations.</param>
    public MongoSinkNode(
        StorageUri uri,
        MongoConfiguration configuration,
        Func<T, BsonDocument>? documentMapper = null,
        Func<T, FilterDefinition<BsonDocument>>? upsertFilterBuilder = null)
    {
        _storageUri = uri ?? throw new ArgumentNullException(nameof(uri));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _storageProvider = new MongoDatabaseStorageProvider();
        ApplyStorageUriDefaults(_storageUri, _configuration);
        _configuration.Validate();
        _documentMapper = documentMapper;
        _upsertFilterBuilder = upsertFilterBuilder;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="MongoSinkNode{T}" /> class using a storage provider.
    /// </summary>
    /// <param name="storageProvider">The storage provider.</param>
    /// <param name="uri">The storage URI.</param>
    /// <param name="configuration">The MongoDB configuration.</param>
    /// <param name="documentMapper">Optional custom document mapper function.</param>
    /// <param name="upsertFilterBuilder">Optional custom filter builder for upsert operations.</param>
    public MongoSinkNode(
        IStorageProvider storageProvider,
        StorageUri uri,
        MongoConfiguration configuration,
        Func<T, BsonDocument>? documentMapper = null,
        Func<T, FilterDefinition<BsonDocument>>? upsertFilterBuilder = null)
    {
        _storageProvider = storageProvider ?? throw new ArgumentNullException(nameof(storageProvider));
        _storageUri = uri ?? throw new ArgumentNullException(nameof(uri));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        ApplyStorageUriDefaults(_storageUri, _configuration);
        _configuration.Validate();
        _documentMapper = documentMapper;
        _upsertFilterBuilder = upsertFilterBuilder;
    }

    /// <summary>
    ///     Gets the batch size for writes.
    /// </summary>
    protected virtual int WriteBatchSize => _configuration.WriteBatchSize;

    /// <summary>
    ///     Gets the policy that retries each batch write.
    /// </summary>
    protected virtual NResilience.Resilience Resilience => _configuration.Resilience;

    /// <summary>
    ///     Disposes resources used by the sink node.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_ownedClient != null)
        {
            _ownedClient.Dispose();
            _ownedClient = null;
        }

        GC.SuppressFinalize(this);
    }

    /// <summary>
    ///     Executes the sink node, writing all input data to MongoDB.
    /// </summary>
    /// <param name="input">The input data pipe.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public override async Task ConsumeAsync(
        IDataStream<T> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var client = GetClient();
        var database = client.GetDatabase(_configuration.DatabaseName);
        var collection = database.GetCollection<BsonDocument>(_configuration.CollectionName);

        var writer = MongoWriterFactory.CreateFromConfiguration(
            _configuration,
            _documentMapper,
            _upsertFilterBuilder);

        var batch = new List<T>(WriteBatchSize);

        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            batch.Add(item);

            if (batch.Count >= WriteBatchSize)
            {
                await WriteBatchWithRetryAsync(writer, collection, batch, cancellationToken).ConfigureAwait(false);
                batch.Clear();
            }
        }

        // Write remaining items
        if (batch.Count > 0)
            await WriteBatchWithRetryAsync(writer, collection, batch, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Gets the MongoDB client.
    /// </summary>
    /// <returns>The MongoDB client.</returns>
    protected virtual IMongoClient GetClient()
    {
        if (_client != null)
            return _client;

        if (!string.IsNullOrEmpty(_connectionString))
        {
            _ownedClient = new MongoClient(_connectionString);
            return _ownedClient;
        }

        if (_storageProvider != null && _storageUri != null)
        {
            if (_storageProvider is IDatabaseStorageProvider dbProvider)
            {
                var connectionString = dbProvider.GetConnectionString(_storageUri);
                _ownedClient = new MongoClient(connectionString);
                return _ownedClient;
            }

            throw new InvalidOperationException(
                $"Storage provider must implement {nameof(IDatabaseStorageProvider)} to use StorageUri.");
        }

        throw new InvalidOperationException("No MongoDB client or connection string provided.");
    }

    private static void ApplyStorageUriDefaults(StorageUri uri, MongoConfiguration configuration)
    {
        if (string.IsNullOrWhiteSpace(configuration.DatabaseName))
            configuration.DatabaseName = ExtractDatabaseName(uri.Path) ?? configuration.DatabaseName;

        if (string.IsNullOrWhiteSpace(configuration.CollectionName))
        {
            if (uri.Parameters.TryGetValue("collection", out var collection) && !string.IsNullOrWhiteSpace(collection))
                configuration.CollectionName = collection;
            else if (uri.Parameters.TryGetValue("table", out var table) && !string.IsNullOrWhiteSpace(table))
                configuration.CollectionName = table;
            else
                configuration.CollectionName = ExtractCollectionName(uri.Path) ?? configuration.CollectionName;
        }
    }

    private static string? ExtractDatabaseName(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return null;

        var segments = path
            .Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        return segments.Length > 0
            ? segments[0]
            : null;
    }

    private static string? ExtractCollectionName(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return null;

        var segments = path
            .Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        return segments.Length > 1
            ? segments[1]
            : null;
    }

    /// <summary>
    ///     Writes a batch, retrying transient failures according to <see cref="Resilience" />.
    /// </summary>
    /// <remarks>
    ///     The batch is mapped once, before the first attempt, and each inserted document gets its <c>_id</c> then. Every
    ///     attempt sends the same documents, so a retry after a partly applied write finds the documents it already wrote
    ///     (a duplicate key) instead of writing them again under new ids. A bulk write that reported write errors is not
    ///     retried at all.
    /// </remarks>
    /// <param name="writer">The writer to use.</param>
    /// <param name="collection">The collection to write to.</param>
    /// <param name="batch">The batch to write.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    private async Task WriteBatchWithRetryAsync(
        IMongoWriter<T> writer,
        IMongoCollection<BsonDocument> collection,
        List<T> batch,
        CancellationToken cancellationToken)
    {
        if (writer is not IPreparedMongoWriter<T> prepared)
        {
            // A writer that maps as it sends can't guarantee a retry re-sends the same documents, so it is not retried.
            await writer.WriteBatchAsync(collection, batch, _configuration, cancellationToken).ConfigureAwait(false);
            return;
        }

        var models = prepared.Prepare(batch, _configuration);

        if (models.Count == 0)
            return;

        try
        {
            await Resilience.RunAsync(
                token => prepared.SendAsync(collection, models, _configuration, token),
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OurMongoWriteException and not OperationCanceledException)
        {
            throw new OurMongoWriteException(
                $"Failed to write batch: {ex.Message}",
                _configuration.CollectionName,
                batch.Count,
                ex);
        }
    }
}

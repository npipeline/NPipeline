using System.Runtime.CompilerServices;
using MongoDB.Bson;
using MongoDB.Driver;
using NPipeline.Connectors.Checkpointing;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.MongoDB.Configuration;
using NPipeline.Connectors.MongoDB.Exceptions;
using NPipeline.Connectors.MongoDB.Mapping;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.MongoDB.Nodes;

/// <summary>
///     A MongoDB source node that reads documents from a MongoDB collection and emits them as typed objects.
/// </summary>
/// <typeparam name="T">The type of objects emitted by this source.</typeparam>
public class MongoSourceNode<T> : SourceNode<T>, IAsyncDisposable
    where T : class, new()
{
    private readonly IMongoClient? _client;
    private readonly MongoConfiguration _configuration;
    private readonly string? _connectionString;
    private readonly Func<MongoRow, T>? _customMapper;
    private readonly FilterDefinition<BsonDocument>? _filter;
    private readonly ProjectionDefinition<BsonDocument>? _projection;
    private readonly SortDefinition<BsonDocument>? _sort;
    private readonly IStorageProvider? _storageProvider;
    private readonly StorageUri? _storageUri;
    private IAsyncCursor<BsonDocument>? _cursor;

    private IMongoClient? _ownedClient;

    /// <summary>
    ///     Initializes a new instance of the <see cref="MongoSourceNode{T}" /> class using a connection string.
    /// </summary>
    /// <param name="connectionString">The MongoDB connection string.</param>
    /// <param name="configuration">The MongoDB configuration.</param>
    /// <param name="filter">Optional filter definition.</param>
    /// <param name="sort">Optional sort definition.</param>
    /// <param name="projection">Optional projection definition.</param>
    /// <param name="customMapper">Optional custom mapper function.</param>
    public MongoSourceNode(
        string connectionString,
        MongoConfiguration configuration,
        FilterDefinition<BsonDocument>? filter = null,
        SortDefinition<BsonDocument>? sort = null,
        ProjectionDefinition<BsonDocument>? projection = null,
        Func<MongoRow, T>? customMapper = null)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _filter = filter;
        _sort = sort;
        _projection = projection;
        _customMapper = customMapper;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="MongoSourceNode{T}" /> class using an existing MongoDB client.
    /// </summary>
    /// <param name="client">The MongoDB client.</param>
    /// <param name="configuration">The MongoDB configuration.</param>
    /// <param name="filter">Optional filter definition.</param>
    /// <param name="sort">Optional sort definition.</param>
    /// <param name="projection">Optional projection definition.</param>
    /// <param name="customMapper">Optional custom mapper function.</param>
    public MongoSourceNode(
        IMongoClient client,
        MongoConfiguration configuration,
        FilterDefinition<BsonDocument>? filter = null,
        SortDefinition<BsonDocument>? sort = null,
        ProjectionDefinition<BsonDocument>? projection = null,
        Func<MongoRow, T>? customMapper = null)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _filter = filter;
        _sort = sort;
        _projection = projection;
        _customMapper = customMapper;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="MongoSourceNode{T}" /> class using a storage URI.
    /// </summary>
    /// <param name="uri">The storage URI.</param>
    /// <param name="configuration">The MongoDB configuration.</param>
    /// <param name="filter">Optional filter definition.</param>
    /// <param name="sort">Optional sort definition.</param>
    /// <param name="projection">Optional projection definition.</param>
    /// <param name="customMapper">Optional custom mapper function.</param>
    public MongoSourceNode(
        StorageUri uri,
        MongoConfiguration configuration,
        FilterDefinition<BsonDocument>? filter = null,
        SortDefinition<BsonDocument>? sort = null,
        ProjectionDefinition<BsonDocument>? projection = null,
        Func<MongoRow, T>? customMapper = null)
    {
        _storageUri = uri ?? throw new ArgumentNullException(nameof(uri));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _storageProvider = new MongoDatabaseStorageProvider();
        _filter = filter;
        _sort = sort;
        _projection = projection;
        _customMapper = customMapper;

        ApplyStorageUriDefaults(_storageUri, _configuration);
        _configuration.Validate();
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="MongoSourceNode{T}" /> class using a storage provider.
    /// </summary>
    /// <param name="storageProvider">The storage provider.</param>
    /// <param name="uri">The storage URI.</param>
    /// <param name="configuration">The MongoDB configuration.</param>
    /// <param name="filter">Optional filter definition.</param>
    /// <param name="sort">Optional sort definition.</param>
    /// <param name="projection">Optional projection definition.</param>
    /// <param name="customMapper">Optional custom mapper function.</param>
    public MongoSourceNode(
        IStorageProvider storageProvider,
        StorageUri uri,
        MongoConfiguration configuration,
        FilterDefinition<BsonDocument>? filter = null,
        SortDefinition<BsonDocument>? sort = null,
        ProjectionDefinition<BsonDocument>? projection = null,
        Func<MongoRow, T>? customMapper = null)
    {
        _storageProvider = storageProvider ?? throw new ArgumentNullException(nameof(storageProvider));
        _storageUri = uri ?? throw new ArgumentNullException(nameof(uri));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _filter = filter;
        _sort = sort;
        _projection = projection;
        _customMapper = customMapper;

        ApplyStorageUriDefaults(_storageUri, _configuration);
        _configuration.Validate();
    }

    /// <summary>
    ///     Gets the batch size for reading.
    /// </summary>
    protected virtual int BatchSize => _configuration.BatchSize;

    /// <summary>
    ///     Gets the delivery semantic.
    /// </summary>
    protected virtual DeliverySemantic DeliverySemantic => _configuration.DeliverySemantic;

    /// <summary>
    ///     Gets the checkpoint strategy.
    /// </summary>
    protected virtual CheckpointStrategy CheckpointStrategy => _configuration.CheckpointStrategy;

    /// <summary>
    ///     Gets a unique identifier for this source node instance for checkpoint tracking.
    /// </summary>
    protected virtual string CheckpointId => $"{GetType().FullName}_{_configuration.CollectionName}";

    /// <summary>
    ///     Gets the pipeline identifier for checkpoint namespacing.
    /// </summary>
    protected virtual string PipelineId => "default";

    /// <summary>
    ///     Gets the checkpoint storage backend.
    /// </summary>
    protected virtual ICheckpointStorage? CheckpointStorage => _configuration.CheckpointStorage;

    /// <summary>
    ///     Gets the checkpoint interval configuration.
    /// </summary>
    protected virtual CheckpointIntervalConfiguration CheckpointInterval => _configuration.CheckpointInterval;

    /// <summary>
    ///     Disposes resources used by the source node.
    /// </summary>
    public override async ValueTask DisposeAsync()
    {
        if (_cursor != null)
            _cursor.Dispose();

        if (_ownedClient != null)
            _ownedClient.Dispose();

        await base.DisposeAsync();
        GC.SuppressFinalize(this);
    }

    /// <summary>
    ///     Initializes the source node and returns a streaming data pipe.
    ///     Documents are streamed via a server-side cursor and never buffered fully in memory.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A streaming data pipe backed by the MongoDB cursor.</returns>
    public override IDataPipe<T> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        var stream = StreamDataAsync(cancellationToken);
        return new StreamingDataPipe<T>(stream, $"{GetType().Name}");
    }

    private IMongoClient GetClient()
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
            // For storage provider-based construction, we need to resolve the connection string
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

    private async IAsyncEnumerable<T> StreamDataAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var client = GetClient();
        var database = client.GetDatabase(_configuration.DatabaseName);
        var collection = database.GetCollection<BsonDocument>(_configuration.CollectionName);

        var options = new FindOptions<BsonDocument>
        {
            BatchSize = BatchSize,
            NoCursorTimeout = _configuration.NoCursorTimeout,
        };

        if (_projection != null)
            options.Projection = _projection;

        if (_sort != null)
            options.Sort = _sort;

        _cursor = await collection.FindAsync(_filter ?? Builders<BsonDocument>.Filter.Empty, options, cancellationToken);

        var mapper = _customMapper ?? MongoMapperBuilder.GetOrCreateMapper<T>();

        await foreach (var document in _cursor.ToAsyncEnumerable().WithCancellation(cancellationToken))
        {
            var row = new MongoRow(document);
            T? item = null;

            try
            {
                item = mapper(row);
            }
            catch (Exception ex) when (ex is not MongoMappingException)
            {
                if (_configuration.ThrowOnMappingError)
                    throw new MongoMappingException($"Failed to map document to {typeof(T).Name}", null, document, ex);

                if (_configuration.DocumentErrorHandler?.Invoke(ex, document) != true)
                    continue;
            }

            if (item != null)
                yield return item;
        }
    }
}

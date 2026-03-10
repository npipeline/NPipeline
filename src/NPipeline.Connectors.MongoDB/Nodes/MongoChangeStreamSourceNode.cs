using System.Runtime.CompilerServices;
using MongoDB.Bson;
using MongoDB.Driver;
using NPipeline.Connectors.MongoDB.ChangeStream;
using NPipeline.Connectors.MongoDB.Exceptions;
using NPipeline.Connectors.MongoDB.Mapping;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.MongoDB.Nodes;

/// <summary>
///     A MongoDB change stream source node that watches for changes in a MongoDB collection
///     and emits change stream events as typed objects.
/// </summary>
/// <typeparam name="T">The type of objects emitted by this source.</typeparam>
public class MongoChangeStreamSourceNode<T> : SourceNode<T>, IAsyncDisposable
    where T : class, new()
{
    private readonly IMongoClient? _client;
    private readonly MongoChangeStreamConfiguration _configuration;
    private readonly string? _connectionString;
    private readonly Func<MongoChangeStreamEvent<BsonDocument>, T>? _mapper;
    private readonly MongoChangeStreamOperationType[]? _operationTypes;
    private BsonDocument? _currentResumeToken;
    private IAsyncCursor<ChangeStreamDocument<BsonDocument>>? _cursor;

    private IMongoClient? _ownedClient;

    /// <summary>
    ///     Initializes a new instance of the <see cref="MongoChangeStreamSourceNode{T}" /> class using a connection string.
    /// </summary>
    /// <param name="connectionString">The MongoDB connection string.</param>
    /// <param name="databaseName">The database name.</param>
    /// <param name="collectionName">The collection name (optional - if null, watches entire database).</param>
    /// <param name="operationTypes">The operation types to include (optional - null includes all).</param>
    /// <param name="resumeToken">The resume token to start from (optional).</param>
    /// <param name="mapper">Optional custom mapper function.</param>
    /// <param name="configuration">Optional configuration.</param>
    public MongoChangeStreamSourceNode(
        string connectionString,
        string databaseName,
        string? collectionName = null,
        MongoChangeStreamOperationType[]? operationTypes = null,
        BsonDocument? resumeToken = null,
        Func<MongoChangeStreamEvent<BsonDocument>, T>? mapper = null,
        MongoChangeStreamConfiguration? configuration = null)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        _operationTypes = operationTypes;
        _mapper = mapper;

        _configuration = configuration ?? new MongoChangeStreamConfiguration
        {
            DatabaseName = databaseName,
            CollectionName = collectionName,
            ResumeToken = resumeToken,
        };

        if (string.IsNullOrWhiteSpace(_configuration.DatabaseName))
            _configuration.DatabaseName = databaseName;

        if (_configuration.CollectionName == null && collectionName != null)
            _configuration.CollectionName = collectionName;

        if (_configuration.ResumeToken == null && resumeToken != null)
            _configuration.ResumeToken = resumeToken;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="MongoChangeStreamSourceNode{T}" /> class using an existing MongoDB client.
    /// </summary>
    /// <param name="client">The MongoDB client.</param>
    /// <param name="databaseName">The database name.</param>
    /// <param name="collectionName">The collection name (optional - if null, watches entire database).</param>
    /// <param name="operationTypes">The operation types to include (optional - null includes all).</param>
    /// <param name="resumeToken">The resume token to start from (optional).</param>
    /// <param name="mapper">Optional custom mapper function.</param>
    /// <param name="configuration">Optional configuration.</param>
    public MongoChangeStreamSourceNode(
        IMongoClient client,
        string databaseName,
        string? collectionName = null,
        MongoChangeStreamOperationType[]? operationTypes = null,
        BsonDocument? resumeToken = null,
        Func<MongoChangeStreamEvent<BsonDocument>, T>? mapper = null,
        MongoChangeStreamConfiguration? configuration = null)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _operationTypes = operationTypes;
        _mapper = mapper;

        _configuration = configuration ?? new MongoChangeStreamConfiguration
        {
            DatabaseName = databaseName,
            CollectionName = collectionName,
            ResumeToken = resumeToken,
        };

        if (string.IsNullOrWhiteSpace(_configuration.DatabaseName))
            _configuration.DatabaseName = databaseName;

        if (_configuration.CollectionName == null && collectionName != null)
            _configuration.CollectionName = collectionName;

        if (_configuration.ResumeToken == null && resumeToken != null)
            _configuration.ResumeToken = resumeToken;
    }

    /// <summary>
    ///     Gets the current resume token that can be used to resume the change stream from the last processed event.
    ///     This property is updated after each successfully yielded event.
    /// </summary>
    public BsonDocument? ResumeToken => _currentResumeToken ?? _configuration.ResumeToken;

    /// <summary>
    ///     Disposes resources used by the source node.
    /// </summary>
    public override async ValueTask DisposeAsync()
    {
        if (_cursor != null)
        {
            _cursor.Dispose();
            _cursor = null;
        }

        if (_ownedClient != null)
        {
            _ownedClient.Dispose();
            _ownedClient = null;
        }

        await base.DisposeAsync();
        GC.SuppressFinalize(this);
    }

    /// <summary>
    ///     Initializes the source node and returns a streaming data pipe.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A streaming data pipe containing the change stream events.</returns>
    public override IDataStream<T> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        _configuration.Validate();
        var stream = StreamChangesAsync(cancellationToken);
        return new DataStream<T>(stream, $"{GetType().Name}");
    }

    private IMongoClient GetClientAsync()
    {
        if (_client != null)
            return _client;

        if (!string.IsNullOrEmpty(_connectionString))
        {
            _ownedClient = new MongoClient(_connectionString);
            return _ownedClient;
        }

        throw new InvalidOperationException("No MongoDB client or connection string provided.");
    }

    private async IAsyncEnumerable<T> StreamChangesAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var client = GetClientAsync();
        var database = client.GetDatabase(_configuration.DatabaseName);

        var options = BuildChangeStreamOptions();
        var pipeline = BuildPipeline();

        var attempt = 0;
        var maxAttempts = _configuration.MaxRetryAttempts + 1;

        while (attempt < maxAttempts && !cancellationToken.IsCancellationRequested)
        {
            attempt++;

            try
            {
                // Watch collection or entire database
                if (!string.IsNullOrWhiteSpace(_configuration.CollectionName))
                {
                    var collection = database.GetCollection<BsonDocument>(_configuration.CollectionName);
                    _cursor = await collection.WatchAsync(pipeline, options, cancellationToken);
                }
                else
                    _cursor = await database.WatchAsync(pipeline, options, cancellationToken);

                break;
            }
            catch (Exception ex) when (IsTransientError(ex) && attempt < maxAttempts)
            {
                await Task.Delay(_configuration.RetryDelay, cancellationToken);
            }
            catch (Exception ex)
            {
                if (_configuration.ContinueOnError)
                {
                    if (_configuration.DocumentErrorHandler?.Invoke(ex, null) == true)
                        yield break;
                }

                throw;
            }
        }

        if (_cursor == null)
            yield break;

        var mapper = _mapper ?? BuildDefaultMapper();

        await foreach (var change in _cursor.ToAsyncEnumerable().WithCancellation(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            T? item = null;

            try
            {
                var changeEvent = MapToChangeEvent(change);
                _currentResumeToken = changeEvent.ResumeToken;
                item = mapper(changeEvent);
            }
            catch (Exception ex) when (ex is not MongoMappingException)
            {
                if (_configuration.ThrowOnMappingError)
                {
                    throw new MongoMappingException(
                        $"Failed to map change stream document to {typeof(T).Name}",
                        null,
                        change.FullDocument,
                        ex);
                }

                if (_configuration.DocumentErrorHandler?.Invoke(ex, change.FullDocument) != true)
                    continue;
            }

            if (item != null)
                yield return item;
        }
    }

    private ChangeStreamOptions BuildChangeStreamOptions()
    {
        var options = new ChangeStreamOptions
        {
            FullDocument = _configuration.FullDocumentOption,
            MaxAwaitTime = _configuration.MaxAwaitTime,
        };

        if (_configuration.ResumeToken != null)
            options.ResumeAfter = _configuration.ResumeToken;

        if (_configuration.BatchSize.HasValue)
            options.BatchSize = _configuration.BatchSize.Value;

        if (_configuration.StartAtOperationTime != null)
            options.StartAtOperationTime = _configuration.StartAtOperationTime;

        return options;
    }

    private PipelineDefinition<ChangeStreamDocument<BsonDocument>, ChangeStreamDocument<BsonDocument>> BuildPipeline()
    {
        var operationTypes = _operationTypes ?? _configuration.OperationTypes;

        if (operationTypes == null || operationTypes.Length == 0)
            return new EmptyPipelineDefinition<ChangeStreamDocument<BsonDocument>>();

        // Build $match stage to filter by operation types
        var operationNames = operationTypes.Select(MapToDriverOperationType).ToArray();
        var matchStage = new BsonDocument("$match", new BsonDocument("operationType", new BsonDocument("$in", new BsonArray(operationNames))));

        return PipelineDefinition<ChangeStreamDocument<BsonDocument>, ChangeStreamDocument<BsonDocument>>.Create(matchStage);
    }

    private static string MapToDriverOperationType(MongoChangeStreamOperationType operationType)
    {
        return operationType switch
        {
            MongoChangeStreamOperationType.Insert => "insert",
            MongoChangeStreamOperationType.Update => "update",
            MongoChangeStreamOperationType.Replace => "replace",
            MongoChangeStreamOperationType.Delete => "delete",
            MongoChangeStreamOperationType.Invalidate => "invalidate",
            MongoChangeStreamOperationType.Drop => "drop",
            MongoChangeStreamOperationType.DropDatabase => "dropDatabase",
            MongoChangeStreamOperationType.Rename => "rename",
            _ => throw new ArgumentOutOfRangeException(nameof(operationType), operationType, $"Unknown MongoDB operation type: {operationType}"),
        };
    }

    private static MongoChangeStreamOperationType MapFromDriverOperationType(string operationType)
    {
        return operationType?.ToLowerInvariant() switch
        {
            "insert" => MongoChangeStreamOperationType.Insert,
            "update" => MongoChangeStreamOperationType.Update,
            "replace" => MongoChangeStreamOperationType.Replace,
            "delete" => MongoChangeStreamOperationType.Delete,
            "invalidate" => MongoChangeStreamOperationType.Invalidate,
            "drop" => MongoChangeStreamOperationType.Drop,
            "dropdatabase" => MongoChangeStreamOperationType.DropDatabase,
            "rename" => MongoChangeStreamOperationType.Rename,
            _ => throw new ArgumentOutOfRangeException(nameof(operationType), $"Unknown MongoDB operation type: {operationType}"),
        };
    }

    private static MongoChangeStreamEvent<BsonDocument> MapToChangeEvent(ChangeStreamDocument<BsonDocument> change)
    {
        var operationType = MapFromDriverOperationType(change.OperationType.ToString());
        var fullDocument = change.FullDocument;
        var resumeToken = change.ResumeToken;
        var documentKey = change.DocumentKey;
        var updateDescription = change.UpdateDescription?.ToBsonDocument();

        var databaseName = change.DatabaseNamespace?.DatabaseName;
        var collectionName = change.CollectionNamespace?.CollectionName;

        var clusterTime = change.ClusterTime;

        var clusterDateTime = clusterTime != null
            ? DateTimeOffset.FromUnixTimeSeconds(clusterTime.Timestamp).DateTime
            : DateTime.UtcNow;

        return new MongoChangeStreamEvent<BsonDocument>(
            operationType,
            fullDocument,
            resumeToken,
            collectionName,
            databaseName,
            clusterDateTime,
            updateDescription,
            documentKey);
    }

    private Func<MongoChangeStreamEvent<BsonDocument>, T> BuildDefaultMapper()
    {
        var rowMapper = MongoMapperBuilder.GetOrCreateMapper<T>();

        return changeEvent =>
        {
            // For delete operations, FullDocument is typically null
            if (changeEvent.OperationType == MongoChangeStreamOperationType.Delete)
            {
                throw new MongoMappingException(
                    $"Change stream delete operation has no FullDocument to map to {typeof(T).Name}",
                    null,
                    changeEvent.FullDocument);
            }

            if (changeEvent.FullDocument == null)
            {
                throw new MongoMappingException(
                    $"Change stream event has no FullDocument to map to {typeof(T).Name}",
                    null,
                    changeEvent.FullDocument);
            }

            var row = new MongoRow(changeEvent.FullDocument);
            return rowMapper(row);
        };
    }

    private static bool IsTransientError(Exception ex)
    {
        // Check for MongoDB connection/timeout exceptions
        return ex is TimeoutException ||
               ex is MongoConnectionException ||
               (ex is MongoCommandException cmdEx && IsRetryableCommandError(cmdEx)) ||
               (ex.InnerException != null && IsTransientError(ex.InnerException));
    }

    /// <summary>
    ///     Determines if a MongoDB command error is retryable.
    /// </summary>
    /// <param name="exception">The command exception.</param>
    /// <returns>True if the error is retryable.</returns>
    private static bool IsRetryableCommandError(MongoCommandException exception)
    {
        // Common retryable error codes
        var retryableCodes = new HashSet<int>
        {
            6, // HostUnreachable
            7, // HostNotFound
            89, // NetworkTimeout
            91, // ShutdownInProgress
            189, // PrimarySteppedDown
            262, // ExceededTimeLimit
            9001, // SocketException
            10107, // NotWritablePrimary
        };

        return retryableCodes.Contains(exception.Code);
    }
}

using Microsoft.Azure.Cosmos;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;
using NPipeline.Connectors.Azure.CosmosDb.Connection;
using NPipeline.Connectors.Azure.CosmosDb.Writers;
using NPipeline.Connectors.Nodes;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Azure.CosmosDb.Nodes;

/// <summary>
///     Cosmos DB sink node for writing data to Cosmos DB.
/// </summary>
/// <typeparam name="T">The type of objects consumed by sink.</typeparam>
public class CosmosSinkNode<T> : DatabaseSinkNode<T>
{
    private static readonly Lazy<IStorageResolver> DefaultResolver = new(
        () => CosmosStorageResolverFactory.CreateResolver(),
        LazyThreadSafetyMode.ExecutionAndPublication);

    private readonly CosmosConfiguration _configuration;
    private readonly string? _connectionName;
    private readonly ICosmosConnectionPool? _connectionPool;
    private readonly string _containerId;
    private readonly string _databaseId;
    private readonly Func<T, string>? _idSelector;
    private readonly Func<T, PartitionKey>? _partitionKeySelector;
    private readonly IStorageProvider? _storageProvider;
    private readonly IStorageResolver? _storageResolver;
    private readonly StorageUri? _storageUri;
    private readonly CosmosWriteStrategy _writeStrategy;

    /// <summary>
    ///     Initializes a new instance of <see cref="CosmosSinkNode{T}" /> class.
    /// </summary>
    /// <param name="connectionString">The Cosmos DB connection string.</param>
    /// <param name="databaseId">The database identifier.</param>
    /// <param name="containerId">The container identifier.</param>
    /// <param name="writeStrategy">The write strategy.</param>
    /// <param name="idSelector">Optional function to extract document ID from item.</param>
    /// <param name="partitionKeySelector">Optional function to extract partition key from item.</param>
    /// <param name="configuration">Optional configuration.</param>
    public CosmosSinkNode(
        string connectionString,
        string databaseId,
        string containerId,
        CosmosWriteStrategy writeStrategy = CosmosWriteStrategy.Batch,
        Func<T, string>? idSelector = null,
        Func<T, PartitionKey>? partitionKeySelector = null,
        CosmosConfiguration? configuration = null)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentNullException(nameof(connectionString));

        if (string.IsNullOrWhiteSpace(databaseId))
            throw new ArgumentNullException(nameof(databaseId));

        if (string.IsNullOrWhiteSpace(containerId))
            throw new ArgumentNullException(nameof(containerId));

        _configuration = configuration ?? new CosmosConfiguration();

        if (string.IsNullOrWhiteSpace(_configuration.ConnectionString))
            _configuration.ConnectionString = connectionString;

        if (string.IsNullOrWhiteSpace(_configuration.DatabaseId))
            _configuration.DatabaseId = databaseId;

        _configuration.WriteStrategy = writeStrategy;

        if (writeStrategy == CosmosWriteStrategy.Insert)
            _configuration.UseUpsert = false;
        else if (writeStrategy == CosmosWriteStrategy.Upsert)
            _configuration.UseUpsert = true;

        _configuration.Validate();
        _connectionPool = new CosmosConnectionPool(connectionString, _configuration);
        _databaseId = databaseId;
        _containerId = containerId;
        _writeStrategy = writeStrategy;
        _idSelector = idSelector;
        _partitionKeySelector = partitionKeySelector;
        _connectionName = null;
    }

    /// <summary>
    ///     Initializes a new instance of <see cref="CosmosSinkNode{T}" /> class with connection pool.
    /// </summary>
    /// <param name="connectionPool">The connection pool.</param>
    /// <param name="databaseId">The database identifier.</param>
    /// <param name="containerId">The container identifier.</param>
    /// <param name="writeStrategy">The write strategy.</param>
    /// <param name="idSelector">Optional function to extract document ID from item.</param>
    /// <param name="partitionKeySelector">Optional function to extract partition key from item.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="connectionName">Optional named connection when using a shared pool.</param>
    public CosmosSinkNode(
        ICosmosConnectionPool connectionPool,
        string databaseId,
        string containerId,
        CosmosWriteStrategy writeStrategy = CosmosWriteStrategy.Batch,
        Func<T, string>? idSelector = null,
        Func<T, PartitionKey>? partitionKeySelector = null,
        CosmosConfiguration? configuration = null,
        string? connectionName = null)
    {
        ArgumentNullException.ThrowIfNull(connectionPool);

        if (string.IsNullOrWhiteSpace(databaseId))
            throw new ArgumentNullException(nameof(databaseId));

        if (string.IsNullOrWhiteSpace(containerId))
            throw new ArgumentNullException(nameof(containerId));

        _configuration = configuration ?? new CosmosConfiguration();
        _configuration.NamedConnection ??= connectionName;

        if (string.IsNullOrWhiteSpace(_configuration.DatabaseId))
            _configuration.DatabaseId = databaseId;

        _configuration.WriteStrategy = writeStrategy;

        if (writeStrategy == CosmosWriteStrategy.Insert)
            _configuration.UseUpsert = false;
        else if (writeStrategy == CosmosWriteStrategy.Upsert)
            _configuration.UseUpsert = true;

        _configuration.Validate();
        _connectionPool = connectionPool;
        _databaseId = databaseId;
        _containerId = containerId;
        _writeStrategy = writeStrategy;
        _idSelector = idSelector;
        _partitionKeySelector = partitionKeySelector;

        _connectionName = string.IsNullOrWhiteSpace(connectionName)
            ? null
            : connectionName;
    }

    /// <summary>
    ///     Initializes a new instance of <see cref="CosmosSinkNode{T}" /> class using a <see cref="StorageUri" />.
    /// </summary>
    /// <param name="uri">The storage URI containing Cosmos DB connection information.</param>
    /// <param name="writeStrategy">The write strategy.</param>
    /// <param name="resolver">The storage resolver used to obtain storage provider.</param>
    /// <param name="idSelector">Optional function to extract document ID from item.</param>
    /// <param name="partitionKeySelector">Optional function to extract partition key from item.</param>
    /// <param name="configuration">Optional configuration.</param>
    public CosmosSinkNode(
        StorageUri uri,
        CosmosWriteStrategy writeStrategy = CosmosWriteStrategy.Batch,
        IStorageResolver? resolver = null,
        Func<T, string>? idSelector = null,
        Func<T, PartitionKey>? partitionKeySelector = null,
        CosmosConfiguration? configuration = null)
    {
        ArgumentNullException.ThrowIfNull(uri);

        // Extract database and container from URI path
        // Path format: /databaseId/containerId
        var (databaseId, containerId) = ParseUriPath(uri.Path);

        _storageUri = uri;
        _storageResolver = resolver;
        _writeStrategy = writeStrategy;
        _idSelector = idSelector;
        _partitionKeySelector = partitionKeySelector;
        _configuration = configuration ?? new CosmosConfiguration();

        if (string.IsNullOrWhiteSpace(_configuration.DatabaseId))
            _configuration.DatabaseId = databaseId;

        _configuration.WriteStrategy = writeStrategy;

        if (writeStrategy == CosmosWriteStrategy.Insert)
            _configuration.UseUpsert = false;
        else if (writeStrategy == CosmosWriteStrategy.Upsert)
            _configuration.UseUpsert = true;

        _configuration.Validate();
        _connectionName = null;

        _databaseId = databaseId;
        _containerId = containerId;
    }

    /// <summary>
    ///     Initializes a new instance of <see cref="CosmosSinkNode{T}" /> class using a specific storage provider.
    /// </summary>
    /// <param name="provider">The storage provider.</param>
    /// <param name="uri">The storage URI containing Cosmos DB connection information.</param>
    /// <param name="writeStrategy">The write strategy.</param>
    /// <param name="idSelector">Optional function to extract document ID from item.</param>
    /// <param name="partitionKeySelector">Optional function to extract partition key from item.</param>
    /// <param name="configuration">Optional configuration.</param>
    public CosmosSinkNode(
        IStorageProvider provider,
        StorageUri uri,
        CosmosWriteStrategy writeStrategy = CosmosWriteStrategy.Batch,
        Func<T, string>? idSelector = null,
        Func<T, PartitionKey>? partitionKeySelector = null,
        CosmosConfiguration? configuration = null)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(uri);

        // Extract database and container from URI path
        // Path format: /databaseId/containerId
        var (databaseId, containerId) = ParseUriPath(uri.Path);

        _storageProvider = provider;
        _storageUri = uri;
        _writeStrategy = writeStrategy;
        _idSelector = idSelector;
        _partitionKeySelector = partitionKeySelector;
        _configuration = configuration ?? new CosmosConfiguration();

        if (string.IsNullOrWhiteSpace(_configuration.DatabaseId))
            _configuration.DatabaseId = databaseId;

        _configuration.WriteStrategy = writeStrategy;

        if (writeStrategy == CosmosWriteStrategy.Insert)
            _configuration.UseUpsert = false;
        else if (writeStrategy == CosmosWriteStrategy.Upsert)
            _configuration.UseUpsert = true;

        _configuration.Validate();
        _connectionName = null;

        _databaseId = databaseId;
        _containerId = containerId;
    }

    /// <summary>
    ///     Gets batch size for batch writes.
    /// </summary>
    protected override int BatchSize => _configuration.WriteBatchSize;

    /// <summary>
    ///     Gets whether to continue on error.
    /// </summary>
    protected override bool ContinueOnError => _configuration.ContinueOnError;

    /// <summary>
    ///     Gets a database connection asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    protected override async Task<IDatabaseConnection> GetConnectionAsync(CancellationToken cancellationToken)
    {
        // If using StorageUri-based construction, get connection from database storage provider
        if (_storageUri != null)
        {
            var provider = _storageProvider ?? StorageProviderFactory.GetProviderOrThrow(
                _storageResolver ?? DefaultResolver.Value,
                _storageUri);

            if (provider is IDatabaseStorageProvider databaseProvider)
                return await databaseProvider.GetConnectionAsync(_storageUri, cancellationToken);

            throw new InvalidOperationException(
                $"Storage provider must implement {nameof(IDatabaseStorageProvider)} to use StorageUri.");
        }

        // Original connection pool logic
        var client = _connectionName is { Length: > 0 }
            ? await _connectionPool!.GetClientAsync(_connectionName, cancellationToken)
            : await _connectionPool!.GetClientAsync(cancellationToken);

        var database = client.GetDatabase(_databaseId);
        var container = database.GetContainer(_containerId);

        return new CosmosDatabaseConnection(database, container, _configuration);
    }

    /// <summary>
    ///     Creates a database writer for the connection.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    protected override Task<IDatabaseWriter<T>> CreateWriterAsync(IDatabaseConnection connection, CancellationToken cancellationToken)
    {
        var cosmosConnection = (CosmosDatabaseConnection)connection;

        var writer = _writeStrategy switch
        {
            CosmosWriteStrategy.Upsert => Task.FromResult<IDatabaseWriter<T>>(
                new CosmosPerRowWriter<T>(cosmosConnection.Container, _idSelector, _partitionKeySelector, _configuration)),
            CosmosWriteStrategy.PerRow => Task.FromResult<IDatabaseWriter<T>>(
                new CosmosPerRowWriter<T>(cosmosConnection.Container, _idSelector, _partitionKeySelector, _configuration)),
            CosmosWriteStrategy.Batch => Task.FromResult<IDatabaseWriter<T>>(
                new CosmosBatchWriter<T>(cosmosConnection.Container, _idSelector, _partitionKeySelector, _configuration)),
            CosmosWriteStrategy.TransactionalBatch => Task.FromResult<IDatabaseWriter<T>>(
                new CosmosTransactionalBatchWriter<T>(cosmosConnection.Container, _idSelector, _partitionKeySelector, _configuration)),
            CosmosWriteStrategy.Bulk => Task.FromResult<IDatabaseWriter<T>>(
                new CosmosBulkWriter<T>(cosmosConnection.Container, _idSelector, _partitionKeySelector, _configuration)),
            _ => throw new NotSupportedException($"Write strategy '{_writeStrategy}' is not supported"),
        };

        return writer;
    }

    /// <summary>
    ///     Parses the URI path to extract database and container IDs.
    /// </summary>
    /// <param name="path">The URI path in format /databaseId/containerId.</param>
    /// <returns>A tuple containing database ID and container ID.</returns>
    private static (string databaseId, string containerId) ParseUriPath(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            throw new ArgumentException("URI path cannot be empty.", nameof(path));

        // Remove leading slash and split
        var segments = path.TrimStart('/').Split('/', StringSplitOptions.RemoveEmptyEntries);

        if (segments.Length < 2)
        {
            throw new ArgumentException(
                "URI path must contain both database and container. Format: /databaseId/containerId", nameof(path));
        }

        return (segments[0], segments[1]);
    }
}

using MongoDB.Bson;
using MongoDB.Driver;
using NPipeline.Connectors.MongoDB.Configuration;
using NPipeline.Connectors.MongoDB.Connection;
using NPipeline.Connectors.MongoDB.Mapping;
using NPipeline.Connectors.MongoDB.Nodes;

namespace NPipeline.Connectors.MongoDB.DependencyInjection;

/// <summary>
///     Factory for creating MongoDB source nodes with dependency injection support.
/// </summary>
public class MongoSourceNodeFactory
{
    private readonly IMongoConnectionPool _connectionPool;

    /// <summary>
    ///     Initializes a new instance of the <see cref="MongoSourceNodeFactory" /> class.
    /// </summary>
    /// <param name="connectionPool">The connection pool.</param>
    public MongoSourceNodeFactory(IMongoConnectionPool connectionPool)
    {
        _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));
    }

    /// <summary>
    ///     Creates a MongoDB source node using a connection from the pool.
    /// </summary>
    /// <typeparam name="T">The type of objects to read.</typeparam>
    /// <param name="collectionName">The name of the collection.</param>
    /// <param name="filter">Optional filter definition.</param>
    /// <param name="connectionName">Optional connection name.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <returns>A configured MongoDB source node.</returns>
    public MongoSourceNode<T> Create<T>(
        string collectionName,
        FilterDefinition<BsonDocument>? filter = null,
        string? connectionName = null,
        MongoConfiguration? configuration = null)
        where T : class, new()
    {
        ArgumentNullException.ThrowIfNull(collectionName);

        var config = configuration ?? new MongoConfiguration();
        config.CollectionName = collectionName;

        if (string.IsNullOrWhiteSpace(config.DatabaseName))
            throw new ArgumentException("DatabaseName must be specified in configuration.", nameof(configuration));

        var client = _connectionPool.GetClient(connectionName);

        return new MongoSourceNode<T>(client, config, filter);
    }

    /// <summary>
    ///     Creates a MongoDB source node using a connection from the pool with explicit database name.
    /// </summary>
    /// <typeparam name="T">The type of objects to read.</typeparam>
    /// <param name="collectionName">The name of the collection.</param>
    /// <param name="databaseName">The name of the database.</param>
    /// <param name="filter">Optional filter definition.</param>
    /// <param name="connectionName">Optional connection name.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <returns>A configured MongoDB source node.</returns>
    public MongoSourceNode<T> Create<T>(
        string collectionName,
        string databaseName,
        FilterDefinition<BsonDocument>? filter = null,
        string? connectionName = null,
        MongoConfiguration? configuration = null)
        where T : class, new()
    {
        ArgumentNullException.ThrowIfNull(collectionName);
        ArgumentNullException.ThrowIfNull(databaseName);

        var config = configuration ?? new MongoConfiguration();
        config.CollectionName = collectionName;
        config.DatabaseName = databaseName;

        var client = _connectionPool.GetClient(connectionName);

        return new MongoSourceNode<T>(client, config, filter);
    }

    /// <summary>
    ///     Creates a MongoDB source node with a custom mapper.
    /// </summary>
    /// <typeparam name="T">The type of objects to read.</typeparam>
    /// <param name="collectionName">The name of the collection.</param>
    /// <param name="databaseName">The name of the database.</param>
    /// <param name="customMapper">Custom mapper function.</param>
    /// <param name="filter">Optional filter definition.</param>
    /// <param name="connectionName">Optional connection name.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <returns>A configured MongoDB source node.</returns>
    public MongoSourceNode<T> CreateWithMapper<T>(
        string collectionName,
        string databaseName,
        Func<MongoRow, T> customMapper,
        FilterDefinition<BsonDocument>? filter = null,
        string? connectionName = null,
        MongoConfiguration? configuration = null)
        where T : class, new()
    {
        ArgumentNullException.ThrowIfNull(collectionName);
        ArgumentNullException.ThrowIfNull(databaseName);
        ArgumentNullException.ThrowIfNull(customMapper);

        var config = configuration ?? new MongoConfiguration();
        config.CollectionName = collectionName;
        config.DatabaseName = databaseName;

        var client = _connectionPool.GetClient(connectionName);

        return new MongoSourceNode<T>(client, config, filter, customMapper: customMapper);
    }
}

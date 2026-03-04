using MongoDB.Bson;
using MongoDB.Driver;
using NPipeline.Connectors.MongoDB.Configuration;
using NPipeline.Connectors.MongoDB.Connection;
using NPipeline.Connectors.MongoDB.Nodes;

namespace NPipeline.Connectors.MongoDB.DependencyInjection;

/// <summary>
///     Factory for creating MongoDB sink nodes with dependency injection support.
/// </summary>
public class MongoSinkNodeFactory
{
    private readonly IMongoConnectionPool _connectionPool;

    /// <summary>
    ///     Initializes a new instance of the <see cref="MongoSinkNodeFactory" /> class.
    /// </summary>
    /// <param name="connectionPool">The connection pool.</param>
    public MongoSinkNodeFactory(IMongoConnectionPool connectionPool)
    {
        _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));
    }

    /// <summary>
    ///     Creates a MongoDB sink node using a connection from the pool.
    /// </summary>
    /// <typeparam name="T">The type of objects to write.</typeparam>
    /// <param name="collectionName">The name of the collection.</param>
    /// <param name="writeStrategy">The write strategy to use.</param>
    /// <param name="connectionName">Optional connection name.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <returns>A configured MongoDB sink node.</returns>
    public MongoSinkNode<T> Create<T>(
        string collectionName,
        MongoWriteStrategy writeStrategy = MongoWriteStrategy.BulkWrite,
        string? connectionName = null,
        MongoConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(collectionName);

        var config = configuration ?? new MongoConfiguration();
        config.CollectionName = collectionName;
        config.WriteStrategy = writeStrategy;

        if (string.IsNullOrWhiteSpace(config.DatabaseName))
            throw new ArgumentException("DatabaseName must be specified in configuration.", nameof(configuration));

        var client = _connectionPool.GetClient(connectionName);

        return new MongoSinkNode<T>(client, config);
    }

    /// <summary>
    ///     Creates a MongoDB sink node using a connection from the pool with explicit database name.
    /// </summary>
    /// <typeparam name="T">The type of objects to write.</typeparam>
    /// <param name="collectionName">The name of the collection.</param>
    /// <param name="databaseName">The name of the database.</param>
    /// <param name="writeStrategy">The write strategy to use.</param>
    /// <param name="connectionName">Optional connection name.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <returns>A configured MongoDB sink node.</returns>
    public MongoSinkNode<T> Create<T>(
        string collectionName,
        string databaseName,
        MongoWriteStrategy writeStrategy = MongoWriteStrategy.BulkWrite,
        string? connectionName = null,
        MongoConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(collectionName);
        ArgumentNullException.ThrowIfNull(databaseName);

        var config = configuration ?? new MongoConfiguration();
        config.CollectionName = collectionName;
        config.DatabaseName = databaseName;
        config.WriteStrategy = writeStrategy;

        var client = _connectionPool.GetClient(connectionName);

        return new MongoSinkNode<T>(client, config);
    }

    /// <summary>
    ///     Creates a MongoDB sink node with a custom document mapper.
    /// </summary>
    /// <typeparam name="T">The type of objects to write.</typeparam>
    /// <param name="collectionName">The name of the collection.</param>
    /// <param name="databaseName">The name of the database.</param>
    /// <param name="documentMapper">Custom document mapper function.</param>
    /// <param name="writeStrategy">The write strategy to use.</param>
    /// <param name="connectionName">Optional connection name.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <returns>A configured MongoDB sink node.</returns>
    public MongoSinkNode<T> CreateWithMapper<T>(
        string collectionName,
        string databaseName,
        Func<T, BsonDocument> documentMapper,
        MongoWriteStrategy writeStrategy = MongoWriteStrategy.BulkWrite,
        string? connectionName = null,
        MongoConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(collectionName);
        ArgumentNullException.ThrowIfNull(databaseName);
        ArgumentNullException.ThrowIfNull(documentMapper);

        var config = configuration ?? new MongoConfiguration();
        config.CollectionName = collectionName;
        config.DatabaseName = databaseName;
        config.WriteStrategy = writeStrategy;

        var client = _connectionPool.GetClient(connectionName);

        return new MongoSinkNode<T>(client, config, documentMapper);
    }

    /// <summary>
    ///     Creates a MongoDB sink node with upsert support.
    /// </summary>
    /// <typeparam name="T">The type of objects to write.</typeparam>
    /// <param name="collectionName">The name of the collection.</param>
    /// <param name="databaseName">The name of the database.</param>
    /// <param name="upsertKeyFields">The key fields for upsert operations.</param>
    /// <param name="connectionName">Optional connection name.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <returns>A configured MongoDB sink node.</returns>
    public MongoSinkNode<T> CreateUpsert<T>(
        string collectionName,
        string databaseName,
        string[] upsertKeyFields,
        string? connectionName = null,
        MongoConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(collectionName);
        ArgumentNullException.ThrowIfNull(databaseName);
        ArgumentNullException.ThrowIfNull(upsertKeyFields);

        if (upsertKeyFields.Length == 0)
            throw new ArgumentException("At least one upsert key field must be specified.", nameof(upsertKeyFields));

        var config = configuration ?? new MongoConfiguration();
        config.CollectionName = collectionName;
        config.DatabaseName = databaseName;
        config.WriteStrategy = MongoWriteStrategy.Upsert;
        config.UpsertKeyFields = upsertKeyFields;

        var client = _connectionPool.GetClient(connectionName);

        return new MongoSinkNode<T>(client, config);
    }

    /// <summary>
    ///     Creates a MongoDB sink node with upsert support and custom filter builder.
    /// </summary>
    /// <typeparam name="T">The type of objects to write.</typeparam>
    /// <param name="collectionName">The name of the collection.</param>
    /// <param name="databaseName">The name of the database.</param>
    /// <param name="upsertFilterBuilder">Custom filter builder for upsert operations.</param>
    /// <param name="connectionName">Optional connection name.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <returns>A configured MongoDB sink node.</returns>
    public MongoSinkNode<T> CreateUpsert<T>(
        string collectionName,
        string databaseName,
        Func<T, FilterDefinition<BsonDocument>> upsertFilterBuilder,
        string? connectionName = null,
        MongoConfiguration? configuration = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(collectionName);
        ArgumentNullException.ThrowIfNull(databaseName);
        ArgumentNullException.ThrowIfNull(upsertFilterBuilder);

        var config = configuration ?? new MongoConfiguration();
        config.CollectionName = collectionName;
        config.DatabaseName = databaseName;
        config.WriteStrategy = MongoWriteStrategy.Upsert;

        var client = _connectionPool.GetClient(connectionName);

        return new MongoSinkNode<T>(client, config, upsertFilterBuilder: upsertFilterBuilder);
    }
}

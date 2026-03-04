using MongoDB.Bson;
using MongoDB.Driver;
using NPipeline.Connectors.MongoDB.Configuration;

namespace NPipeline.Connectors.MongoDB.Writers;

/// <summary>
///     Factory for creating the appropriate writer based on configuration.
/// </summary>
public static class MongoWriterFactory
{
    /// <summary>
    ///     Creates the appropriate writer based on the specified write strategy.
    /// </summary>
    /// <typeparam name="T">The type of objects to write.</typeparam>
    /// <param name="strategy">The write strategy to use.</param>
    /// <param name="documentMapper">Optional custom document mapper function.</param>
    /// <param name="upsertFilterBuilder">Optional custom filter builder for upsert operations.</param>
    /// <param name="upsertKeyFields">Optional key fields for upsert operations.</param>
    /// <returns>An instance of <see cref="IMongoWriter{T}" /> appropriate for the strategy.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when an unsupported strategy is specified.</exception>
    public static IMongoWriter<T> Create<T>(
        MongoWriteStrategy strategy,
        Func<T, BsonDocument>? documentMapper = null,
        Func<T, FilterDefinition<BsonDocument>>? upsertFilterBuilder = null,
        string[]? upsertKeyFields = null)
    {
        return strategy switch
        {
            MongoWriteStrategy.InsertMany => new MongoInsertManyWriter<T>(documentMapper),
            MongoWriteStrategy.Upsert => new MongoUpsertWriter<T>(upsertKeyFields, documentMapper, upsertFilterBuilder),
            MongoWriteStrategy.BulkWrite => new MongoBulkWriter<T>(documentMapper),
            _ => throw new ArgumentOutOfRangeException(nameof(strategy), strategy, $"Unsupported write strategy: {strategy}"),
        };
    }

    /// <summary>
    ///     Creates the appropriate writer based on configuration settings.
    /// </summary>
    /// <typeparam name="T">The type of objects to write.</typeparam>
    /// <param name="configuration">The MongoDB configuration.</param>
    /// <param name="documentMapper">Optional custom document mapper function.</param>
    /// <param name="upsertFilterBuilder">Optional custom filter builder for upsert operations.</param>
    /// <returns>An instance of <see cref="IMongoWriter{T}" /> appropriate for the configuration.</returns>
    public static IMongoWriter<T> CreateFromConfiguration<T>(
        MongoConfiguration configuration,
        Func<T, BsonDocument>? documentMapper = null,
        Func<T, FilterDefinition<BsonDocument>>? upsertFilterBuilder = null)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        return Create(
            configuration.WriteStrategy,
            documentMapper,
            upsertFilterBuilder,
            configuration.UpsertKeyFields);
    }
}

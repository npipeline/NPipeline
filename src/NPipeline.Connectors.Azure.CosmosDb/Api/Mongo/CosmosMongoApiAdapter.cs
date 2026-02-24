using MongoDB.Driver;
using NPipeline.Connectors.Azure.CosmosDb.Abstractions;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;

namespace NPipeline.Connectors.Azure.CosmosDb.Api.Mongo;

/// <summary>
///     Mongo API adapter for Cosmos Mongo-compatible endpoints.
/// </summary>
public sealed class CosmosMongoApiAdapter : ICosmosApiAdapter
{
    /// <inheritdoc />
    public CosmosApiType ApiType => CosmosApiType.Mongo;

    /// <inheritdoc />
    public IReadOnlyCollection<string> SupportedSchemes { get; } = ["cosmos-mongo"];

    /// <inheritdoc />
    public Task<object> CreateClientAsync(CosmosConfiguration configuration, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var connectionString = configuration.MongoConnectionString ?? configuration.ConnectionString;

        if (string.IsNullOrWhiteSpace(connectionString))
            throw new InvalidOperationException("MongoConnectionString or ConnectionString is required for Mongo adapter.");

        return Task.FromResult<object>(new MongoClient(connectionString));
    }

    /// <inheritdoc />
    public ICosmosSourceExecutor CreateSourceExecutor(object client, CosmosConfiguration configuration)
    {
        return new CosmosMongoSourceExecutor((MongoClient)client, configuration);
    }

    /// <inheritdoc />
    public ICosmosSinkExecutor<T> CreateSinkExecutor<T>(
        object client,
        CosmosConfiguration configuration,
        Func<T, string>? idSelector = null)
    {
        return new CosmosMongoSinkExecutor<T>((MongoClient)client, configuration, idSelector);
    }
}

using Microsoft.Azure.Cosmos;
using NPipeline.Connectors.Azure.CosmosDb.Abstractions;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;

namespace NPipeline.Connectors.Azure.CosmosDb.Api.Sql;

/// <summary>
///     SQL API adapter for Cosmos Core API.
/// </summary>
public sealed class CosmosSqlApiAdapter : ICosmosApiAdapter
{
    /// <inheritdoc />
    public CosmosApiType ApiType => CosmosApiType.Sql;

    /// <inheritdoc />
    public IReadOnlyCollection<string> SupportedSchemes { get; } = ["cosmosdb", "cosmos"];

    /// <inheritdoc />
    public Task<object> CreateClientAsync(CosmosConfiguration configuration, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var connectionString = configuration.ConnectionString;

        if (string.IsNullOrWhiteSpace(connectionString))
            throw new InvalidOperationException("ConnectionString is required for SQL API adapter.");

        var clientOptions = new CosmosClientOptions
        {
            RequestTimeout = TimeSpan.FromSeconds(configuration.RequestTimeout),
            AllowBulkExecution = configuration.AllowBulkExecution,
            EnableContentResponseOnWrite = configuration.EnableContentResponseOnWrite,
            MaxRetryAttemptsOnRateLimitedRequests = configuration.MaxRetryAttempts,
            MaxRetryWaitTimeOnRateLimitedRequests = configuration.MaxRetryWaitTime,
        };

        return Task.FromResult<object>(new CosmosClient(connectionString, clientOptions));
    }

    /// <inheritdoc />
    public ICosmosSourceExecutor CreateSourceExecutor(object client, CosmosConfiguration configuration)
    {
        return new CosmosSqlSourceExecutor((CosmosClient)client, configuration);
    }

    /// <inheritdoc />
    public ICosmosSinkExecutor<T> CreateSinkExecutor<T>(
        object client,
        CosmosConfiguration configuration,
        Func<T, string>? idSelector = null)
    {
        return new CosmosSqlSinkExecutor<T>((CosmosClient)client, configuration, idSelector);
    }
}

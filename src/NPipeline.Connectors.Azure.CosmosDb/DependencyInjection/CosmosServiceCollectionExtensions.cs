using Azure.Core;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.Azure.CosmosDb.Abstractions;
using NPipeline.Connectors.Azure.CosmosDb.Api.Cassandra;
using NPipeline.Connectors.Azure.CosmosDb.Api.Mongo;
using NPipeline.Connectors.Azure.CosmosDb.Api.Sql;
using NPipeline.Connectors.Azure.CosmosDb.ChangeFeed;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;
using NPipeline.Connectors.Azure.CosmosDb.Connection;

namespace NPipeline.Connectors.Azure.CosmosDb.DependencyInjection;

/// <summary>
///     Extension methods for configuring Cosmos DB connector services.
/// </summary>
public static class CosmosServiceCollectionExtensions
{
    /// <summary>
    ///     Adds Cosmos DB connector services to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">An action to configure Cosmos DB options.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddCosmosDbConnector(
        this IServiceCollection services,
        Action<CosmosOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var options = new CosmosOptions();
        configure(options);
        options.Validate();

        services.AddSingleton(options);

        // Register the connection pool
        services.AddSingleton<ICosmosConnectionPool>(sp =>
            new CosmosConnectionPool(sp.GetRequiredService<CosmosOptions>()));

        // Register API adapters and resolver
        services.AddSingleton<ICosmosApiAdapter, CosmosSqlApiAdapter>();
        services.AddSingleton<ICosmosApiAdapter, CosmosMongoApiAdapter>();
        services.AddSingleton<ICosmosApiAdapter, CosmosCassandraApiAdapter>();
        services.AddSingleton<ICosmosApiAdapterResolver, CosmosApiAdapterResolver>();

        // Register the default checkpoint store (in-memory)
        services.AddSingleton<IChangeFeedCheckpointStore, InMemoryChangeFeedCheckpointStore>();

        return services;
    }

    /// <summary>
    ///     Adds Cosmos DB connector services with a connection string.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The Cosmos DB connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddCosmosDbConnector(
        this IServiceCollection services,
        string connectionString)
    {
        ArgumentNullException.ThrowIfNull(connectionString);

        return services.AddCosmosDbConnector(options => options.DefaultConnectionString = connectionString);
    }

    /// <summary>
    ///     Adds Cosmos DB connector services with Azure AD authentication.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="endpoint">The Cosmos DB endpoint.</param>
    /// <param name="credential">The token credential for authentication.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddCosmosDbConnector(
        this IServiceCollection services,
        Uri endpoint,
        TokenCredential credential)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        ArgumentNullException.ThrowIfNull(credential);

        return services.AddCosmosDbConnector(options =>
        {
            options.DefaultEndpoint = endpoint;
            options.DefaultCredential = credential;
        });
    }

    /// <summary>
    ///     Adds a custom checkpoint store for Change Feed persistence.
    /// </summary>
    /// <typeparam name="TCheckpointStore">The type of checkpoint store.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddCosmosChangeFeedCheckpointStore<TCheckpointStore>(
        this IServiceCollection services)
        where TCheckpointStore : class, IChangeFeedCheckpointStore
    {
        services.AddSingleton<IChangeFeedCheckpointStore, TCheckpointStore>();
        return services;
    }
}

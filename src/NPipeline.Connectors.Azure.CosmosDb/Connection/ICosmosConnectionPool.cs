using Microsoft.Azure.Cosmos;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;

namespace NPipeline.Connectors.Azure.CosmosDb.Connection;

/// <summary>
///     Abstraction for Cosmos DB connection pool management.
///     Provides CosmosClient lifecycle management and Container access.
/// </summary>
public interface ICosmosConnectionPool : IAsyncDisposable
{
    /// <summary>
    ///     Gets the connection string used by this pool.
    ///     Returns the default connection string, or null if no default connection is configured.
    /// </summary>
    string? ConnectionString { get; }

    /// <summary>
    ///     Gets a CosmosClient from the pool asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A CosmosClient instance.</returns>
    Task<CosmosClient> GetClientAsync(CancellationToken cancellationToken = default);

    /// <summary>
    ///     Gets a CosmosClient for a named connection.
    /// </summary>
    /// <param name="name">The connection name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A CosmosClient instance.</returns>
    Task<CosmosClient> GetClientAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Gets an API-specific client from the pool.
    /// </summary>
    /// <typeparam name="TClient">Client type to retrieve.</typeparam>
    /// <param name="apiType">The Cosmos API type.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The API client instance.</returns>
    Task<TClient> GetClientAsync<TClient>(CosmosApiType apiType, CancellationToken cancellationToken = default)
        where TClient : class;

    /// <summary>
    ///     Gets an API-specific named client from the pool.
    /// </summary>
    /// <typeparam name="TClient">Client type to retrieve.</typeparam>
    /// <param name="name">The connection name.</param>
    /// <param name="apiType">The Cosmos API type.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The API client instance.</returns>
    Task<TClient> GetClientAsync<TClient>(string name, CosmosApiType apiType, CancellationToken cancellationToken = default)
        where TClient : class;

    /// <summary>
    ///     Gets a Container reference for the specified database and container.
    /// </summary>
    /// <param name="databaseId">The database identifier.</param>
    /// <param name="containerId">The container identifier.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A Container reference.</returns>
    Task<Container> GetContainerAsync(
        string databaseId,
        string containerId,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Gets a Container reference using a named connection.
    /// </summary>
    /// <param name="name">The connection name.</param>
    /// <param name="databaseId">The database identifier.</param>
    /// <param name="containerId">The container identifier.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A Container reference.</returns>
    Task<Container> GetContainerAsync(
        string name,
        string databaseId,
        string containerId,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Checks if a named connection exists.
    /// </summary>
    /// <param name="name">The name of the connection.</param>
    /// <returns>True if the named connection exists; otherwise, false.</returns>
    bool HasNamedConnection(string name);

    /// <summary>
    ///     Gets all named connection names.
    /// </summary>
    /// <returns>A collection of named connection names.</returns>
    IEnumerable<string> GetNamedConnectionNames();
}

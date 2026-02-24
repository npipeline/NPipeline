namespace NPipeline.Connectors.Azure.CosmosDb.ChangeFeed;

/// <summary>
///     Stores and retrieves continuation tokens for Cosmos DB Change Feed processing.
///     Implementations should provide persistent storage for production scenarios.
/// </summary>
public interface IChangeFeedCheckpointStore
{
    /// <summary>
    ///     Gets the continuation token for a container's change feed.
    /// </summary>
    /// <param name="databaseId">The database identifier.</param>
    /// <param name="containerId">The container identifier.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>
    ///     The continuation token if found; otherwise, null.
    ///     When null is returned, the change feed will start from the configured default position.
    /// </returns>
    Task<string?> GetTokenAsync(
        string databaseId,
        string containerId,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Saves the continuation token for a container's change feed.
    /// </summary>
    /// <param name="databaseId">The database identifier.</param>
    /// <param name="containerId">The container identifier.</param>
    /// <param name="token">The continuation token to save.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task SaveTokenAsync(
        string databaseId,
        string containerId,
        string token,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Deletes the continuation token for a container's change feed.
    /// </summary>
    /// <param name="databaseId">The database identifier.</param>
    /// <param name="containerId">The container identifier.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task DeleteTokenAsync(
        string databaseId,
        string containerId,
        CancellationToken cancellationToken = default);
}

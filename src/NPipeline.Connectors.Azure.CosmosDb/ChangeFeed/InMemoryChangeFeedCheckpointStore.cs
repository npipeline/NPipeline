using System.Collections.Concurrent;

namespace NPipeline.Connectors.Azure.CosmosDb.ChangeFeed;

/// <summary>
///     An in-memory implementation of <see cref="IChangeFeedCheckpointStore" />.
///     Stores continuation tokens in memory, which are lost when the application restarts.
///     For production scenarios, use a persistent implementation such as Azure Blob Storage.
/// </summary>
/// <remarks>
///     This implementation is suitable for:
///     - Development and testing scenarios
///     - Short-lived processes where checkpoint persistence is not required
///     - Scenarios where change feed replay on restart is acceptable
/// </remarks>
public class InMemoryChangeFeedCheckpointStore : IChangeFeedCheckpointStore
{
    private readonly ConcurrentDictionary<string, string> _tokens = new();

    /// <inheritdoc />
    public Task<string?> GetTokenAsync(
        string databaseId,
        string containerId,
        CancellationToken cancellationToken = default)
    {
        var key = GetKey(databaseId, containerId);
        _tokens.TryGetValue(key, out var token);
        return Task.FromResult(token);
    }

    /// <inheritdoc />
    public Task SaveTokenAsync(
        string databaseId,
        string containerId,
        string token,
        CancellationToken cancellationToken = default)
    {
        var key = GetKey(databaseId, containerId);
        _tokens[key] = token;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task DeleteTokenAsync(
        string databaseId,
        string containerId,
        CancellationToken cancellationToken = default)
    {
        var key = GetKey(databaseId, containerId);
        _tokens.TryRemove(key, out _);
        return Task.CompletedTask;
    }

    /// <summary>
    ///     Gets all stored checkpoint keys for debugging purposes.
    /// </summary>
    /// <returns>A collection of stored checkpoint keys.</returns>
    public IReadOnlyCollection<string> GetStoredKeys()
    {
        return [.. _tokens.Keys];
    }

    /// <summary>
    ///     Clears all stored checkpoints.
    /// </summary>
    public void Clear()
    {
        _tokens.Clear();
    }

    private static string GetKey(string databaseId, string containerId)
    {
        return $"{databaseId}|{containerId}";
    }
}

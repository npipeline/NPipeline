using System.Collections.Concurrent;

namespace NPipeline.Execution.CircuitBreaking;

/// <summary>
///     Tracks the last access time for circuit breakers to enable memory management cleanup.
///     Provides thread-safe tracking of circuit breaker usage patterns.
/// </summary>
internal sealed class CircuitBreakerTracker : IDisposable
{
    private readonly ConcurrentDictionary<string, DateTime> _lastAccessTimes = new();
    private readonly object _lockObject = new();

    /// <summary>
    ///     Releases all resources used by the CircuitBreakerTracker.
    /// </summary>
    public void Dispose()
    {
        _lastAccessTimes.Clear();
    }

    /// <summary>
    ///     Updates the last access time for the specified circuit breaker.
    /// </summary>
    /// <param name="nodeId">The unique identifier for the node.</param>
    public void UpdateAccessTime(string nodeId)
    {
        ArgumentNullException.ThrowIfNull(nodeId);
        var now = DateTime.UtcNow;
        _ = _lastAccessTimes.AddOrUpdate(nodeId, now, (_, _) => now);
    }

    /// <summary>
    ///     Gets the last access time for the specified circuit breaker.
    /// </summary>
    /// <param name="nodeId">The unique identifier for the node.</param>
    /// <returns>The last access time, or null if the circuit breaker is not tracked.</returns>
    public DateTime? GetLastAccessTime(string nodeId)
    {
        ArgumentNullException.ThrowIfNull(nodeId);

        return _lastAccessTimes.TryGetValue(nodeId, out var lastAccess)
            ? lastAccess
            : null;
    }

    /// <summary>
    ///     Removes the tracking entry for the specified circuit breaker.
    /// </summary>
    /// <param name="nodeId">The unique identifier for the node.</param>
    /// <returns>True if the entry was removed, false if it didn't exist.</returns>
    public bool RemoveTracking(string nodeId)
    {
        ArgumentNullException.ThrowIfNull(nodeId);
        return _lastAccessTimes.TryRemove(nodeId, out _);
    }

    /// <summary>
    ///     Gets all circuit breaker IDs that haven't been accessed within the specified threshold.
    /// </summary>
    /// <param name="inactivityThreshold">The inactivity threshold.</param>
    /// <returns>A collection of node IDs that are inactive.</returns>
    public ICollection<string> GetInactiveCircuitBreakers(TimeSpan inactivityThreshold)
    {
        if (inactivityThreshold <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(inactivityThreshold), "Inactivity threshold must be positive.");

        var cutoffTime = DateTime.UtcNow - inactivityThreshold;
        var inactiveIds = new List<string>();

        lock (_lockObject)
        {
            foreach (var kvp in _lastAccessTimes)
            {
                if (kvp.Value < cutoffTime)
                    inactiveIds.Add(kvp.Key);
            }
        }

        return inactiveIds;
    }

    /// <summary>
    ///     Attempts to retrieve the least-recently-used circuit breaker entry.
    /// </summary>
    /// <param name="nodeId">The node identifier of the least recently accessed circuit breaker.</param>
    /// <param name="lastAccess">The timestamp of the last recorded access.</param>
    /// <returns>True if an entry was found; otherwise, false.</returns>
    public bool TryGetLeastRecentlyUsedNode(out string nodeId, out DateTime lastAccess)
    {
        nodeId = string.Empty;
        lastAccess = default;

        lock (_lockObject)
        {
            var found = false;

            foreach (var kvp in _lastAccessTimes)
            {
                if (!found || kvp.Value < lastAccess)
                {
                    nodeId = kvp.Key;
                    lastAccess = kvp.Value;
                    found = true;
                }
            }

            return found;
        }
    }

    /// <summary>
    ///     Gets the total count of tracked circuit breakers.
    /// </summary>
    /// <returns>The number of tracked circuit breakers.</returns>
    public int GetTrackedCount()
    {
        return _lastAccessTimes.Count;
    }
}

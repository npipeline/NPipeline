using System.Collections.Concurrent;
using System.Diagnostics;
using NPipeline.Execution.Plans;
using NPipeline.Graph;

namespace NPipeline.Execution.Caching;

/// <summary>
///     In-memory implementation of <see cref="IPipelineExecutionPlanCache" /> using a concurrent dictionary.
///     This cache persists for the lifetime of the application domain and is thread-safe.
/// </summary>
/// <remarks>
///     <para>
///         This implementation uses a composite cache key based on:
///         - Pipeline definition type
///         - The node properties consumed when execution plans are built
///     </para>
///     <para>
///         The cache has a maximum size of 100 entries. When the limit is reached,
///         the least recently used entry is evicted using an approximate LRU algorithm based on timestamps.
///         For applications with many dynamic pipeline definitions,
///         consider implementing a custom cache with different eviction policies or using a distributed cache.
///     </para>
/// </remarks>
public sealed class InMemoryPipelineExecutionPlanCache : IPipelineExecutionPlanCache
{
    private const int MaxCacheSize = 100;
    private readonly ConcurrentDictionary<PipelineExecutionPlanCacheKey, CacheEntry> _cache = new();
    private readonly object _evictionLock = new();

    /// <inheritdoc />
    public bool TryGetCachedPlans(
        Type pipelineDefinitionType,
        PipelineGraph graph,
        out Dictionary<string, NodeExecutionPlan>? cachedPlans)
    {
        ArgumentNullException.ThrowIfNull(pipelineDefinitionType);
        ArgumentNullException.ThrowIfNull(graph);

        var cacheKey = GenerateCacheKey(pipelineDefinitionType, graph);

        if (_cache.TryGetValue(cacheKey, out var entry))
        {
            Volatile.Write(ref entry.LastAccess, Stopwatch.GetTimestamp());

            cachedPlans = entry.Plans;
            return true;
        }

        cachedPlans = null;
        return false;
    }

    /// <inheritdoc />
    public void CachePlans(
        Type pipelineDefinitionType,
        PipelineGraph graph,
        Dictionary<string, NodeExecutionPlan> plans)
    {
        ArgumentNullException.ThrowIfNull(pipelineDefinitionType);
        ArgumentNullException.ThrowIfNull(graph);
        ArgumentNullException.ThrowIfNull(plans);

        var cacheKey = GenerateCacheKey(pipelineDefinitionType, graph);

        // Store a copy to prevent external modifications from cached data
        var entry = new CacheEntry(new Dictionary<string, NodeExecutionPlan>(plans), Stopwatch.GetTimestamp());

        lock (_evictionLock)
        {
            if (_cache.ContainsKey(cacheKey))
            {
                _cache[cacheKey] = entry;
                return;
            }

            if (_cache.Count >= MaxCacheSize)
                EvictOldestEntry();

            _cache.TryAdd(cacheKey, entry);
        }
    }

    /// <inheritdoc />
    public void Clear()
    {
        _cache.Clear();
    }

    /// <inheritdoc />
    public int Count => _cache.Count;

    /// <summary>
    ///     Evicts the entry with the oldest LastAccess timestamp.
    ///     Must be called while holding _evictionLock.
    /// </summary>
    private void EvictOldestEntry()
    {
        PipelineExecutionPlanCacheKey? oldestKey = null;
        var oldestTimestamp = long.MaxValue;

        // Find the entry with the oldest timestamp
        foreach (var kvp in _cache)
        {
            var lastAccess = Volatile.Read(ref kvp.Value.LastAccess);

            if (lastAccess < oldestTimestamp)
            {
                oldestTimestamp = lastAccess;
                oldestKey = kvp.Key;
            }
        }

        // Remove the oldest entry
        if (oldestKey is { } key)
            _cache.TryRemove(key, out _);
    }

    /// <summary>
    ///     Generates a cache key from the pipeline definition type and the graph properties plans are built from.
    /// </summary>
    /// <remarks>
    ///     See <see cref="PipelineExecutionPlanCacheKey" /> for why the key is derived from the node definitions
    ///     directly rather than from a hash of the graph as a whole.
    /// </remarks>
    private static PipelineExecutionPlanCacheKey GenerateCacheKey(Type pipelineDefinitionType, PipelineGraph graph) =>
        PipelineExecutionPlanCacheKey.Create(pipelineDefinitionType, graph);

    private sealed class CacheEntry(Dictionary<string, NodeExecutionPlan> plans, long lastAccess)
    {
        public readonly Dictionary<string, NodeExecutionPlan> Plans = plans;
        public long LastAccess = lastAccess;
    }
}

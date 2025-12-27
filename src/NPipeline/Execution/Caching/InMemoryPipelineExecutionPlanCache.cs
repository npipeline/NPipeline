using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
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
///         - Pipeline definition type name
///         - Graph structure hash (nodes, edges, and node types)
///     </para>
///     <para>
///         The cache has a maximum size of 1000 entries. When the limit is reached,
///         the least recently used entry is evicted. For applications with many dynamic pipeline definitions,
///         consider implementing a custom cache with different eviction policies or using a distributed cache.
///     </para>
/// </remarks>
public sealed class InMemoryPipelineExecutionPlanCache : IPipelineExecutionPlanCache
{
    private const int MaxCacheSize = 1000;
    private readonly ConcurrentDictionary<string, (Dictionary<string, NodeExecutionPlan> Plans, LinkedListNode<string> LruNode)> _cache = new();
    private readonly LinkedList<string> _lruList = new();
    private readonly object _lruLock = new();

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
            // Update LRU order
            lock (_lruLock)
            {
                if (entry.LruNode != null)
                {
                    _lruList.Remove(entry.LruNode);
                    _lruList.AddFirst(entry.LruNode);
                }
            }

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

        // Store a copy to prevent external modifications from affecting cached data
        var plansCopy = new Dictionary<string, NodeExecutionPlan>(plans);

        lock (_lruLock)
        {
            // Evict if at capacity
            while (_cache.Count >= MaxCacheSize && _lruList.Count > 0)
            {
                var lruKey = _lruList.Last?.Value;

                if (lruKey is not null)
                {
                    _lruList.RemoveLast();
                    _cache.TryRemove(lruKey, out _);
                }
            }

            // Add to LRU list
            var lruNode = _lruList.AddFirst(cacheKey);

            // Store in cache with LRU node reference
            _cache[cacheKey] = (plansCopy, lruNode);
        }
    }

    /// <inheritdoc />
    public void Clear()
    {
        lock (_lruLock)
        {
            _cache.Clear();
            _lruList.Clear();
        }
    }

    /// <inheritdoc />
    public int Count => _cache.Count;

    /// <summary>
    ///     Generates a cache key based on pipeline definition type and graph structure.
    /// </summary>
    /// <remarks>
    ///     The cache key includes:
    ///     - Pipeline definition type full name
    ///     - Hash of node definitions (ID, type, input/output types)
    ///     - Hash of edge connections
    ///     This ensures that structurally identical pipelines share cached plans.
    /// </remarks>
    private static string GenerateCacheKey(Type pipelineDefinitionType, PipelineGraph graph)
    {
        var sb = new StringBuilder();

        // Include pipeline definition type
        _ = sb.Append(pipelineDefinitionType.FullName ?? pipelineDefinitionType.Name);
        _ = sb.Append('|');

        // Create a stable hash of the graph structure
        _ = sb.Append(ComputeGraphHash(graph));

        return sb.ToString();
    }

    /// <summary>
    ///     Computes a stable hash of the pipeline graph structure.
    /// </summary>
    /// <remarks>
    ///     The hash includes all structural elements that affect execution plan compilation:
    ///     - Node IDs, types, input types, output types
    ///     - Edge connections (source and target node IDs)
    ///     - Execution strategies (if specified at definition time)
    ///     Changes to any of these elements will result in a different hash and cache miss.
    /// </remarks>
    private static string ComputeGraphHash(PipelineGraph graph)
    {
        var sb = new StringBuilder();

        // Sort nodes by ID for stable hashing
        foreach (var node in graph.Nodes.OrderBy(n => n.Id))
        {
            _ = sb.Append(node.Id);
            _ = sb.Append(':');
            _ = sb.Append(node.NodeType.FullName ?? node.NodeType.Name);
            _ = sb.Append(':');
            _ = sb.Append(node.InputType?.FullName ?? "null");
            _ = sb.Append(':');
            _ = sb.Append(node.OutputType?.FullName ?? "null");
            _ = sb.Append(':');
            _ = sb.Append(node.ExecutionStrategy?.GetType().FullName ?? "null");
            _ = sb.Append(';');
        }

        _ = sb.Append('|');

        // Sort edges for stable hashing
        foreach (var edge in graph.Edges.OrderBy(e => e.SourceNodeId).ThenBy(e => e.TargetNodeId))
        {
            _ = sb.Append(edge.SourceNodeId);
            _ = sb.Append("->");
            _ = sb.Append(edge.TargetNodeId);
            _ = sb.Append(';');
        }

        // Use SHA256 to create a fixed-length hash
        var hashBytes = SHA256.HashData(Encoding.UTF8.GetBytes(sb.ToString()));
        return Convert.ToBase64String(hashBytes);
    }
}

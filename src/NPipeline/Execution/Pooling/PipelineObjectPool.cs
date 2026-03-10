using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using NPipeline.DataFlow;
using NPipeline.Nodes;

namespace NPipeline.Execution.Pooling;

/// <summary>
///     Provides object pooling for common allocations in pipeline execution to reduce GC pressure.
/// </summary>
/// <remarks>
///     <para>
///         <strong>Performance Benefits:</strong>
///     </para>
///     <list type="bullet">
///         <item>
///             <description>Reduces allocation overhead by ~30-80μs per pipeline run</description>
///         </item>
///         <item>
///             <description>Decreases GC pressure by reusing temporary collections</description>
///         </item>
///         <item>
///             <description>Improves cache locality through object reuse</description>
///         </item>
///     </list>
///     <para>
///         <strong>Thread Safety:</strong>
///         All pool operations are thread-safe and can be called concurrently from multiple threads.
///     </para>
/// </remarks>
public static class PipelineObjectPool
{
    // Maximum capacity threshold - don't pool collections that have grown too large
    private const int MaxPooledCapacity = 100;
    private const int MaxRetainedPerPool = 32;

    // Lightweight object pools using ConcurrentBag for thread-safe pooling
    private static readonly BoundedConcurrentBag<List<string>> StringListPool = new(MaxRetainedPerPool);
    private static readonly BoundedConcurrentBag<Dictionary<string, int>> StringIntDictionaryPool = new(MaxRetainedPerPool);
    private static readonly BoundedConcurrentBag<Dictionary<string, object>> StringObjectDictionaryPool = new(MaxRetainedPerPool);
    private static readonly BoundedConcurrentBag<Dictionary<string, INode>> NodeDictionaryPool = new(MaxRetainedPerPool);
    private static readonly BoundedConcurrentBag<Dictionary<string, IDataStream?>> NodeOutputDictionaryPool = new(MaxRetainedPerPool);
    private static readonly BoundedConcurrentBag<Queue<string>> StringQueuePool = new(MaxRetainedPerPool);
    private static readonly BoundedConcurrentBag<HashSet<string>> StringHashSetPool = new(MaxRetainedPerPool);

    // Generic list pool using ConcurrentDictionary to maintain type-specific pools
    private static readonly ConcurrentDictionary<Type, object> GenericListPools = new();

    /// <summary>
    ///     Rents a List&lt;string&gt; from the pool. Must be returned via <see cref="Return(List{string})" />.
    /// </summary>
    /// <remarks>
    ///     The list will be empty when returned from the pool.
    ///     Typical usage is for storing node IDs during topological sort operations.
    /// </remarks>
    /// <returns>A List&lt;string&gt; instance from the pool.</returns>
    public static List<string> RentStringList()
    {
        if (StringListPool.TryTake(out var list))
            return list;

        // Pool is empty, create a new instance pre-sized for typical pipeline node count
        return new List<string>(10);
    }

    /// <summary>
    ///     Returns a List&lt;string&gt; to the pool for reuse.
    /// </summary>
    /// <param name="list">The list to return. It will be cleared before being returned to the pool.</param>
    public static void Return(List<string> list)
    {
        // Don't pool lists that have grown too large to avoid holding excessive memory
        if (list.Capacity > MaxPooledCapacity)
            return;

        list.Clear();
        _ = StringListPool.TryAdd(list);
    }

    /// <summary>
    ///     Rents a List&lt;T&gt; from the pool. Must be returned via <see cref="Return{T}(List{T})" />.
    /// </summary>
    /// <remarks>
    ///     The list will be empty when returned from the pool.
    ///     Typical usage is for batching operations in async enumerable extensions.
    /// </remarks>
    /// <param name="capacityHint">The initial capacity hint for the list.</param>
    /// <returns>A List&lt;T&gt; instance from the pool.</returns>
    public static List<T> RentList<T>(int capacityHint = 10)
    {
        var pool = (BoundedConcurrentBag<List<T>>)GenericListPools.GetOrAdd(typeof(T), _ => new BoundedConcurrentBag<List<T>>(MaxRetainedPerPool));

        if (pool.TryTake(out var list))
        {
            list.EnsureCapacity(capacityHint);
            return list;
        }

        return new List<T>(capacityHint);
    }

    /// <summary>
    ///     Returns a List&lt;T&gt; to the pool for reuse.
    /// </summary>
    /// <param name="list">The list to return. It will be cleared before being returned to the pool.</param>
    public static void Return<T>(List<T> list)
    {
        // Don't pool lists that have grown too large to avoid holding excessive memory
        if (list.Capacity > MaxPooledCapacity)
            return;

        list.Clear();
        var pool = (BoundedConcurrentBag<List<T>>)GenericListPools.GetOrAdd(typeof(T), _ => new BoundedConcurrentBag<List<T>>(MaxRetainedPerPool));
        _ = pool.TryAdd(list);
    }

    /// <summary>
    ///     Rents a Dictionary&lt;string, int&gt; from the pool. Must be returned via <see cref="Return(Dictionary{string, int})" />.
    /// </summary>
    /// <remarks>
    ///     The dictionary will be empty when returned from the pool.
    ///     Typical usage is for in-degree tracking during topological sort operations.
    /// </remarks>
    /// <returns>A Dictionary&lt;string, int&gt; instance from the pool.</returns>
    public static Dictionary<string, int> RentStringIntDictionary()
    {
        if (StringIntDictionaryPool.TryTake(out var dictionary))
            return dictionary;

        // Pool is empty, create a new instance pre-sized for typical pipeline node count
        return new Dictionary<string, int>(10);
    }

    /// <summary>
    ///     Returns a Dictionary&lt;string, int&gt; to the pool for reuse.
    /// </summary>
    /// <param name="dictionary">The dictionary to return. It will be cleared before being returned to the pool.</param>
    public static void Return(Dictionary<string, int> dictionary)
    {
        // Don't pool dictionaries that have grown too large
        if (dictionary.Count > MaxPooledCapacity)
            return;

        dictionary.Clear();
        _ = StringIntDictionaryPool.TryAdd(dictionary);
    }

    /// <summary>
    ///     Rents a Dictionary&lt;string, object&gt; from the pool for context state.
    /// </summary>
    public static Dictionary<string, object> RentStringObjectDictionary(int capacityHint = 10)
    {
        if (StringObjectDictionaryPool.TryTake(out var dictionary))
        {
            dictionary.EnsureCapacity(capacityHint);
            return dictionary;
        }

        return new Dictionary<string, object>(capacityHint);
    }

    /// <summary>
    ///     Returns a Dictionary&lt;string, object&gt; to the pool for reuse.
    /// </summary>
    public static void Return(Dictionary<string, object> dictionary)
    {
        if (dictionary.Count > MaxPooledCapacity)
            return;

        dictionary.Clear();
        _ = StringObjectDictionaryPool.TryAdd(dictionary);
    }

    /// <summary>
    ///     Rents a Dictionary&lt;string, INode&gt; for node instantiation maps.
    /// </summary>
    public static Dictionary<string, INode> RentNodeDictionary(int capacityHint = 10)
    {
        if (NodeDictionaryPool.TryTake(out var dictionary))
        {
            dictionary.EnsureCapacity(capacityHint);
            return dictionary;
        }

        return new Dictionary<string, INode>(capacityHint);
    }

    /// <summary>
    ///     Returns a Dictionary&lt;string, INode&gt; to the pool.
    /// </summary>
    public static void Return(Dictionary<string, INode> dictionary)
    {
        if (dictionary.Count > MaxPooledCapacity)
            return;

        dictionary.Clear();
        _ = NodeDictionaryPool.TryAdd(dictionary);
    }

    /// <summary>
    ///     Rents a Dictionary&lt;string, IDataStream?&gt; for node output tracking.
    /// </summary>
    public static Dictionary<string, IDataStream?> RentNodeOutputDictionary(int capacityHint = 10)
    {
        if (NodeOutputDictionaryPool.TryTake(out var dictionary))
        {
            dictionary.EnsureCapacity(capacityHint);
            return dictionary;
        }

        return new Dictionary<string, IDataStream?>(capacityHint);
    }

    /// <summary>
    ///     Returns a Dictionary&lt;string, IDataStream?&gt; to the pool.
    /// </summary>
    public static void Return(Dictionary<string, IDataStream?> dictionary)
    {
        if (dictionary.Count > MaxPooledCapacity)
            return;

        dictionary.Clear();
        _ = NodeOutputDictionaryPool.TryAdd(dictionary);
    }

    /// <summary>
    ///     Rents a Queue&lt;string&gt; from the pool. Must be returned via <see cref="Return(Queue{string})" />.
    /// </summary>
    /// <remarks>
    ///     The queue will be empty when returned from the pool.
    ///     Typical usage is for BFS-style graph traversal during topological sort operations.
    /// </remarks>
    /// <returns>A Queue&lt;string&gt; instance from the pool.</returns>
    public static Queue<string> RentStringQueue()
    {
        if (StringQueuePool.TryTake(out var queue))
            return queue;

        // Pool is empty, create a new instance
        return new Queue<string>();
    }

    /// <summary>
    ///     Returns a Queue&lt;string&gt; to the pool for reuse.
    /// </summary>
    /// <param name="queue">The queue to return. It will be cleared before being returned to the pool.</param>
    public static void Return(Queue<string> queue)
    {
        // Don't pool queues that have grown too large
        if (queue.Count > MaxPooledCapacity)
            return;

        queue.Clear();
        _ = StringQueuePool.TryAdd(queue);
    }

    /// <summary>
    ///     Rents a HashSet&lt;string&gt; from the pool. Must be returned via <see cref="Return(HashSet{string})" />.
    /// </summary>
    /// <remarks>
    ///     The set will be empty when returned from the pool.
    ///     Typical usage is for tracking visited nodes or detecting duplicates.
    /// </remarks>
    /// <returns>A HashSet&lt;string&gt; instance from the pool.</returns>
    public static HashSet<string> RentStringHashSet()
    {
        if (StringHashSetPool.TryTake(out var set))
            return set;

        // Pool is empty, create a new instance pre-sized for typical pipeline node count
        return new HashSet<string>(10);
    }

    /// <summary>
    ///     Returns a HashSet&lt;string&gt; to the pool for reuse.
    /// </summary>
    /// <param name="set">The set to return. It will be cleared before being returned to the pool.</param>
    public static void Return(HashSet<string> set)
    {
        // Don't pool sets that have grown too large
        if (set.Count > MaxPooledCapacity)
            return;

        set.Clear();
        _ = StringHashSetPool.TryAdd(set);
    }

    private sealed class BoundedConcurrentBag<T>(int maxRetained)
    {
        private readonly ConcurrentBag<T> _items = new();
        private readonly int _maxRetained = maxRetained;
        private int _retainedCount;

        public bool TryTake([MaybeNullWhen(false)] out T item)
        {
            if (_items.TryTake(out item))
            {
                _ = Interlocked.Decrement(ref _retainedCount);
                return true;
            }

            item = default;
            return false;
        }

        public bool TryAdd(T item)
        {
            var newCount = Interlocked.Increment(ref _retainedCount);

            if (newCount > _maxRetained)
            {
                _ = Interlocked.Decrement(ref _retainedCount);
                return false;
            }

            _items.Add(item);
            return true;
        }
    }
}

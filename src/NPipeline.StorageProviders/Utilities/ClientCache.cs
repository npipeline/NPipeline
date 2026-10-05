using System.Collections.Concurrent;

namespace NPipeline.StorageProviders.Utilities;

/// <summary>
///     A bounded, thread-safe cache of SDK clients, keyed by the endpoint a client talks to. A cache hit is one dictionary
///     lookup. When the cache grows past its limit, the least recently used client is dropped from it.
/// </summary>
/// <remarks>
///     The key must hold routing data only (host, port, account, region) and never a secret, or a value derived from one.
///     Credentials belong to the provider's options, so every client a provider creates uses the same ones.
///     An evicted client is not disposed, because an operation may still be using it; <see cref="Dispose" /> disposes the
///     clients still in the cache.
/// </remarks>
/// <typeparam name="TKey">The endpoint key type, usually a <c>readonly record struct</c>.</typeparam>
/// <typeparam name="TClient">The client type.</typeparam>
public sealed class ClientCache<TKey, TClient> : IDisposable
    where TKey : notnull
{
    private readonly ConcurrentDictionary<TKey, Entry> _entries = new();
    private readonly object _gate = new();
    private readonly int _limit;
    private bool _disposed;
    private long _tick;

    /// <summary>Creates the cache.</summary>
    /// <param name="limit">The most clients kept. Must be positive.</param>
    public ClientCache(int limit)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        _limit = limit;
    }

    /// <summary>Gets the number of cached clients.</summary>
    public int Count => _entries.Count;

    /// <summary>Gets the cached client for <paramref name="key" />, creating it with <paramref name="create" /> on a miss.</summary>
    /// <param name="key">The endpoint key.</param>
    /// <param name="create">Creates the client. Called at most once per miss, and not concurrently for different keys.</param>
    /// <returns>The client.</returns>
    public TClient GetOrCreate(TKey key, Func<TKey, TClient> create)
    {
        ArgumentNullException.ThrowIfNull(create);
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_entries.TryGetValue(key, out var hit))
        {
            hit.LastUsed = Interlocked.Increment(ref _tick);
            return hit.Client;
        }

        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            // Another caller may have created it while this one waited for the lock.
            if (_entries.TryGetValue(key, out hit))
            {
                hit.LastUsed = Interlocked.Increment(ref _tick);
                return hit.Client;
            }

            var entry = new Entry(create(key)) { LastUsed = Interlocked.Increment(ref _tick) };
            _entries[key] = entry;

            while (_entries.Count > _limit)
            {
                var oldest = default(KeyValuePair<TKey, Entry>);
                var oldestTick = long.MaxValue;

                foreach (var candidate in _entries)
                {
                    if (candidate.Value.LastUsed < oldestTick && !EqualityComparer<TKey>.Default.Equals(candidate.Key, key))
                    {
                        oldest = candidate;
                        oldestTick = candidate.Value.LastUsed;
                    }
                }

                if (oldestTick == long.MaxValue)
                    break;

                _ = _entries.TryRemove(oldest.Key, out _);
            }

            return entry.Client;
        }
    }

    /// <summary>Empties the cache without disposing the clients, which operations may still be using.</summary>
    public void Clear()
    {
        lock (_gate)
        {
            _entries.Clear();
        }
    }

    /// <summary>Disposes every cached client that is <see cref="IDisposable" /> and empties the cache.</summary>
    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed)
                return;

            _disposed = true;

            foreach (var entry in _entries.Values)
            {
                (entry.Client as IDisposable)?.Dispose();
            }

            _entries.Clear();
        }
    }

    private sealed class Entry(TClient client)
    {
        public TClient Client { get; } = client;
        public long LastUsed;
    }
}

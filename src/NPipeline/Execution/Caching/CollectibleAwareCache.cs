using System.Collections.Concurrent;

namespace NPipeline.Execution.Caching;

/// <summary>
///     Process-wide caches keyed on user types. A type from a collectible <see cref="System.Runtime.Loader.AssemblyLoadContext" />
///     is never cached: a static cache entry would keep its assembly loaded for the life of the process.
/// </summary>
internal static class CollectibleAwareCache
{
    private static volatile bool s_bypass;

    /// <summary>
    ///     When true, every lookup builds a fresh value, as before the caches existed. Benchmark-only: it measures what
    ///     the caches save. Never set in production.
    /// </summary>
    internal static bool Bypass
    {
        get => s_bypass;
        set => s_bypass = value;
    }

    public static TValue GetOrAdd<TKey, TValue>(ConcurrentDictionary<TKey, TValue> cache, TKey key, bool collectible, Func<TKey, TValue> factory)
        where TKey : notnull =>
        collectible || s_bypass
            ? factory(key)
            : cache.GetOrAdd(key, factory);

    public static TValue GetOrAdd<TKey, TValue, TArg>(
        ConcurrentDictionary<TKey, TValue> cache,
        TKey key,
        bool collectible,
        Func<TKey, TArg, TValue> factory,
        TArg factoryArgument)
        where TKey : notnull =>
        collectible || s_bypass
            ? factory(key, factoryArgument)
            : cache.GetOrAdd(key, factory, factoryArgument);
}

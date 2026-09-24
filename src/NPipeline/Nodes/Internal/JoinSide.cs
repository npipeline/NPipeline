using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace NPipeline.Nodes.Internal;

/// <summary>
///     The retained items for one side of a streaming hash join, grouped by key.
/// </summary>
/// <remarks>
///     <para>
///         Every retained item stays available for matching against items that arrive later on the other side, so a key that
///         occurs several times on both sides produces every pairing. The first item for a key is stored inline, so keys that
///         occur once allocate nothing beyond the dictionary entry.
///     </para>
///     <para>
///         Outer joins need to know which items never matched. Within a key, the matched items are always the oldest ones: an
///         item from the other side matches every item retained for the key so far, and once the other side retains an item
///         for the key, every later item on this side matches on arrival. Each key therefore tracks a matched count instead of
///         a flag per item.
///     </para>
/// </remarks>
/// <typeparam name="TKey">The join key type.</typeparam>
/// <typeparam name="TItem">The type of the items on this side.</typeparam>
internal sealed class JoinSide<TKey, TItem> where TKey : notnull
{
    private readonly Dictionary<TKey, Bucket> _buckets = [];

    /// <summary>
    ///     Gets the total number of retained items across all keys.
    /// </summary>
    public int Count { get; private set; }

    /// <summary>
    ///     Returns the items retained for <paramref name="key" /> and marks them all as matched.
    /// </summary>
    public bool TryMatch(TKey key, out Matches matches)
    {
        ref var bucket = ref CollectionsMarshal.GetValueRefOrNullRef(_buckets, key);

        if (Unsafe.IsNullRef(ref bucket))
        {
            matches = default;
            return false;
        }

        bucket.MatchedCount = bucket.Count;
        matches = new Matches(bucket.First, bucket.Rest);
        return true;
    }

    public void Add(TKey key, TItem item, bool hasMatched)
    {
        ref var bucket = ref CollectionsMarshal.GetValueRefOrAddDefault(_buckets, key, out var exists);

        if (exists)
        {
            (bucket.Rest ??= []).Add(item);
        }
        else
        {
            bucket.First = item;
        }

        bucket.Count++;
        Count++;

        if (hasMatched)
        {
            Debug.Assert(bucket.MatchedCount == bucket.Count - 1, "Matched items must be the oldest items retained for a key.");
            bucket.MatchedCount = bucket.Count;
        }
    }

    /// <summary>
    ///     Returns the retained items that were never matched.
    /// </summary>
    public IEnumerable<TItem> Unmatched()
    {
        foreach (var bucket in _buckets.Values)
        {
            for (var i = bucket.MatchedCount; i < bucket.Count; i++)
            {
                yield return bucket[i];
            }
        }
    }

    public void Clear()
    {
        _buckets.Clear();
        Count = 0;
    }

    /// <summary>
    ///     The items retained for a key when it was matched, oldest first.
    /// </summary>
    internal readonly struct Matches(TItem first, List<TItem>? rest)
    {
        public int Count => 1 + (rest?.Count ?? 0);

        public TItem this[int index] => index == 0 ? first : rest![index - 1];
    }

    private struct Bucket
    {
        public TItem First;
        public List<TItem>? Rest;
        public int Count;
        public int MatchedCount;

        public readonly TItem this[int index] => index == 0 ? First : Rest![index - 1];
    }
}

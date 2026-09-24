using System.Collections;
using NPipeline.DataFlow;

namespace NPipeline.Execution.Orchestration;

/// <summary>
///     Lock-guarded view over the run's node-output map, used while terminal nodes drain concurrently.
/// </summary>
/// <remarks>
///     The underlying map is a plain <see cref="Dictionary{TKey,TValue}" /> rented from the pipeline object pool, so it
///     tolerates neither concurrent writes nor a read racing a write. Terminal nodes read their upstream outputs and
///     record their own, so every access goes through the supplied gate - the same gate the execution stage uses for
///     the shared context flags. Access happens once per node, not per item, so the lock is not on a hot path.
/// </remarks>
internal sealed class SynchronizedNodeOutputs(IDictionary<string, IDataStream?> inner, object gate)
    : IDictionary<string, IDataStream?>
{
    private Dictionary<string, IDataStream?>? _detachedSnapshot;

    private IDictionary<string, IDataStream?> Target => _detachedSnapshot ?? inner;

    public IDataStream? this[string key]
    {
        get
        {
            lock (gate)
            {
                return Target[key];
            }
        }
        set
        {
            lock (gate)
            {
                Target[key] = value;
            }
        }
    }

    public int Count
    {
        get
        {
            lock (gate)
            {
                return Target.Count;
            }
        }
    }

    public bool IsReadOnly => false;

    public ICollection<string> Keys
    {
        get
        {
            lock (gate)
            {
                return [.. Target.Keys];
            }
        }
    }

    public ICollection<IDataStream?> Values
    {
        get
        {
            lock (gate)
            {
                return [.. Target.Values];
            }
        }
    }

    public void Add(string key, IDataStream? value)
    {
        lock (gate)
        {
            Target.Add(key, value);
        }
    }

    public void Add(KeyValuePair<string, IDataStream?> item)
    {
        lock (gate)
        {
            Target.Add(item);
        }
    }

    public void Clear()
    {
        lock (gate)
        {
            Target.Clear();
        }
    }

    public bool Contains(KeyValuePair<string, IDataStream?> item)
    {
        lock (gate)
        {
            return Target.Contains(item);
        }
    }

    public bool ContainsKey(string key)
    {
        lock (gate)
        {
            return Target.ContainsKey(key);
        }
    }

    public void CopyTo(KeyValuePair<string, IDataStream?>[] array, int arrayIndex)
    {
        lock (gate)
        {
            Target.CopyTo(array, arrayIndex);
        }
    }

    public bool Remove(string key)
    {
        lock (gate)
        {
            return Target.Remove(key);
        }
    }

    public bool Remove(KeyValuePair<string, IDataStream?> item)
    {
        lock (gate)
        {
            return Target.Remove(item);
        }
    }

    public bool TryGetValue(string key, out IDataStream? value)
    {
        lock (gate)
        {
            return Target.TryGetValue(key, out value);
        }
    }

    /// <summary>
    ///     Enumerates a snapshot taken under the gate, so iteration cannot observe a concurrent write.
    /// </summary>
    public IEnumerator<KeyValuePair<string, IDataStream?>> GetEnumerator()
    {
        List<KeyValuePair<string, IDataStream?>> snapshot;

        lock (gate)
        {
            snapshot = [.. Target];
        }

        return snapshot.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <summary>
    ///     Hands the underlying map back to the caller and serves everything from a snapshot afterwards.
    /// </summary>
    /// <remarks>
    ///     Used when the drain gives up on terminals that a stalled pump may never release: the run is already failing
    ///     and cleanup is about to iterate and dispose the map, so a straggler must not still be writing into it. After
    ///     this call the stragglers read a frozen copy and their writes are discarded.
    /// </remarks>
    public void DetachFromInner()
    {
        lock (gate)
        {
            _detachedSnapshot ??= new Dictionary<string, IDataStream?>(inner, StringComparer.Ordinal);
        }
    }
}

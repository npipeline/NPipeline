using System.Collections;

namespace NPipeline.Pipeline;

/// <summary>
///     Wraps a caller-supplied dictionary so that every access is serialized, giving the thread-safety guarantee the
///     <see cref="Configuration.PipelineOptimizationProfile.Default" /> profile documents without taking ownership of
///     the caller's instance.
/// </summary>
/// <remarks>
///     <see cref="PipelineContext" /> uses this for the <c>Items</c> and <c>Properties</c> dictionaries when a caller
///     supplies a non-concurrent instance, because existing behaviour expects the caller to observe entries written
///     through the context. Reads and writes are serialized on a single lock, which is enough for the dictionary's own
///     consistency.
/// </remarks>
internal sealed class SynchronizedDictionary : IDictionary<string, object>
{
    private readonly IDictionary<string, object> _inner;

    public SynchronizedDictionary(IDictionary<string, object> inner)
    {
        ArgumentNullException.ThrowIfNull(inner);
        _inner = inner;
    }

    public object this[string key]
    {
        get
        {
            lock (_inner)
            {
                return _inner[key];
            }
        }
        set
        {
            lock (_inner)
            {
                _inner[key] = value;
            }
        }
    }

    public ICollection<string> Keys
    {
        get
        {
            lock (_inner)
            {
                return [.. _inner.Keys];
            }
        }
    }

    public ICollection<object> Values
    {
        get
        {
            lock (_inner)
            {
                return [.. _inner.Values];
            }
        }
    }

    public int Count
    {
        get
        {
            lock (_inner)
            {
                return _inner.Count;
            }
        }
    }

    public bool IsReadOnly => _inner.IsReadOnly;

    public void Add(string key, object value)
    {
        lock (_inner)
        {
            _inner.Add(key, value);
        }
    }

    public bool ContainsKey(string key)
    {
        lock (_inner)
        {
            return _inner.ContainsKey(key);
        }
    }

    public bool Remove(string key)
    {
        lock (_inner)
        {
            return _inner.Remove(key);
        }
    }

    public bool TryGetValue(string key, out object value)
    {
        lock (_inner)
        {
            return _inner.TryGetValue(key, out value!);
        }
    }

    public void Add(KeyValuePair<string, object> item)
    {
        lock (_inner)
        {
            _inner.Add(item);
        }
    }

    public void Clear()
    {
        lock (_inner)
        {
            _inner.Clear();
        }
    }

    public bool Contains(KeyValuePair<string, object> item)
    {
        lock (_inner)
        {
            return _inner.Contains(item);
        }
    }

    public void CopyTo(KeyValuePair<string, object>[] array, int arrayIndex)
    {
        lock (_inner)
        {
            _inner.CopyTo(array, arrayIndex);
        }
    }

    public bool Remove(KeyValuePair<string, object> item)
    {
        lock (_inner)
        {
            return _inner.Remove(item);
        }
    }

    public IEnumerator<KeyValuePair<string, object>> GetEnumerator()
    {
        lock (_inner)
        {
            return _inner.ToList().GetEnumerator();
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

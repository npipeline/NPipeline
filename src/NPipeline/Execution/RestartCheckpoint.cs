namespace NPipeline.Execution;

/// <summary>
///     The point a restarted node resumes from: the index of the first input item whose outcome has not been
///     delivered. Reported by an <see cref="IResumableExecutionStrategy" /> as items are delivered.
/// </summary>
/// <remarks>
///     The checkpoint only moves forward. It is safe to report from several threads.
/// </remarks>
public sealed class RestartCheckpoint
{
    private readonly object _gate = new();
    private readonly Action<long>? _advanced;
    private HashSet<long>? _completedAhead;
    private long _watermark;

    /// <summary>
    ///     Creates a checkpoint that starts at <paramref name="start" />.
    /// </summary>
    /// <param name="start">The index of the first item not yet delivered.</param>
    public RestartCheckpoint(long start = 0) : this(start, null)
    {
    }

    internal RestartCheckpoint(long start, Action<long>? advanced)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(start);
        _watermark = start;
        _advanced = advanced;
    }

    /// <summary>
    ///     The index of the first input item whose outcome has not been delivered.
    /// </summary>
    public long Watermark => Volatile.Read(ref _watermark);

    /// <summary>
    ///     Reports that every item below <paramref name="watermark" /> has been delivered. A strategy that delivers in
    ///     input order calls this after each item, with the item's index plus one.
    /// </summary>
    /// <param name="watermark">The index of the first item not yet delivered. A value at or below the current one is ignored.</param>
    public void Advance(long watermark)
    {
        lock (_gate)
        {
            if (watermark <= _watermark)
                return;

            _watermark = watermark;

            if (_completedAhead is { Count: > 0 })
            {
                _ = _completedAhead.RemoveWhere(index => index < watermark);
                AdvanceOverCompleted();
            }

            _advanced?.Invoke(_watermark);
        }
    }

    /// <summary>
    ///     Reports that the item at <paramref name="index" /> has been delivered, in any order. The checkpoint moves
    ///     once every item below it has been reported.
    /// </summary>
    /// <param name="index">The item's index in the node's input.</param>
    public void Complete(long index)
    {
        lock (_gate)
        {
            if (index < _watermark)
                return;

            if (index > _watermark)
            {
                _ = (_completedAhead ??= []).Add(index);
                return;
            }

            _watermark++;
            AdvanceOverCompleted();
            _advanced?.Invoke(_watermark);
        }
    }

    private void AdvanceOverCompleted()
    {
        while (_completedAhead is { Count: > 0 } && _completedAhead.Remove(_watermark))
        {
            _watermark++;
        }
    }
}

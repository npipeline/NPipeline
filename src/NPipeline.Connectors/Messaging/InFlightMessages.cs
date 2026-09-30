namespace NPipeline.Connectors.Messaging;

/// <summary>
///     Counts the messages a source has handed on and that are not yet settled, so the source can keep its channel or
///     consumer open until they are: settling a message needs the connection it was received on, and a sink downstream
///     may still be writing after the source's read has ended.
/// </summary>
public sealed class InFlightMessages
{
    private readonly object _lock = new();
    private int _count;
    private TaskCompletionSource? _changed;
    private TaskCompletionSource? _idle;

    /// <summary>The number of messages handed on and not settled.</summary>
    public int Count => Volatile.Read(ref _count);

    /// <summary>Counts a message handed on.</summary>
    public void Add() => Interlocked.Increment(ref _count);

    /// <summary>Counts a message settled.</summary>
    public void Remove()
    {
        var count = Interlocked.Decrement(ref _count);

        lock (_lock)
        {
            _changed?.TrySetResult();
            _changed = null;

            if (count <= 0)
                _idle?.TrySetResult();
        }
    }

    /// <summary>Completes once fewer than <paramref name="max" /> messages are in flight, so a source can bound what it holds.</summary>
    public async Task WaitForRoomAsync(int max, CancellationToken cancellationToken = default)
    {
        while (true)
        {
            Task changed;

            lock (_lock)
            {
                if (Count < max)
                    return;

                _changed ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                changed = _changed.Task;
            }

            await changed.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Completes when every message is settled, or when <paramref name="timeout" /> passes; returns whether all were.</summary>
    public async Task<bool> WhenSettledAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        Task idle;

        lock (_lock)
        {
            if (Count <= 0)
                return true;

            _idle ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            idle = _idle.Task;
        }

        try
        {
            await idle.WaitAsync(timeout, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (TimeoutException)
        {
            return false;
        }
    }
}

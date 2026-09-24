using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;

namespace NPipeline.Execution.Strategies;

/// <summary>
///     A node's input, read once and numbered, that a restart can reopen at its checkpoint.
/// </summary>
/// <remarks>
///     <para>
///         Only the items between the checkpoint and the read head are held: they have been read but their outcome has
///         not been delivered, so a restart must process them again. Items are released as the checkpoint advances.
///         Nothing is read ahead of the consumer, so the input streams, and an input that never ends works.
///     </para>
///     <para>
///         Once <c>maxRetained</c> items are held, reading stops until the checkpoint advances. The bound is
///         backpressure, never an error.
///     </para>
///     <para>
///         Each <see cref="Open" /> starts a new generation. An enumeration from an earlier generation, such as a
///         parallel strategy's feeder that is still winding down after its run failed, ends at its next read instead
///         of taking items from the current one.
///     </para>
/// </remarks>
internal sealed class ResumableInput<T> : IAsyncDisposable
{
    private readonly object _gate = new();
    private readonly int _maxRetained;

    // Serializes reads from the source enumerator, which supports only one MoveNextAsync at a time.
    private readonly SemaphoreSlim _readLock = new(1, 1);
    private readonly IDataStream<T> _source;
    private readonly CancellationToken _sourceToken;

    // Items from _released to _readHead - 1; the item at index i is _retained[_head + (i - _released)].
    private readonly List<T> _retained = [];
    private TaskCompletionSource _changed = NewSignal();

    // Whether a reader is waiting on _changed. Signalling is needed only then, so the common path allocates nothing.
    private bool _changeAwaited;
    private bool _closed;
    private bool _drained;
    private Exception? _fault;
    private int _generation;
    private int _head;
    private long _readHead;
    private long _released;
    private IAsyncEnumerator<T>? _sourceEnumerator;

    /// <param name="source">The node's input.</param>
    /// <param name="maxRetained">The most items held for replay.</param>
    /// <param name="sourceToken">
    ///     The token the input is read with. It must outlive every generation, so it is the node's token, not the
    ///     token of any one enumeration.
    /// </param>
    public ResumableInput(IDataStream<T> source, int maxRetained, CancellationToken sourceToken)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxRetained);

        _source = source;
        _maxRetained = maxRetained;
        _sourceToken = sourceToken;
    }

    /// <summary>
    ///     The failure of the input itself, if reading it threw. A restart cannot recover from it: the input cannot
    ///     be read again.
    /// </summary>
    public Exception? InputFault
    {
        get
        {
            lock (_gate)
            {
                return _fault;
            }
        }
    }

    /// <summary>
    ///     Opens the input at the current checkpoint, ending every earlier enumeration.
    /// </summary>
    /// <param name="offset">The index of the first item the returned input yields.</param>
    /// <returns>The input from the checkpoint, and the checkpoint to report delivered items to.</returns>
    public (IDataStream<T> Input, RestartCheckpoint Checkpoint) Open(out long offset)
    {
        int generation;

        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_closed, this);

            generation = ++_generation;
            offset = _released;
            Signal();
        }

        var checkpoint = new RestartCheckpoint(offset, watermark => Release(generation, watermark));
        return (new DataStream<T>(ReadAsync(generation, offset), _source.StreamName), checkpoint);
    }

    /// <summary>
    ///     Ends every enumeration and disposes the input's enumerator.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        lock (_gate)
        {
            if (_closed)
                return;

            _closed = true;
            _generation++;
            Signal();
        }

        // A read in progress keeps the lock; that reader disposes the enumerator when its read returns.
        if (!_readLock.Wait(0))
            return;

        try
        {
            await DisposeSourceAsync().ConfigureAwait(false);
        }
        finally
        {
            _ = _readLock.Release();
        }
    }

    private async IAsyncEnumerable<T> ReadAsync(int generation, long position, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        while (true)
        {
            Task? waitFor = null;
            var available = false;
            T item = default!;

            lock (_gate)
            {
                if (generation != _generation)
                    yield break;

                if (position < _released)
                    throw new InvalidOperationException($"The restart checkpoint ({_released}) moved past an item that was not read yet ({position}).");

                if (position < _readHead)
                {
                    item = _retained[_head + (int)(position - _released)];
                    available = true;
                }
                else if (_fault is not null)
                    ExceptionDispatchInfo.Throw(_fault);
                else if (_drained)
                    yield break;
                else if (_readHead - _released >= _maxRetained)
                {
                    waitFor = _changed.Task;
                    _changeAwaited = true;
                }
            }

            if (available)
            {
                position++;
                yield return item;
                continue;
            }

            if (waitFor is not null)
            {
                // The replay window is full: wait for the checkpoint to advance (or for a restart).
                await waitFor.WaitAsync(cancellationToken).ConfigureAwait(false);
                continue;
            }

            await ReadNextAsync(generation, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    ///     Reads one item from the input into the retained window, unless another reader got there first.
    /// </summary>
    private async Task ReadNextAsync(int generation, CancellationToken cancellationToken)
    {
        await _readLock.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            lock (_gate)
            {
                // Re-checked under the read lock: while this reader waited, another may have read the item, failed,
                // drained the input, filled the window, or been superseded.
                if (generation != _generation || _closed || _fault is not null || _drained || _readHead - _released >= _maxRetained)
                    return;
            }

            _sourceEnumerator ??= _source.GetAsyncEnumerator(_sourceToken);

            bool hasItem;

            try
            {
                hasItem = await _sourceEnumerator.MoveNextAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                lock (_gate)
                {
                    _fault = ex;
                    Signal();
                }

                throw;
            }

            lock (_gate)
            {
                if (hasItem)
                {
                    _retained.Add(_sourceEnumerator.Current);
                    _readHead++;
                }
                else
                    _drained = true;

                Signal();
            }
        }
        finally
        {
            bool closed;

            lock (_gate)
            {
                closed = _closed;
            }

            if (closed)
                await DisposeSourceAsync().ConfigureAwait(false);

            _ = _readLock.Release();
        }
    }

    /// <summary>
    ///     Releases the items below <paramref name="watermark" />, which have been delivered and will not be replayed.
    /// </summary>
    private void Release(int generation, long watermark)
    {
        lock (_gate)
        {
            // A superseded run's reports are ignored: the current run resumed from an earlier checkpoint and is
            // processing those items again.
            if (generation != _generation || watermark <= _released)
                return;

            if (watermark > _readHead)
                throw new InvalidOperationException($"The restart checkpoint ({watermark}) moved past the last item read ({_readHead}).");

            var count = (int)(watermark - _released);
            _head += count;
            _released = watermark;

            // Compact once the released prefix outweighs what is held, so the list does not grow with the input.
            if (_head > 32 && _head >= _retained.Count - _head)
            {
                _retained.RemoveRange(0, _head);
                _head = 0;
            }

            Signal();
        }
    }

    private async ValueTask DisposeSourceAsync()
    {
        var enumerator = _sourceEnumerator;
        _sourceEnumerator = null;

        if (enumerator is not null)
            await enumerator.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    ///     Wakes the readers waiting for a change. Called under <c>_gate</c>.
    /// </summary>
    private void Signal()
    {
        if (!_changeAwaited)
            return;

        _changeAwaited = false;
        var previous = _changed;
        _changed = NewSignal();
        _ = previous.TrySetResult();
    }

    private static TaskCompletionSource NewSignal()
    {
        return new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}

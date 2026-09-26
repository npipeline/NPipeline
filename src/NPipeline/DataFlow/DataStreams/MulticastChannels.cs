using System.Runtime.CompilerServices;
using System.Threading.Channels;
using NPipeline.DataFlow.Branching;

namespace NPipeline.DataFlow.DataStreams;

/// <summary>
///     The per-subscriber channels behind a multicast stream: one channel per subscriber, fed by the stream's pump.
/// </summary>
/// <remarks>
///     <para>
///         The pump feeds every subscriber in lockstep, so a subscriber that stops reading would fill its buffer and
///         block the pump, stalling every sibling. A channel is therefore abandoned (the pump skips it and whatever it
///         holds is discarded) as soon as nobody will read it again:
///     </para>
///     <list type="bullet">
///         <item>a subscriber that started reading is disposed before the channel completed;</item>
///         <item>a subscriber claimed by index is disposed before its first read;</item>
///         <item>the owner of an edge releases the edge, once the node consuming it can no longer read it.</item>
///     </list>
///     <para>
///         An edge subscriber disposed before its first read does not abandon its channel: node retry re-executes the
///         consuming node while it has read nothing, and the next attempt claims the same channel again. Each claim
///         bumps the channel's generation, so a superseded subscriber cannot start reading.
///     </para>
/// </remarks>
internal sealed class MulticastChannels<T>
{
    private readonly int[] _abandoned;
    private readonly Channel<T>[] _channels;
    private readonly int[] _generation;
    private readonly BranchMetrics _metrics;
    private readonly object _owner;
    private readonly int[] _readStarted;
    private readonly string _streamName;
    private volatile bool _disposed;
    private int _itemsSinceSample;

    public MulticastChannels(object owner, string streamName, int count, int? perSubscriberBuffer, BranchMetrics metrics)
    {
        _owner = owner;
        _streamName = streamName;
        _metrics = metrics;
        _channels = new Channel<T>[count];
        _abandoned = new int[count];
        _generation = new int[count];
        _readStarted = new int[count];

        metrics.SetSubscriberCount(count);
        metrics.EnsurePerSubscriberArrays();

        for (var i = 0; i < count; i++)
        {
            _channels[i] = perSubscriberBuffer is { } cap and > 0
                ? Channel.CreateBounded<T>(new BoundedChannelOptions(cap)
                {
                    SingleReader = true,
                    SingleWriter = true,
                    FullMode = BoundedChannelFullMode.Wait,
                })
                : Channel.CreateUnbounded<T>(new UnboundedChannelOptions
                {
                    // The single-reader unbounded channel cannot report its Count, which backlog sampling needs.
                    SingleReader = false,
                    SingleWriter = true,
                });
        }

        if (perSubscriberBuffer.HasValue)
            metrics.SetPerSubscriberCapacity(perSubscriberBuffer.Value);
    }

    public int Count => _channels.Length;

    /// <summary>
    ///     Stops new claims. Called by the owning stream when it is disposed.
    /// </summary>
    public void MarkDisposed() => _disposed = true;

    /// <summary>
    ///     Writes one item to one channel, synchronously whenever the buffer has room. Abandoned channels are skipped.
    /// </summary>
    public ValueTask WriteAsync(int index, T item, CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _abandoned[index]) != 0)
            return ValueTask.CompletedTask;

        var writer = _channels[index].Writer;

        return writer.TryWrite(item)
            ? ValueTask.CompletedTask
            : writer.WriteAsync(item, cancellationToken);
    }

    /// <summary>
    ///     Completes every channel, with <paramref name="error" /> when the pump failed.
    /// </summary>
    public void CompleteAll(Exception? error = null)
    {
        foreach (var channel in _channels)
            _ = channel.Writer.TryComplete(error);
    }

    /// <summary>
    ///     Claims a channel for reading.
    /// </summary>
    /// <param name="index">The channel to claim.</param>
    /// <param name="abandonIfUnstarted">
    ///     Whether disposing the subscriber before its first read abandons the channel. True for subscribers claimed by
    ///     index, which have no other release path; false for edge views, which their owner releases.
    /// </param>
    /// <param name="cancellationToken">The token the subscriber observes while waiting for items.</param>
    /// <exception cref="InvalidOperationException">The channel was already read or released.</exception>
    public IAsyncEnumerator<T> Claim(int index, bool abandonIfUnstarted, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, _owner);

        if (Volatile.Read(ref _readStarted[index]) != 0 || Volatile.Read(ref _abandoned[index]) != 0)
            throw AlreadyConsumed(index);

        var generation = Interlocked.Increment(ref _generation[index]);
        return new Subscriber(this, index, generation, abandonIfUnstarted, cancellationToken);
    }

    /// <summary>
    ///     Creates the view a single edge reads. Disposing the view does not release its channel; see
    ///     <see cref="Abandon" />.
    /// </summary>
    public IForwardOnlyDataStream<T> CreateEdgeView(int index) => new EdgeView(this, index);

    /// <summary>
    ///     Marks a channel as no longer read: the pump skips it, later claims fail, and anything still buffered is
    ///     discarded. Idempotent.
    /// </summary>
    public void Abandon(int index)
    {
        if (Interlocked.Exchange(ref _abandoned[index], 1) != 0)
            return;

        var reader = _channels[index].Reader;

        // A channel its subscriber drained to completion holds nothing, so there is nothing to discard.
        if (!reader.Completion.IsCompleted)
            _ = Task.Run(() => DiscardAsync(reader), CancellationToken.None);
    }

    /// <summary>
    ///     Samples backlog from the channels every 64 items instead of doing per-item, per-subscriber bookkeeping, which
    ///     used to contend the pump and consumer threads on the same cache line.
    /// </summary>
    public void SampleBacklog(bool force = false)
    {
        if (!force && (++_itemsSinceSample & 63) != 0)
            return;

        var aggregate = 0;

        for (var i = 0; i < _channels.Length; i++)
        {
            var reader = _channels[i].Reader;

            if (!reader.CanCount)
                continue;

            var pending = reader.Count;
            _metrics.ObservePerSubscriberPending(i, pending);
            aggregate += pending;
        }

        _metrics.ObservePending(aggregate);
    }

    private static async Task DiscardAsync(ChannelReader<T> reader)
    {
        try
        {
            await foreach (var _ in reader.ReadAllAsync(CancellationToken.None).ConfigureAwait(false))
            {
                // Discard: nobody reads this channel any more; the items only exist to unblock the pump.
            }
        }
        catch
        {
            // The pump completed the channel with a fault; there is nothing left to discard.
        }
    }

    private InvalidOperationException AlreadyConsumed(int index) =>
        new($"Channel {index} on stream '{_streamName}' has already been consumed.");

    private void BeginRead(int index, int generation)
    {
        if (Volatile.Read(ref _generation[index]) != generation || Interlocked.CompareExchange(ref _readStarted[index], 1, 0) != 0)
            throw AlreadyConsumed(index);
    }

    /// <summary>
    ///     Hand-written enumerator over a subscriber's channel. Unlike a compiler-generated async iterator, its
    ///     <see cref="DisposeAsync" /> always runs, even before the first <see cref="MoveNextAsync" />, and an item
    ///     already buffered is returned without an async state machine.
    /// </summary>
    private sealed class Subscriber(
        MulticastChannels<T> owner,
        int index,
        int generation,
        bool abandonIfUnstarted,
        CancellationToken cancellationToken) : IAsyncEnumerator<T>
    {
        private readonly ChannelReader<T> _reader = owner._channels[index].Reader;
        private bool _completed;
        private bool _started;

        public T Current { get; private set; } = default!;

        public ValueTask<bool> MoveNextAsync()
        {
            if (_completed)
                return new ValueTask<bool>(false);

            if (!_started)
            {
                owner.BeginRead(index, generation);
                _started = true;
            }

            if (_reader.TryRead(out var item))
            {
                Current = item;
                return new ValueTask<bool>(true);
            }

            return WaitAndReadAsync();
        }

        public ValueTask DisposeAsync()
        {
            if (!_completed && (_started || abandonIfUnstarted))
                owner.Abandon(index);

            _completed = true;
            return ValueTask.CompletedTask;
        }

        private async ValueTask<bool> WaitAndReadAsync()
        {
            while (await _reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                if (_reader.TryRead(out var item))
                {
                    Current = item;
                    return true;
                }
            }

            _completed = true;
            owner._metrics.MarkSubscriberCompleted();
            return false;
        }
    }

    private sealed class EdgeView(MulticastChannels<T> owner, int index) : IForwardOnlyDataStream<T>
    {
        public string StreamName => $"{owner._streamName}_edge{index}";

        public Type GetDataType() => typeof(T);

        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
            owner.Claim(index, abandonIfUnstarted: false, cancellationToken);

        public async IAsyncEnumerable<object?> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await foreach (var item in this.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }
        }

        // The channel outlives the view: node retry reads it again through a new view. The execution stage releases
        // the edge once the consuming node can no longer read it.
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}

using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow.Branching;
using NPipeline.DataFlow.Routing;
using NPipeline.Graph;
using NPipeline.Observability.Logging;

namespace NPipeline.DataFlow.DataStreams;

/// <summary>
///     Multicast data pipe that integrates counting directly into the pump.
///     This eliminates one layer of wrapping by combining counting and multicasting.
/// </summary>
internal sealed class CountingMulticastDataStream<T> : IForwardOnlyDataStream<T>, IHasBranchMetrics, IEdgeRoutedDataStream
{
    private static readonly TimeSpan PumpShutdownTimeout = TimeSpan.FromSeconds(30);

    private readonly int[] _abandonedChannels;
    private readonly int[] _channelTaken;
    private readonly Channel<T>[] _channels;
    private readonly StatsCounter _counter;
    private readonly CancellationTokenSource _cts = new();
    private readonly Dictionary<Edge, int>? _edgeToChannel;
    private readonly ILogger _logger;
    private readonly Task _pumpTask;
    private readonly IDataStream<T> _source;
    private int _disposedFlag;
    private int _itemsSinceSample;
    private int _nextSubscriber;

    public CountingMulticastDataStream(
        IDataStream<T> source,
        StatsCounter counter,
        int subscriberCount,
        int? perSubscriberBuffer,
        BranchMetrics metrics,
        IReadOnlyList<Edge>? subscriberEdges = null,
        ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(counter);
        ArgumentNullException.ThrowIfNull(metrics);

        _source = source;
        _counter = counter;
        _logger = logger ?? NullLogger.Instance;
        _channels = new Channel<T>[subscriberCount];
        Metrics = metrics;
        _channelTaken = new int[subscriberCount];
        _abandonedChannels = new int[subscriberCount];

        if (subscriberEdges is not null)
        {
            _edgeToChannel = new Dictionary<Edge, int>(subscriberEdges.Count);

            for (var i = 0; i < subscriberEdges.Count; i++)
                _edgeToChannel[subscriberEdges[i]] = i;
        }

        Metrics.SetSubscriberCount(subscriberCount);
        Metrics.EnsurePerSubscriberArrays();

        for (var i = 0; i < subscriberCount; i++)
        {
            _channels[i] = perSubscriberBuffer is { } cap and > 0
                ? Channel.CreateBounded<T>(new BoundedChannelOptions(cap)
                {
                    SingleReader = true,
                    SingleWriter = false,
                    FullMode = BoundedChannelFullMode.Wait,
                })
                : Channel.CreateUnbounded<T>(new UnboundedChannelOptions
                {
                    SingleReader = true,
                    SingleWriter = false,
                });
        }

        _pumpTask = Task.Run(PumpAsync, CancellationToken.None);

        if (perSubscriberBuffer.HasValue)
            Metrics.SetPerSubscriberCapacity(perSubscriberBuffer.Value);
    }

    public string StreamName => $"CountedMulticast_{_source.StreamName}";

    public Type GetDataType() => typeof(T);

    public IDataStream GetEdgeView(Edge edge)
    {
        ArgumentNullException.ThrowIfNull(edge);

        if (_edgeToChannel is null || !_edgeToChannel.TryGetValue(edge, out var channelIndex))
        {
            throw new InvalidOperationException(
                $"Edge '{edge.SourceNodeId}->{edge.TargetNodeId}' (output='{edge.SourceOutputName ?? "<default>"}') was not registered for stream '{StreamName}'.");
        }

        return new EdgeView(this, channelIndex);
    }

    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposedFlag) != 0, this);

        var idx = Interlocked.Increment(ref _nextSubscriber) - 1;

        if (idx >= _channels.Length)
        {
            throw new InvalidOperationException(
                $"Too many subscribers requested (max {_channels.Length}). " +
                $"Stream: {StreamName}. Subscriber #{idx + 1}.");
        }

        return ClaimChannel(idx, cancellationToken);
    }

    public async IAsyncEnumerable<object?> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposedFlag) != 0, this);

        await foreach (var item in this.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return item;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposedFlag, 1) != 0)
            return;

        await _cts.CancelAsync().ConfigureAwait(false);

        try
        {
            await _pumpTask.WaitAsync(PumpShutdownTimeout).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            DataStreamLog.MulticastPumpShutdownTimedOut(_logger, StreamName, PumpShutdownTimeout);
        }

        _cts.Dispose();
        await _source.DisposeAsync().ConfigureAwait(false);
    }

    public BranchMetrics Metrics { get; }

    /// <summary>
    ///     Claims a channel for reading, allowing re-claiming while no item has been read yet (needed so an
    ///     edge view and the underlying subscriber slot can share a channel index).
    /// </summary>
    internal IAsyncEnumerator<T> ClaimChannel(int channelIndex, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposedFlag) != 0, this);

        if (Interlocked.Exchange(ref _channelTaken[channelIndex], 1) != 0)
        {
            throw new InvalidOperationException(
                $"Channel {channelIndex} on stream '{StreamName}' has already been consumed.");
        }

        return new ChannelSubscriber(this, channelIndex, cancellationToken);
    }

    /// <summary>
    ///     Releases a channel that a caller took but never (or not yet) enumerated, so an edge view whose owner
    ///     never called <see cref="IAsyncEnumerable{T}.GetAsyncEnumerator"/> does not stall its siblings.
    /// </summary>
    internal void ReleaseChannel(int channelIndex)
    {
        AbandonChannel(channelIndex);
    }

    private async Task PumpAsync()
    {
        // The pump is the only writer, so the count is accumulated locally and folded into the shared
        // counter once, keeping the per-item path free of atomics and of cross-node cache-line contention.
        var counted = 0L;

        try
        {
            // Enumerate source and count + multicast in single pass
            await foreach (var item in _source.WithCancellation(_cts.Token))
            {
                // Count once per item (before broadcasting)
                counted++;

                // Broadcast to all subscribers that are still reading.
                for (var i = 0; i < _channels.Length; i++)
                {
                    // A subscriber that stopped early would otherwise fill its buffer and block the pump,
                    // stalling every sibling. Its channel is drained and discarded by AbandonChannel.
                    if (Volatile.Read(ref _abandonedChannels[i]) != 0)
                        continue;

                    var writer = _channels[i].Writer;

                    // TryWrite succeeds whenever the buffer has room, which keeps the common path allocation-free.
                    if (!writer.TryWrite(item))
                        await writer.WriteAsync(item, _cts.Token).ConfigureAwait(false);
                }

                SampleBacklog();
            }

            // Always take one final sample so a short-lived stream (fewer than the sampling interval's worth
            // of items) still reports an accurate backlog instead of never sampling at all.
            SampleBacklog(force: true);

            // Complete all channels successfully
            foreach (var ch in _channels)
            {
                _ = ch.Writer.TryComplete();
            }
        }
        catch (OperationCanceledException) when (_cts.IsCancellationRequested)
        {
            // Disposal cancelled the pump; this is a normal shutdown, not a fault.
            foreach (var ch in _channels)
            {
                _ = ch.Writer.TryComplete();
            }
        }
        catch (Exception ex)
        {
            // Propagate exception to all subscribers
            foreach (var ch in _channels)
            {
                _ = ch.Writer.TryComplete(ex);
            }

            Metrics.MarkFault();
        }
        finally
        {
            _counter.Add(counted);
        }
    }

    /// <summary>
    ///     Samples backlog from the channels themselves every 64 items instead of doing per-item, per-subscriber
    ///     Interlocked bookkeeping, which used to contend the pump and consumer threads on the same cache line.
    /// </summary>
    private void SampleBacklog(bool force = false)
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
            Metrics.ObservePerSubscriberPending(i, pending);
            aggregate += pending;
        }

        Metrics.ObservePending(aggregate);
    }

    /// <summary>
    ///     Marks a subscriber as no longer reading and discards whatever it leaves behind.
    /// </summary>
    /// <remarks>
    ///     The pump feeds every subscriber in lockstep, so a reader that stops early would fill its buffer and block
    ///     the pump forever, stalling subscribers that are still consuming. Discarding the abandoned channel keeps the
    ///     rest of the fan-out running.
    /// </remarks>
    private void AbandonChannel(int channelIndex)
    {
        if (Interlocked.Exchange(ref _abandonedChannels[channelIndex], 1) != 0)
            return;

        var reader = _channels[channelIndex].Reader;

        _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var _ in reader.ReadAllAsync(CancellationToken.None).ConfigureAwait(false))
                {
                    // Discard: this subscriber is gone, the items only exist to unblock the pump.
                }
            }
            catch
            {
                // The pump completed the channel with a fault; there is nothing left to discard.
            }
        });
    }

    /// <summary>
    ///     Hand-written enumerator over a subscriber's channel. Unlike a compiler-generated async iterator, its
    ///     <see cref="DisposeAsync"/> always runs the abandon logic, even when it is disposed before the first
    ///     <see cref="MoveNextAsync"/> call - the case an async-iterator's not-started disposal would otherwise skip.
    /// </summary>
    private sealed class ChannelSubscriber(CountingMulticastDataStream<T> owner, int index, CancellationToken ct) : IAsyncEnumerator<T>
    {
        private readonly ChannelReader<T> _reader = owner._channels[index].Reader;
        private bool _completed;

        public T Current { get; private set; } = default!;

        public async ValueTask<bool> MoveNextAsync()
        {
            while (true)
            {
                if (_reader.TryRead(out var item))
                {
                    Current = item;
                    return true;
                }

                if (!await _reader.WaitToReadAsync(ct).ConfigureAwait(false))
                {
                    _completed = true;
                    owner.Metrics.MarkSubscriberCompleted();
                    return false;
                }
            }
        }

        public ValueTask DisposeAsync()
        {
            if (!_completed)
                owner.AbandonChannel(index);

            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    ///     Edge-specific view returned by <see cref="GetEdgeView"/>. Releases its channel on disposal even if it was
    ///     never enumerated, so a sink/aggregate/join node that does not read its input does not stall its siblings.
    /// </summary>
    private sealed class EdgeView(CountingMulticastDataStream<T> owner, int channelIndex) : IForwardOnlyDataStream<T>
    {
        public string StreamName => $"{owner.StreamName}_edge{channelIndex}";

        public Type GetDataType() => typeof(T);

        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
            owner.ClaimChannel(channelIndex, cancellationToken);

        public async IAsyncEnumerable<object?> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await foreach (var item in this.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }
        }

        public ValueTask DisposeAsync()
        {
            owner.ReleaseChannel(channelIndex);
            return ValueTask.CompletedTask;
        }
    }
}

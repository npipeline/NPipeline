using System.Runtime.CompilerServices;
using System.Threading.Channels;
using NPipeline.DataFlow.Branching;

namespace NPipeline.DataFlow.DataStreams;

/// <summary>
///     Multicast data pipe that integrates counting directly into the pump.
///     This eliminates one layer of wrapping by combining counting and multicasting.
/// </summary>
internal sealed class CountingMulticastDataStream<T> : IForwardOnlyDataStream<T>, IHasBranchMetrics
{
    private readonly Channel<T>[] _channels;
    private readonly StatsCounter _counter;
    private readonly CancellationTokenSource _cts = new();
    private readonly int[] _abandonedChannels;
    private readonly int[] _pendingPerChannel;
    private readonly Task _pumpTask;
    private readonly IDataStream<T> _source;
    private bool _disposed;
    private int _nextSubscriber;

    public CountingMulticastDataStream(
        IDataStream<T> source,
        StatsCounter counter,
        int subscriberCount,
        int? perSubscriberBuffer,
        BranchMetrics metrics)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(counter);
        ArgumentNullException.ThrowIfNull(metrics);

        _source = source;
        _counter = counter;
        _channels = new Channel<T>[subscriberCount];
        Metrics = metrics;
        _pendingPerChannel = new int[subscriberCount];
        _abandonedChannels = new int[subscriberCount];

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

    public Type GetDataType()
    {
        return typeof(T);
    }

    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var idx = Interlocked.Increment(ref _nextSubscriber) - 1;

        if (idx >= _channels.Length)
        {
            throw new InvalidOperationException(
                $"Too many subscribers requested (max {_channels.Length}). " +
                $"Stream: {StreamName}. Subscriber #{idx + 1}.");
        }

        return ReadChannel(_channels[idx], idx, cancellationToken);
    }

    public async IAsyncEnumerable<object?> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await using var enumerator = GetAsyncEnumerator(cancellationToken);

        while (await enumerator.MoveNextAsync())
        {
            yield return enumerator.Current;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        await _cts.CancelAsync();

        await _pumpTask.ConfigureAwait(false);

        _cts.Dispose();
        await _source.DisposeAsync().ConfigureAwait(false);
    }

    public BranchMetrics Metrics { get; }

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
                var aggregatePending = 0;

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

                    var pending = Interlocked.Increment(ref _pendingPerChannel[i]);
                    aggregatePending += pending;
                    Metrics.ObservePerSubscriberPending(i, pending);
                }

                Metrics.ObservePending(aggregatePending);
            }

            // Complete all channels successfully
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

    private async IAsyncEnumerator<T> ReadChannel(Channel<T> channel, int channelIndex, CancellationToken ct)
    {
        var drainedToCompletion = false;

        try
        {
            await foreach (var item in channel.Reader.ReadAllAsync(ct).ConfigureAwait(false))
            {
                yield return item;

                var remaining = Interlocked.Decrement(ref _pendingPerChannel[channelIndex]);

                if (remaining < 0)
                    remaining = 0;

                Metrics.ObservePerSubscriberPending(channelIndex, remaining);
            }

            drainedToCompletion = true;
            Metrics.MarkSubscriberCompleted();
        }
        finally
        {
            // Reached on break, on an exception in the consumer, and on cancellation - any case where this
            // subscriber stops before the stream ends.
            if (!drainedToCompletion)
                AbandonChannel(channelIndex);
        }
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
}

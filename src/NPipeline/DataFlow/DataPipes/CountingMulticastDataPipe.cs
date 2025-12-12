using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using NPipeline.DataFlow.Branching;

namespace NPipeline.DataFlow.DataPipes;

/// <summary>
///     Multicast data pipe that integrates counting directly into the pump.
///     This eliminates one layer of wrapping by combining counting and multicasting.
/// </summary>
internal sealed class CountingMulticastDataPipe<T> : IDataPipe<T>, IStreamingDataPipe, IHasBranchMetrics
{
    private readonly Channel<T>[] _channels;
    private readonly StatsCounter _counter;
    private readonly CancellationTokenSource _cts = new();
    private readonly int[] _pendingPerChannel;
    private readonly Task _pumpTask;
    private readonly IDataPipe<T> _source;
    private bool _disposed;
    private int _nextSubscriber;

    public CountingMulticastDataPipe(
        IDataPipe<T> source,
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

        return idx >= _channels.Length
            ? throw new InvalidOperationException($"Too many subscribers requested (max {_channels.Length}).")
            : ReadChannel(_channels[idx], idx, cancellationToken);
    }

    public async IAsyncEnumerable<object?> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await using var enumerator = GetAsyncEnumerator(cancellationToken);

        while (await enumerator.MoveNextAsync())
        {
            if (enumerator.Current is not null)
                yield return enumerator.Current;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        await _cts.CancelAsync();

        try
        {
            await _pumpTask.ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"[CountingMulticastDataPipe] Pump task failed during disposal: {ex.Message}");
        }

        _cts.Dispose();
        await _source.DisposeAsync().ConfigureAwait(false);
    }

    public BranchMetrics Metrics { get; }

    private async Task PumpAsync()
    {
        try
        {
            // Enumerate source and count + multicast in single pass
            await foreach (var item in _source.WithCancellation(_cts.Token))
            {
                // Count once per item (before broadcasting)
                _ = Interlocked.Increment(ref _counter.GetTotalRef());

                // Broadcast to all subscribers
                var writes = new Task[_channels.Length];
                var aggregatePending = 0;

                for (var i = 0; i < _channels.Length; i++)
                {
                    writes[i] = _channels[i].Writer.WriteAsync(item, _cts.Token).AsTask();
                    var pending = Interlocked.Increment(ref _pendingPerChannel[i]);
                    aggregatePending += pending;
                    Metrics.ObservePerSubscriberPending(i, pending);
                }

                await Task.WhenAll(writes).ConfigureAwait(false);
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
    }

    private async IAsyncEnumerator<T> ReadChannel(Channel<T> channel, int channelIndex, CancellationToken ct)
    {
        await foreach (var item in channel.Reader.ReadAllAsync(ct))
        {
            yield return item;

            var remaining = Interlocked.Decrement(ref _pendingPerChannel[channelIndex]);

            if (remaining < 0)
                remaining = 0;

            Metrics.ObservePerSubscriberPending(channelIndex, remaining);
        }

        Metrics.MarkSubscriberCompleted();
    }
}

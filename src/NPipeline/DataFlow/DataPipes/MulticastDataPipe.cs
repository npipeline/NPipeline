using System.Runtime.CompilerServices;
using System.Threading.Channels;
using NPipeline.DataFlow.Branching;

namespace NPipeline.DataFlow.DataPipes;

/// <summary>
///     Simple adapter that wraps an IAsyncEnumerable&lt;T&gt; to implement IDataPipe&lt;T&gt;.
/// </summary>
internal sealed class AsyncEnumerableDataPipe<T>(IAsyncEnumerable<T> source, string streamName) : IStreamingDataPipe<T>
{
    public string StreamName { get; } = streamName;

    public Type GetDataType()
    {
        return typeof(T);
    }

    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        return source.GetAsyncEnumerator(cancellationToken);
    }

    public async IAsyncEnumerable<object?> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in source.WithCancellation(cancellationToken))
        {
            if (item is not null)
                yield return item;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (source is IAsyncDisposable disposable)
            await disposable.DisposeAsync().ConfigureAwait(false);
    }
}

/// <summary>
///     Wraps a single upstream <see cref="IAsyncEnumerable{T}" /> and provides N independent subscribers (branching)
///     ensuring the upstream is enumerated only once. Each subscriber receives every item.
/// </summary>
internal interface IHasBranchMetrics
{
    BranchMetrics Metrics { get; }
}

internal sealed class MulticastDataPipe<T> : DataPipeBase<T>, IHasBranchMetrics
{
    private readonly Channel<T>[] _channels;
    private readonly CancellationTokenSource _cts = new();
    private readonly int[] _pendingPerChannel; // approximate pending counts
    private readonly Task _pumpTask;
    private bool _disposed;
    private int _nextSubscriber;

    private MulticastDataPipe(IAsyncEnumerable<T> source, int subscriberCount, int? perSubscriberBuffer, string streamName, BranchMetrics metrics)
        : base(new AsyncEnumerableDataPipe<T>(source, streamName))
    {
        _channels = new Channel<T>[subscriberCount];
        Metrics = metrics;
        _pendingPerChannel = new int[subscriberCount];
        Metrics.SetSubscriberCount(subscriberCount);
        Metrics.EnsurePerSubscriberArrays();

        for (var i = 0; i < subscriberCount; i++)
        {
            if (perSubscriberBuffer is { } cap and > 0)
            {
                _channels[i] = Channel.CreateBounded<T>(new BoundedChannelOptions(cap)
                {
                    SingleReader = true,
                    SingleWriter = false,
                    FullMode = BoundedChannelFullMode.Wait,
                });
            }
            else
                _channels[i] = Channel.CreateUnbounded<T>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });
        }

        _pumpTask = Task.Run(PumpAsync, CancellationToken.None);

        if (perSubscriberBuffer is { } cap2)
            Metrics.SetPerSubscriberCapacity(cap2);
    }

    public BranchMetrics Metrics { get; }

    public override IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        var idx = Interlocked.Increment(ref _nextSubscriber) - 1;

        if (idx >= _channels.Length)
            throw new InvalidOperationException($"Too many subscribers requested (max {_channels.Length}).");

        return ReadChannel(_channels[idx], cancellationToken);
    }

    public override async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        await _cts.CancelAsync();

        await _pumpTask.ConfigureAwait(false);

        _cts.Dispose();

        // Dispose of base class
        await base.DisposeAsync().ConfigureAwait(false);
    }

    public static MulticastDataPipe<T> Create(IAsyncEnumerable<T> source, int subscriberCount, int? perSubscriberBuffer, string streamName,
        BranchMetrics metrics)
    {
        return new MulticastDataPipe<T>(source, subscriberCount, perSubscriberBuffer, streamName, metrics);
    }

    private async Task PumpAsync()
    {
        try
        {
            await foreach (var item in Inner.WithCancellation(_cts.Token))
            {
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

            foreach (var ch in _channels)
            {
                ch.Writer.TryComplete();
            }
        }
        catch (Exception ex)
        {
            foreach (var ch in _channels)
            {
                ch.Writer.TryComplete(ex);
            }

            Metrics.MarkFault();
        }
    }

    private async IAsyncEnumerator<T> ReadChannel(Channel<T> channel, CancellationToken ct)
    {
        var idx = Array.IndexOf(_channels, channel);

        await foreach (var item in channel.Reader.ReadAllAsync(ct))
        {
            yield return item!;

            if (idx >= 0)
            {
                var remaining = Interlocked.Decrement(ref _pendingPerChannel[idx]);

                if (remaining < 0)
                    remaining = 0;

                Metrics.ObservePerSubscriberPending(idx, remaining); // high-water logic ignores lower values
            }
        }

        Metrics.MarkSubscriberCompleted();

        // When all subscribers complete we could emit tracing tags (not available here without activity),
        // left as a future enhancement.
    }
}

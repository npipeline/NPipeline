using System.Runtime.CompilerServices;
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

    private readonly MulticastChannels<T> _channels;
    private readonly StatsCounter _counter;
    private readonly CancellationTokenSource _cts = new();
    private readonly Dictionary<Edge, int>? _edgeToChannel;
    private readonly ILogger _logger;
    private readonly Task _pumpTask;
    private readonly IDataStream<T> _source;
    private int _disposedFlag;
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
        Metrics = metrics;
        _channels = new MulticastChannels<T>(this, StreamName, subscriberCount, perSubscriberBuffer, metrics);

        if (subscriberEdges is not null)
        {
            _edgeToChannel = new Dictionary<Edge, int>(subscriberEdges.Count);

            for (var i = 0; i < subscriberEdges.Count; i++)
                _edgeToChannel[subscriberEdges[i]] = i;
        }

        _pumpTask = Task.Run(PumpAsync, CancellationToken.None);
    }

    public string StreamName => $"CountedMulticast_{_source.StreamName}";

    public Type GetDataType() => typeof(T);

    public IDataStream GetEdgeView(Edge edge) => _channels.CreateEdgeView(ResolveChannel(edge));

    public void ReleaseEdge(Edge edge) => _channels.Abandon(ResolveChannel(edge));

    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposedFlag) != 0, this);

        var idx = Interlocked.Increment(ref _nextSubscriber) - 1;

        if (idx >= _channels.Count)
        {
            throw new InvalidOperationException(
                $"Too many subscribers requested (max {_channels.Count}). " +
                $"Stream: {StreamName}. Subscriber #{idx + 1}.");
        }

        return _channels.Claim(idx, abandonIfUnstarted: true, cancellationToken);
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

        _channels.MarkDisposed();
        await _cts.CancelAsync().ConfigureAwait(false);

        try
        {
            await _pumpTask.WaitAsync(PumpShutdownTimeout).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            // The source ignores cancellation. Disposing the source while the pump still enumerates it is unsafe, so
            // the disposal is handed to the pump's own completion.
            DataStreamLog.MulticastPumpShutdownTimedOut(_logger, StreamName, PumpShutdownTimeout);
            _ = DisposeResourcesAfterPumpAsync();
            return;
        }

        await DisposeResourcesAsync().ConfigureAwait(false);
    }

    public BranchMetrics Metrics { get; }

    private int ResolveChannel(Edge edge)
    {
        ArgumentNullException.ThrowIfNull(edge);

        if (_edgeToChannel is null || !_edgeToChannel.TryGetValue(edge, out var channelIndex))
        {
            throw new InvalidOperationException(
                $"Edge '{edge.SourceNodeId}->{edge.TargetNodeId}' (output='{edge.SourceOutputName ?? "<default>"}') was not registered for stream '{StreamName}'.");
        }

        return channelIndex;
    }

    private async Task DisposeResourcesAsync()
    {
        _cts.Dispose();
        await _source.DisposeAsync().ConfigureAwait(false);
    }

    private async Task DisposeResourcesAfterPumpAsync()
    {
        try
        {
            await _pumpTask.ConfigureAwait(false); // the pump never faults: failures complete the channels instead
            await DisposeResourcesAsync().ConfigureAwait(false);
        }
        catch
        {
            // Nobody awaits this; a source that fails to dispose after a timed-out shutdown has nowhere to report.
        }
    }

    private async Task PumpAsync()
    {
        // The pump is the only writer, so the count is accumulated locally and folded into the shared
        // counter once, keeping the per-item path free of atomics and of cross-node cache-line contention.
        var counted = 0L;
        var token = _cts.Token;

        try
        {
            await foreach (var item in _source.WithCancellation(token).ConfigureAwait(false))
            {
                counted++;

                // Broadcast to every subscriber that is still reading; abandoned channels are skipped.
                for (var i = 0; i < _channels.Count; i++)
                    await _channels.WriteAsync(i, item, token).ConfigureAwait(false);

                _channels.SampleBacklog();
            }

            // Always take one final sample so a short-lived stream (fewer than the sampling interval's worth
            // of items) still reports an accurate backlog instead of never sampling at all.
            _channels.SampleBacklog(force: true);
            _channels.CompleteAll();
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // Disposal cancelled the pump; this is a normal shutdown, not a fault.
            _channels.CompleteAll();
        }
        catch (Exception ex)
        {
            _channels.CompleteAll(ex);
            Metrics.MarkFault();
        }
        finally
        {
            _counter.Add(counted);
        }
    }
}

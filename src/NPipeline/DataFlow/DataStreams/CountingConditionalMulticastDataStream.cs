using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow.Branching;
using NPipeline.DataFlow.Routing;
using NPipeline.Graph;
using NPipeline.Observability.Logging;

namespace NPipeline.DataFlow.DataStreams;

/// <summary>
///     Multicast stream that integrates counting and conditional routing by named source outputs.
/// </summary>
internal sealed class CountingConditionalMulticastDataStream<T> : IForwardOnlyDataStream<T>, IHasBranchMetrics, IEdgeRoutedDataStream
{
    private static readonly TimeSpan PumpShutdownTimeout = TimeSpan.FromSeconds(30);

    private readonly int[] _abandonedChannels;
    private readonly int[] _channelTaken;
    private readonly Channel<T>[] _channels;
    private readonly StatsCounter _counter;
    private readonly CancellationTokenSource _cts = new();
    private readonly Dictionary<Edge, int> _edgeToChannel;
    private readonly ILogger _logger;
    private readonly Dictionary<string, int[]> _namedOutputChannels;
    private readonly int[]? _otherwiseChannels;
    private readonly RouteOptions<T> _options;
    private readonly bool[] _queuedScratch;
    private readonly int[][] _ruleChannels;
    private readonly Task _pumpTask;
    private readonly IDataStream<T> _source;
    private int _disposedFlag;
    private int _itemsSinceSample;
    private int _nextSubscriber;

    public CountingConditionalMulticastDataStream(
        IDataStream<T> source,
        StatsCounter counter,
        IReadOnlyList<Edge> subscriberEdges,
        int? perSubscriberBuffer,
        RouteOptions<T> options,
        BranchMetrics metrics,
        ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(counter);
        ArgumentNullException.ThrowIfNull(subscriberEdges);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(metrics);

        _source = source;
        _counter = counter;
        _options = options;
        _logger = logger ?? NullLogger.Instance;
        Metrics = metrics;

        _channels = new Channel<T>[subscriberEdges.Count];
        _channelTaken = new int[subscriberEdges.Count];
        _abandonedChannels = new int[subscriberEdges.Count];
        _edgeToChannel = new Dictionary<Edge, int>(subscriberEdges.Count);
        _queuedScratch = new bool[subscriberEdges.Count];

        var namedOutputChannels = new Dictionary<string, List<int>>(StringComparer.Ordinal);

        Metrics.SetSubscriberCount(subscriberEdges.Count);
        Metrics.EnsurePerSubscriberArrays();

        for (var i = 0; i < subscriberEdges.Count; i++)
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

            var edge = subscriberEdges[i];
            _edgeToChannel[edge] = i;

            if (edge.SourceOutputName is null)
                continue;

            if (!namedOutputChannels.TryGetValue(edge.SourceOutputName, out var channelIndexes))
            {
                channelIndexes = [];
                namedOutputChannels[edge.SourceOutputName] = channelIndexes;
            }

            channelIndexes.Add(i);
        }

        _namedOutputChannels = namedOutputChannels.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.ToArray(), StringComparer.Ordinal);

        ValidateConfiguredOutputs();

        _ruleChannels = new int[_options.Rules.Count][];

        for (var r = 0; r < _options.Rules.Count; r++)
        {
            _ruleChannels[r] = _namedOutputChannels.TryGetValue(_options.Rules[r].OutputName, out var channelIndexes)
                ? channelIndexes
                : [];
        }

        _otherwiseChannels = _options.OtherwiseOutputName is { } otherwiseOutputName &&
                              _namedOutputChannels.TryGetValue(otherwiseOutputName, out var otherwiseChannelIndexes)
            ? otherwiseChannelIndexes
            : null;

        _pumpTask = Task.Run(PumpAsync, CancellationToken.None);

        if (perSubscriberBuffer.HasValue)
            Metrics.SetPerSubscriberCapacity(perSubscriberBuffer.Value);
    }

    public IDataStream GetEdgeView(Edge edge)
    {
        ArgumentNullException.ThrowIfNull(edge);

        if (!_edgeToChannel.TryGetValue(edge, out var channelIndex))
        {
            throw new InvalidOperationException(
                $"Edge '{edge.SourceNodeId}->{edge.TargetNodeId}' (output='{edge.SourceOutputName ?? "<default>"}') was not registered for stream '{StreamName}'.");
        }

        return new EdgeView(this, channelIndex);
    }

    public string StreamName => $"CountedConditionalMulticast_{_source.StreamName}";

    public Type GetDataType() => typeof(T);

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

    internal void ReleaseChannel(int channelIndex)
    {
        AbandonChannel(channelIndex);
    }

    private void ValidateConfiguredOutputs()
    {
        foreach (var rule in _options.Rules)
        {
            if (!_namedOutputChannels.ContainsKey(rule.OutputName))
            {
                throw new InvalidOperationException(
                    $"Route output '{rule.OutputName}' is configured but no edge from node output '{rule.OutputName}' exists.");
            }
        }

        if (_options.OtherwiseOutputName is { } otherwiseOutputName && !_namedOutputChannels.ContainsKey(otherwiseOutputName))
        {
            throw new InvalidOperationException(
                $"Otherwise route output '{otherwiseOutputName}' is configured but no edge from node output '{otherwiseOutputName}' exists.");
        }
    }

    private async Task PumpAsync()
    {
        // The pump is the only writer, so the count is accumulated locally and folded into the shared
        // counter once, keeping the per-item path free of atomics and of cross-node cache-line contention.
        var counted = 0L;
        var rules = _options.Rules;

        try
        {
            await foreach (var item in _source.WithCancellation(_cts.Token))
            {
                counted++;

                var matched = false;

                if (_options.MatchMode == RouteMatchMode.FirstMatch)
                {
                    for (var r = 0; r < rules.Count; r++)
                    {
                        if (!rules[r].Predicate(item))
                            continue;

                        foreach (var ch in _ruleChannels[r])
                            await WriteOneAsync(ch, item).ConfigureAwait(false);

                        matched = true;
                        break;
                    }
                }
                else
                {
                    Array.Clear(_queuedScratch);

                    for (var r = 0; r < rules.Count; r++)
                    {
                        if (!rules[r].Predicate(item))
                            continue;

                        matched = true;

                        foreach (var ch in _ruleChannels[r])
                        {
                            if (_queuedScratch[ch])
                                continue;

                            _queuedScratch[ch] = true;
                            await WriteOneAsync(ch, item).ConfigureAwait(false);
                        }
                    }
                }

                if (!matched)
                {
                    if (_otherwiseChannels is { } otherwiseChannels)
                    {
                        foreach (var ch in otherwiseChannels)
                            await WriteOneAsync(ch, item).ConfigureAwait(false);
                    }
                    else if (_options.NoMatchBehavior == NoRouteMatchBehavior.Throw)
                    {
                        throw new InvalidOperationException(
                            $"No route rule matched an item for stream '{StreamName}' and no otherwise route was configured.");
                    }
                }

                SampleBacklog();
            }

            // Always take one final sample so a short-lived stream (fewer than the sampling interval's worth
            // of items) still reports an accurate backlog instead of never sampling at all.
            SampleBacklog(force: true);

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
    ///     Writes one item to one channel, synchronously in the common case. Replaces the old closure-based
    ///     QueueWrite/Task[] pattern, which allocated per item and could orphan a write task's exception when a
    ///     later rule's predicate threw.
    /// </summary>
    private ValueTask WriteOneAsync(int channelIndex, T item)
    {
        if (Volatile.Read(ref _abandonedChannels[channelIndex]) != 0)
            return ValueTask.CompletedTask;

        var writer = _channels[channelIndex].Writer;

        return writer.TryWrite(item) ? ValueTask.CompletedTask : WriteBlockedAsync(writer, item);
    }

    private async ValueTask WriteBlockedAsync(ChannelWriter<T> writer, T item) =>
        await writer.WriteAsync(item, _cts.Token).ConfigureAwait(false);

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
    ///     The pump feeds every matching subscriber in lockstep, so a reader that stops early would fill its buffer and
    ///     block the pump forever, stalling subscribers that are still consuming. Discarding the abandoned channel
    ///     keeps the rest of the fan-out running.
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
    private sealed class ChannelSubscriber(CountingConditionalMulticastDataStream<T> owner, int index, CancellationToken ct) : IAsyncEnumerator<T>
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
    private sealed class EdgeView(CountingConditionalMulticastDataStream<T> owner, int channelIndex) : IForwardOnlyDataStream<T>
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

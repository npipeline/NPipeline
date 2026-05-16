using System.Runtime.CompilerServices;
using System.Threading.Channels;
using NPipeline.DataFlow.Branching;
using NPipeline.DataFlow.Routing;
using NPipeline.Graph;

namespace NPipeline.DataFlow.DataStreams;

/// <summary>
///     Multicast stream that integrates counting and conditional routing by named source outputs.
/// </summary>
internal sealed class CountingConditionalMulticastDataStream<T> : IForwardOnlyDataStream<T>, IHasBranchMetrics, IEdgeRoutedDataStream
{
    private readonly Channel<T>[] _channels;
    private readonly int[] _channelTaken;
    private readonly StatsCounter _counter;
    private readonly CancellationTokenSource _cts = new();
    private readonly Dictionary<Edge, int> _edgeToChannel;
    private readonly Dictionary<string, int[]> _namedOutputChannels;
    private readonly RouteOptions<T> _options;
    private readonly int[] _pendingPerChannel;
    private readonly Task _pumpTask;
    private readonly IDataStream<T> _source;
    private bool _disposed;
    private int _nextSubscriber;

    public CountingConditionalMulticastDataStream(
        IDataStream<T> source,
        StatsCounter counter,
        IReadOnlyList<Edge> subscriberEdges,
        int? perSubscriberBuffer,
        RouteOptions<T> options,
        BranchMetrics metrics)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(counter);
        ArgumentNullException.ThrowIfNull(subscriberEdges);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(metrics);

        _source = source;
        _counter = counter;
        _options = options;
        Metrics = metrics;

        _channels = new Channel<T>[subscriberEdges.Count];
        _channelTaken = new int[subscriberEdges.Count];
        _pendingPerChannel = new int[subscriberEdges.Count];
        _edgeToChannel = new Dictionary<Edge, int>(subscriberEdges.Count);

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
            {
                continue;
            }

            if (!namedOutputChannels.TryGetValue(edge.SourceOutputName, out var channelIndexes))
            {
                channelIndexes = [];
                namedOutputChannels[edge.SourceOutputName] = channelIndexes;
            }

            channelIndexes.Add(i);
        }

        _namedOutputChannels = namedOutputChannels.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.ToArray(), StringComparer.Ordinal);

        ValidateConfiguredOutputs();

        _pumpTask = Task.Run(PumpAsync, CancellationToken.None);

        if (perSubscriberBuffer.HasValue)
            Metrics.SetPerSubscriberCapacity(perSubscriberBuffer.Value);
    }

    public string StreamName => $"CountedConditionalMulticast_{_source.StreamName}";

    public BranchMetrics Metrics { get; }

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

        return GetAsyncEnumeratorForChannel(idx, cancellationToken);
    }

    public IDataStream GetEdgeView(Edge edge)
    {
        ArgumentNullException.ThrowIfNull(edge);

        if (!_edgeToChannel.TryGetValue(edge, out var channelIndex))
        {
            throw new InvalidOperationException(
                $"Edge '{edge.SourceNodeId}->{edge.TargetNodeId}' (output='{edge.SourceOutputName ?? "<default>"}') was not registered for stream '{StreamName}'.");
        }

        return new EdgeRoutedDataStream(this, edge, channelIndex);
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

        await _pumpTask.ConfigureAwait(false);

        _cts.Dispose();
        await _source.DisposeAsync().ConfigureAwait(false);
    }

    internal IAsyncEnumerator<T> GetAsyncEnumeratorForChannel(int channelIndex, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (Interlocked.Exchange(ref _channelTaken[channelIndex], 1) != 0)
        {
            throw new InvalidOperationException(
                $"Channel {channelIndex} on stream '{StreamName}' has already been consumed.");
        }

        return ReadChannel(_channels[channelIndex], channelIndex, cancellationToken);
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
        try
        {
            await foreach (var item in _source.WithCancellation(_cts.Token))
            {
                _ = Interlocked.Increment(ref _counter.GetTotalRef());

                var writes = new Task[_channels.Length];
                var writeCount = 0;
                var aggregatePending = 0;

                void QueueWrite(int channelIndex)
                {
                    writes[writeCount++] = _channels[channelIndex].Writer.WriteAsync(item, _cts.Token).AsTask();
                    var pending = Interlocked.Increment(ref _pendingPerChannel[channelIndex]);
                    aggregatePending += pending;
                    Metrics.ObservePerSubscriberPending(channelIndex, pending);
                }

                var matched = false;

                if (_options.MatchMode == RouteMatchMode.FirstMatch)
                {
                    foreach (var rule in _options.Rules)
                    {
                        if (!rule.Predicate(item))
                            continue;

                        QueueNamedOutput(rule.OutputName, QueueWrite);
                        matched = true;
                        break;
                    }
                }
                else
                {
                    HashSet<int>? queuedChannels = null;

                    foreach (var rule in _options.Rules)
                    {
                        if (!rule.Predicate(item))
                            continue;

                        matched = true;

                        if (!_namedOutputChannels.TryGetValue(rule.OutputName, out var channelIndexes))
                            continue;

                        queuedChannels ??= [];

                        foreach (var channelIndex in channelIndexes)
                        {
                            if (queuedChannels.Add(channelIndex))
                                QueueWrite(channelIndex);
                        }
                    }
                }

                if (!matched)
                {
                    if (_options.OtherwiseOutputName is { } otherwiseOutput)
                    {
                        QueueNamedOutput(otherwiseOutput, QueueWrite);
                    }
                    else if (_options.NoMatchBehavior == NoRouteMatchBehavior.Throw)
                    {
                        throw new InvalidOperationException(
                            $"No route rule matched an item for stream '{StreamName}' and no otherwise route was configured.");
                    }
                }

                if (writeCount == 0)
                    continue;

                if (writeCount == 1)
                {
                    await writes[0].ConfigureAwait(false);
                }
                else
                {
                    await Task.WhenAll(writes[..writeCount]).ConfigureAwait(false);
                }

                Metrics.ObservePending(aggregatePending);
            }

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
    }

    private void QueueNamedOutput(string outputName, Action<int> enqueue)
    {
        if (!_namedOutputChannels.TryGetValue(outputName, out var channelIndexes))
            return;

        foreach (var channelIndex in channelIndexes)
        {
            enqueue(channelIndex);
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

    private sealed class EdgeRoutedDataStream(
        CountingConditionalMulticastDataStream<T> owner,
        Edge edge,
        int channelIndex)
        : IForwardOnlyDataStream<T>
    {
        public string StreamName => $"{owner.StreamName}_{edge.SourceNodeId}_{edge.TargetNodeId}";

        public Type GetDataType()
        {
            return typeof(T);
        }

        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return owner.GetAsyncEnumeratorForChannel(channelIndex, cancellationToken);
        }

        public async IAsyncEnumerable<object?> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await using var enumerator = GetAsyncEnumerator(cancellationToken);

            while (await enumerator.MoveNextAsync())
            {
                if (enumerator.Current is not null)
                    yield return enumerator.Current;
            }
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }
}

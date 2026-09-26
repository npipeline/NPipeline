using System.Runtime.CompilerServices;
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

    private readonly MulticastChannels<T> _channels;
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

        _channels = new MulticastChannels<T>(this, StreamName, subscriberEdges.Count, perSubscriberBuffer, metrics);
        _edgeToChannel = new Dictionary<Edge, int>(subscriberEdges.Count);
        _queuedScratch = new bool[subscriberEdges.Count];

        var namedOutputChannels = new Dictionary<string, List<int>>(StringComparer.Ordinal);

        for (var i = 0; i < subscriberEdges.Count; i++)
        {
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
    }

    public IDataStream GetEdgeView(Edge edge) => _channels.CreateEdgeView(ResolveChannel(edge));

    public void ReleaseEdge(Edge edge) => _channels.Abandon(ResolveChannel(edge));

    public string StreamName => $"CountedConditionalMulticast_{_source.StreamName}";

    public Type GetDataType() => typeof(T);

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

        if (!_edgeToChannel.TryGetValue(edge, out var channelIndex))
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
        var token = _cts.Token;

        try
        {
            await foreach (var item in _source.WithCancellation(token).ConfigureAwait(false))
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
                            await _channels.WriteAsync(ch, item, token).ConfigureAwait(false);

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
                            await _channels.WriteAsync(ch, item, token).ConfigureAwait(false);
                        }
                    }
                }

                if (!matched)
                {
                    if (_otherwiseChannels is { } otherwiseChannels)
                    {
                        foreach (var ch in otherwiseChannels)
                            await _channels.WriteAsync(ch, item, token).ConfigureAwait(false);
                    }
                    else if (_options.NoMatchBehavior == NoRouteMatchBehavior.Throw)
                    {
                        throw new InvalidOperationException(
                            $"No route rule matched an item for stream '{StreamName}' and no otherwise route was configured.");
                    }
                }

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

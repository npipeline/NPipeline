using System.Collections.Concurrent;
using NPipeline.Observability.Metrics;

namespace NPipeline.Observability;

/// <summary>
///     Thread-safe collector for comprehensive observability metrics during pipeline execution.
/// </summary>
public sealed class ObservabilityCollector : IObservabilityCollector
{
    private readonly IObservabilityFactory _factory;
    private readonly ConcurrentDictionary<string, NodeMetricsBuilder> _nodeMetrics = new();

    /// <summary>
    ///     Initializes a new instance of the <see cref="ObservabilityCollector" /> class.
    /// </summary>
    /// <param name="factory">The factory for resolving observability components.</param>
    public ObservabilityCollector(IObservabilityFactory factory)
    {
        _factory = factory ?? throw new ArgumentNullException(nameof(factory));
    }

    /// <inheritdoc />
    public void RecordNodeStart(string nodeId, DateTimeOffset timestamp, int? threadId = null, double? initialMemoryMb = null, string? pipelineName = null)
    {
        ArgumentNullException.ThrowIfNull(nodeId);

        var builder = GetOrCreateBuilder(nodeId, pipelineName);
        builder.TrySetPipelineName(pipelineName);
        builder.RecordStart(timestamp, threadId, initialMemoryMb);
    }

    /// <inheritdoc />
    public void RecordNodeEnd(string nodeId, DateTimeOffset timestamp, bool success, Exception? exception = null, double? peakMemoryMb = null,
        long? processorTimeMs = null, string? pipelineName = null)
    {
        ArgumentNullException.ThrowIfNull(nodeId);

        var builder = GetOrCreateBuilder(nodeId, pipelineName);
        builder.TrySetPipelineName(pipelineName);
        builder.RecordEnd(timestamp, success, exception, peakMemoryMb, processorTimeMs);
    }

    /// <inheritdoc />
    public void RecordItemMetrics(string nodeId, long itemsProcessed, long itemsEmitted, string? pipelineName = null)
    {
        ArgumentNullException.ThrowIfNull(nodeId);

        var builder = GetOrCreateBuilder(nodeId, pipelineName);
        builder.TrySetPipelineName(pipelineName);
        builder.RecordItemMetrics(itemsProcessed, itemsEmitted);
    }

    /// <inheritdoc />
    public void RecordRetry(string nodeId, int retryCount, string? reason = null, string? pipelineName = null)
    {
        ArgumentNullException.ThrowIfNull(nodeId);

        var builder = GetOrCreateBuilder(nodeId, pipelineName);
        builder.TrySetPipelineName(pipelineName);
        builder.RecordRetry(retryCount);
    }

    /// <inheritdoc />
    public void RecordPerformanceMetrics(string nodeId, double throughputItemsPerSec, double averageItemProcessingMs, string? pipelineName = null)
    {
        ArgumentNullException.ThrowIfNull(nodeId);

        var builder = GetOrCreateBuilder(nodeId, pipelineName);
        builder.TrySetPipelineName(pipelineName);
        builder.RecordPerformanceMetrics(throughputItemsPerSec, averageItemProcessingMs);
    }

    /// <inheritdoc />
    public IReadOnlyList<INodeMetrics> GetNodeMetrics()
    {
        return [.. _nodeMetrics.Values.Select(static builder => builder.Build())];
    }

    /// <inheritdoc />
    public INodeMetrics? GetNodeMetrics(string nodeId, string? pipelineName = null)
    {
        if (nodeId is null)
            return null;

        if (!string.IsNullOrWhiteSpace(pipelineName) &&
            _nodeMetrics.TryGetValue(BuildMetricKey(nodeId, pipelineName), out var qualified))
        {
            return qualified.Build();
        }

        // Backward-compatible fast path for top-level (non-qualified) entries
        if (_nodeMetrics.TryGetValue(nodeId, out var direct))
            return direct.Build();

        // Fallback: locate by logical node id across pipeline-qualified entries.
        // If multiple entries exist, prefer top-level (null pipeline), then lexical order.
        var matches = _nodeMetrics.Values
            .Where(b => string.Equals(b.NodeId, nodeId, StringComparison.Ordinal))
            .OrderBy(b => b.PipelineName is null ? 0 : 1)
            .ThenBy(b => b.PipelineName, StringComparer.Ordinal)
            .ToArray();

        return matches.Length == 0 ? null : matches[0].Build();
    }

    /// <inheritdoc />
    public IPipelineMetrics CreatePipelineMetrics(string pipelineName, Guid runId, DateTimeOffset startTime, DateTimeOffset? endTime, bool success,
        Exception? exception = null)
    {
        ArgumentNullException.ThrowIfNull(pipelineName);

        var nodeMetrics = GetNodeMetrics();
        var totalItemsProcessed = nodeMetrics.Sum(m => m.ItemsProcessed);

        var durationMs = endTime.HasValue
            ? (long?)(long)(endTime.Value - startTime).TotalMilliseconds
            : null;

        return new PipelineMetrics(
            pipelineName,
            runId,
            startTime,
            endTime,
            durationMs,
            success,
            totalItemsProcessed,
            nodeMetrics,
            exception);
    }

    /// <inheritdoc />
    public async Task EmitMetricsAsync(string pipelineName, Guid runId, DateTimeOffset startTime, DateTimeOffset? endTime, bool success,
        Exception? exception = null, CancellationToken cancellationToken = default)
    {
        // Create pipeline metrics
        var pipelineMetrics = CreatePipelineMetrics(pipelineName, runId, startTime, endTime, success, exception);

        // Resolve and invoke node metrics sinks
        var nodeMetricsSink = _factory.ResolveMetricsSink();

        if (nodeMetricsSink != null)
        {
            foreach (var nodeMetric in pipelineMetrics.NodeMetrics)
            {
                await nodeMetricsSink.RecordAsync(nodeMetric, cancellationToken).ConfigureAwait(false);
            }
        }

        // Resolve and invoke pipeline metrics sink
        var pipelineMetricsSink = _factory.ResolvePipelineMetricsSink();

        if (pipelineMetricsSink != null)
            await pipelineMetricsSink.RecordAsync(pipelineMetrics, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Initializes a node entry without recording timing information.
    /// </summary>
    /// <param name="nodeId">The unique identifier of the node.</param>
    /// <param name="threadId">The thread ID executing the node.</param>
    /// <param name="initialMemoryMb">The initial memory usage in megabytes.</param>
    public void InitializeNode(string nodeId, int? threadId = null, double? initialMemoryMb = null)
    {
        if (nodeId == null)
            return;

        var builder = GetOrCreateBuilder(nodeId, null);
        builder.Initialize(threadId, initialMemoryMb);
    }

    private NodeMetricsBuilder GetOrCreateBuilder(string nodeId, string? pipelineName)
    {
        if (!string.IsNullOrWhiteSpace(pipelineName))
        {
            var qualifiedKey = BuildMetricKey(nodeId, pipelineName);
            return _nodeMetrics.GetOrAdd(qualifiedKey, _ => new NodeMetricsBuilder(nodeId, pipelineName));
        }

        // Backward-compatible behavior for top-level pipelines and callers that do not provide pipeline identity
        return _nodeMetrics.GetOrAdd(nodeId, _ => new NodeMetricsBuilder(nodeId));
    }

    private static string BuildMetricKey(string nodeId, string? pipelineName)
    {
        return string.IsNullOrWhiteSpace(pipelineName)
            ? nodeId
            : string.Concat(pipelineName, "::", nodeId);
    }

    private sealed class NodeMetricsBuilder(string nodeId, string? pipelineName = null)
    {
        private readonly object _identityLock = new();
        private readonly string _nodeId = nodeId;
        private string? _pipelineName = pipelineName;
        private readonly object _performanceMetricsLock = new();
        private double? _averageItemProcessingMs;
        private long? _durationMs;
        private DateTimeOffset? _endTime;
        private Exception? _exception;
        private long _itemsEmitted;
        private long _itemsProcessed;
        private double? _peakMemoryUsageMb;
        private long? _processorTimeMs;
        private int _retryCount;
        private DateTimeOffset? _startTime;
        private bool _success = true;
        private int? _threadId;
        private double? _throughputItemsPerSec;

        public string NodeId => _nodeId;

        public string? PipelineName => _pipelineName;

        public void TrySetPipelineName(string? pipelineName)
        {
            if (string.IsNullOrWhiteSpace(pipelineName))
                return;

            lock (_identityLock)
            {
                _pipelineName ??= pipelineName;
            }
        }

        public void RecordStart(DateTimeOffset timestamp, int? threadId, double? initialMemoryMb)
        {
            _startTime = timestamp;
            _threadId = threadId;

            if (initialMemoryMb.HasValue)
                _peakMemoryUsageMb = initialMemoryMb;
        }

        public void Initialize(int? threadId, double? initialMemoryMb)
        {
            _threadId = threadId;

            if (initialMemoryMb.HasValue)
                _peakMemoryUsageMb = initialMemoryMb;
        }

        public void RecordEnd(DateTimeOffset timestamp, bool success, Exception? exception, double? peakMemoryMb, long? processorTimeMs)
        {
            _endTime = timestamp;
            _success = success;
            _exception = exception;
            _peakMemoryUsageMb = peakMemoryMb;
            _processorTimeMs = processorTimeMs;

            if (_startTime.HasValue)
                _durationMs = (long)(timestamp - _startTime.Value).TotalMilliseconds;
        }

        public void RecordItemMetrics(long itemsProcessed, long itemsEmitted)
        {
            _ = Interlocked.Add(ref _itemsProcessed, itemsProcessed);
            _ = Interlocked.Add(ref _itemsEmitted, itemsEmitted);
        }

        public void RecordRetry(int retryCount)
        {
            int initial, computed;

            do
            {
                initial = _retryCount;
                computed = Math.Max(initial, retryCount);
            } while (Interlocked.CompareExchange(ref _retryCount, computed, initial) != initial);
        }

        public void RecordPerformanceMetrics(double throughputItemsPerSec, double averageItemProcessingMs)
        {
            lock (_performanceMetricsLock)
            {
                _throughputItemsPerSec = throughputItemsPerSec;
                _averageItemProcessingMs = averageItemProcessingMs;
            }
        }

        public INodeMetrics Build()
        {
            return new NodeMetrics(
                _nodeId,
                _startTime,
                _endTime,
                _durationMs,
                _success,
                _itemsProcessed,
                _itemsEmitted,
                _exception,
                _retryCount,
                _peakMemoryUsageMb,
                _processorTimeMs,
                _throughputItemsPerSec,
                _averageItemProcessingMs,
                _threadId,
                _pipelineName);
        }
    }
}

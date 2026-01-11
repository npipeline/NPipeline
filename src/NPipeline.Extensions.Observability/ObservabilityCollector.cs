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

    /// <summary>
    ///     Records the start of a node execution.
    /// </summary>
    /// <param name="nodeId">The unique identifier of the node.</param>
    /// <param name="timestamp">The timestamp when execution started.</param>
    /// <param name="threadId">The thread ID executing the node.</param>
    /// <param name="initialMemoryMb">The initial memory usage in megabytes.</param>
    public void RecordNodeStart(string nodeId, DateTimeOffset timestamp, int? threadId = null, long? initialMemoryMb = null)
    {
        ArgumentNullException.ThrowIfNull(nodeId);

        var builder = _nodeMetrics.GetOrAdd(nodeId, _ => new NodeMetricsBuilder(nodeId));
        builder.RecordStart(timestamp, threadId, initialMemoryMb);
    }

    /// <summary>
    ///     Records the completion of a node execution.
    /// </summary>
    /// <param name="nodeId">The unique identifier of the node.</param>
    /// <param name="timestamp">The timestamp when execution completed.</param>
    /// <param name="success">Whether the execution was successful.</param>
    /// <param name="exception">Any exception that occurred during execution.</param>
    /// <param name="peakMemoryMb">The peak memory usage in megabytes during execution.</param>
    /// <param name="processorTimeMs">The processor time used in milliseconds.</param>
    public void RecordNodeEnd(string nodeId, DateTimeOffset timestamp, bool success, Exception? exception = null, long? peakMemoryMb = null,
        long? processorTimeMs = null)
    {
        if (nodeId == null)
            return;

        var builder = _nodeMetrics.GetOrAdd(nodeId, _ => new NodeMetricsBuilder(nodeId));
        builder.RecordEnd(timestamp, success, exception, peakMemoryMb, processorTimeMs);
    }

    /// <summary>
    ///     Records item processing metrics for a node.
    /// </summary>
    /// <param name="nodeId">The unique identifier of node.</param>
    /// <param name="itemsProcessed">The number of items processed.</param>
    /// <param name="itemsEmitted">The number of items emitted.</param>
    public void RecordItemMetrics(string nodeId, long itemsProcessed, long itemsEmitted)
    {
        if (nodeId == null)
            return;

        var builder = _nodeMetrics.GetOrAdd(nodeId, _ => new NodeMetricsBuilder(nodeId));
        builder.RecordItemMetrics(itemsProcessed, itemsEmitted);
    }

    /// <summary>
    ///     Records a retry attempt for a node.
    /// </summary>
    /// <param name="nodeId">The unique identifier of node.</param>
    /// <param name="retryCount">The current retry attempt number.</param>
    /// <param name="reason">The reason for retry.</param>
    public void RecordRetry(string nodeId, int retryCount, string? reason = null)
    {
        if (nodeId == null)
            return;

        var builder = _nodeMetrics.GetOrAdd(nodeId, _ => new NodeMetricsBuilder(nodeId));
        builder.RecordRetry(retryCount);
    }

    /// <summary>
    ///     Records performance metrics for a completed node execution.
    /// </summary>
    /// <param name="nodeId">The unique identifier of node.</param>
    /// <param name="throughputItemsPerSec">The throughput in items per second.</param>
    /// <param name="averageItemProcessingMs">The average time per item in milliseconds.</param>
    public void RecordPerformanceMetrics(string nodeId, double throughputItemsPerSec, double averageItemProcessingMs)
    {
        if (nodeId == null)
            return;

        var builder = _nodeMetrics.GetOrAdd(nodeId, _ => new NodeMetricsBuilder(nodeId));
        builder.RecordPerformanceMetrics(throughputItemsPerSec, averageItemProcessingMs);
    }

    /// <summary>
    ///     Gets the collected metrics for all nodes.
    /// </summary>
    /// <returns>A collection of node metrics.</returns>
    public IReadOnlyList<INodeMetrics> GetNodeMetrics()
    {
        return [.. _nodeMetrics.Values.Select(static builder => builder.Build())];
    }

    /// <summary>
    ///     Gets the collected metrics for a specific node.
    /// </summary>
    /// <param name="nodeId">The unique identifier of the node.</param>
    /// <returns>The node metrics, or null if not found.</returns>
    public INodeMetrics? GetNodeMetrics(string nodeId)
    {
        return nodeId != null && _nodeMetrics.TryGetValue(nodeId, out var builder)
            ? builder.Build()
            : null;
    }

    /// <summary>
    ///     Creates pipeline-level metrics from the collected data.
    /// </summary>
    /// <param name="pipelineName">The name of the pipeline.</param>
    /// <param name="runId">The unique identifier for this pipeline run.</param>
    /// <param name="startTime">When the pipeline started.</param>
    /// <param name="endTime">When the pipeline ended.</param>
    /// <param name="success">Whether the pipeline execution was successful.</param>
    /// <param name="exception">Any exception that occurred during pipeline execution.</param>
    /// <returns>The pipeline metrics.</returns>
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

    /// <summary>
    ///     Emits all collected metrics to the registered sinks.
    /// </summary>
    /// <param name="pipelineName">The name of the pipeline.</param>
    /// <param name="runId">The unique identifier for this pipeline run.</param>
    /// <param name="startTime">When the pipeline started.</param>
    /// <param name="endTime">When the pipeline ended.</param>
    /// <param name="success">Whether the pipeline execution was successful.</param>
    /// <param name="exception">Any exception that occurred during pipeline execution.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
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
    public void InitializeNode(string nodeId, int? threadId = null, long? initialMemoryMb = null)
    {
        if (nodeId == null)
            return;

        var builder = _nodeMetrics.GetOrAdd(nodeId, _ => new NodeMetricsBuilder(nodeId));
        builder.Initialize(threadId, initialMemoryMb);
    }

    /// <summary>
    ///     Builder for constructing node metrics with thread-safe updates.
    /// </summary>
    private sealed class NodeMetricsBuilder(string nodeId)
    {
        private readonly string _nodeId = nodeId;
        private readonly object _performanceMetricsLock = new();
        private double? _averageItemProcessingMs;
        private long? _durationMs;
        private DateTimeOffset? _endTime;
        private Exception? _exception;
        private long _itemsEmitted;
        private long _itemsProcessed;
        private long? _peakMemoryUsageMb;
        private long? _processorTimeMs;
        private int _retryCount;
        private DateTimeOffset? _startTime;
        private bool _success = true;
        private int? _threadId;
        private double? _throughputItemsPerSec;

        public void RecordStart(DateTimeOffset timestamp, int? threadId, long? initialMemoryMb)
        {
            _startTime = timestamp;
            _threadId = threadId;

            if (initialMemoryMb.HasValue)
                _peakMemoryUsageMb = initialMemoryMb;
        }

        public void Initialize(int? threadId, long? initialMemoryMb)
        {
            _threadId = threadId;

            if (initialMemoryMb.HasValue)
                _peakMemoryUsageMb = initialMemoryMb;
        }

        public void RecordEnd(DateTimeOffset timestamp, bool success, Exception? exception, long? peakMemoryMb, long? processorTimeMs)
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
            // Thread-safe: Use lock to ensure atomic updates for nullable double fields
            // Interlocked.Exchange doesn't support nullable value types, so we use a lock
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
                _threadId);
        }
    }
}

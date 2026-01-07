using System.Collections.Concurrent;
using NPipeline.Observability.Metrics;

namespace NPipeline.Observability;

/// <summary>
///     Thread-safe collector for comprehensive observability metrics during pipeline execution.
/// </summary>
public sealed class ObservabilityCollector : IObservabilityCollector
{
    private readonly ConcurrentDictionary<string, NodeMetricsBuilder> _nodeMetrics = new();

    /// <summary>
    ///     Records the start of a node execution.
    /// </summary>
    /// <param name="nodeId">The unique identifier of the node.</param>
    /// <param name="timestamp">The timestamp when execution started.</param>
    /// <param name="threadId">The thread ID executing the node.</param>
    /// <param name="initialMemoryMb">The initial memory usage in megabytes.</param>
    public void RecordNodeStart(string nodeId, DateTimeOffset timestamp, int? threadId = null, long? initialMemoryMb = null)
    {
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
        if (_nodeMetrics.TryGetValue(nodeId, out var builder))
        {
            builder.RecordEnd(timestamp, success, exception, peakMemoryMb, processorTimeMs);
        }
    }

    /// <summary>
    ///     Records item processing metrics for a node.
    /// </summary>
    /// <param name="nodeId">The unique identifier of the node.</param>
    /// <param name="itemsProcessed">The number of items processed.</param>
    /// <param name="itemsEmitted">The number of items emitted.</param>
    public void RecordItemMetrics(string nodeId, long itemsProcessed, long itemsEmitted)
    {
        if (_nodeMetrics.TryGetValue(nodeId, out var builder))
        {
            builder.RecordItemMetrics(itemsProcessed, itemsEmitted);
        }
    }

    /// <summary>
    ///     Records a retry attempt for a node.
    /// </summary>
    /// <param name="nodeId">The unique identifier of the node.</param>
    /// <param name="retryCount">The current retry attempt number.</param>
    /// <param name="reason">The reason for the retry.</param>
    public void RecordRetry(string nodeId, int retryCount, string? reason = null)
    {
        if (_nodeMetrics.TryGetValue(nodeId, out var builder))
        {
            builder.RecordRetry(retryCount);
        }
    }

    /// <summary>
    ///     Records performance metrics for a completed node execution.
    /// </summary>
    /// <param name="nodeId">The unique identifier of the node.</param>
    /// <param name="throughputItemsPerSec">The throughput in items per second.</param>
    /// <param name="averageItemProcessingMs">The average time per item in milliseconds.</param>
    public void RecordPerformanceMetrics(string nodeId, double throughputItemsPerSec, double averageItemProcessingMs)
    {
        if (_nodeMetrics.TryGetValue(nodeId, out var builder))
        {
            builder.RecordPerformanceMetrics(throughputItemsPerSec, averageItemProcessingMs);
        }
    }

    /// <summary>
    ///     Gets the collected metrics for all nodes.
    /// </summary>
    /// <returns>A collection of node metrics.</returns>
    public IReadOnlyList<INodeMetrics> GetNodeMetrics()
    {
        return _nodeMetrics.Values
            .Select(builder => builder.Build())
            .ToArray();
    }

    /// <summary>
    ///     Gets the collected metrics for a specific node.
    /// </summary>
    /// <param name="nodeId">The unique identifier of the node.</param>
    /// <returns>The node metrics, or null if not found.</returns>
    public INodeMetrics? GetNodeMetrics(string nodeId)
    {
        return _nodeMetrics.TryGetValue(nodeId, out var builder) ? builder.Build() : null;
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
        var nodeMetrics = GetNodeMetrics();
        var totalItemsProcessed = nodeMetrics.Sum(m => m.ItemsProcessed);
        var durationMs = endTime.HasValue ? (long?)(long)(endTime.Value - startTime).TotalMilliseconds : null;

        return new PipelineMetrics(
            PipelineName: pipelineName,
            RunId: runId,
            StartTime: startTime,
            EndTime: endTime,
            DurationMs: durationMs,
            Success: success,
            TotalItemsProcessed: totalItemsProcessed,
            NodeMetrics: nodeMetrics,
            Exception: exception);
    }

    /// <summary>
    ///     Builder for constructing node metrics with thread-safe updates.
    /// </summary>
    private sealed class NodeMetricsBuilder
    {
        private readonly string _nodeId;
        private DateTimeOffset? _startTime;
        private DateTimeOffset? _endTime;
        private long? _durationMs;
        private bool _success;
        private long _itemsProcessed;
        private long _itemsEmitted;
        private Exception? _exception;
        private int _retryCount;
        private long? _peakMemoryUsageMb;
        private long? _processorTimeMs;
        private double? _throughputItemsPerSec;
        private double? _averageItemProcessingMs;
        private int? _threadId;

        public NodeMetricsBuilder(string nodeId)
        {
            _nodeId = nodeId;
            _success = true;
        }

        public void RecordStart(DateTimeOffset timestamp, int? threadId, long? initialMemoryMb)
        {
            _startTime = timestamp;
            _threadId = threadId;
        }

        public void RecordEnd(DateTimeOffset timestamp, bool success, Exception? exception, long? peakMemoryMb, long? processorTimeMs)
        {
            _endTime = timestamp;
            _success = success;
            _exception = exception;
            _peakMemoryUsageMb = peakMemoryMb;
            _processorTimeMs = processorTimeMs;

            if (_startTime.HasValue)
            {
                _durationMs = (long)(timestamp - _startTime.Value).TotalMilliseconds;
            }
        }

        public void RecordItemMetrics(long itemsProcessed, long itemsEmitted)
        {
            Interlocked.Add(ref _itemsProcessed, itemsProcessed);
            Interlocked.Add(ref _itemsEmitted, itemsEmitted);
        }

        public void RecordRetry(int retryCount)
        {
            Interlocked.Exchange(ref _retryCount, Math.Max(_retryCount, retryCount));
        }

        public void RecordPerformanceMetrics(double throughputItemsPerSec, double averageItemProcessingMs)
        {
            _throughputItemsPerSec = throughputItemsPerSec;
            _averageItemProcessingMs = averageItemProcessingMs;
        }

        public INodeMetrics Build()
        {
            return new NodeMetrics(
                NodeId: _nodeId,
                StartTime: _startTime,
                EndTime: _endTime,
                DurationMs: _durationMs,
                Success: _success,
                ItemsProcessed: _itemsProcessed,
                ItemsEmitted: _itemsEmitted,
                Exception: _exception,
                RetryCount: _retryCount,
                PeakMemoryUsageMb: _peakMemoryUsageMb,
                ProcessorTimeMs: _processorTimeMs,
                ThroughputItemsPerSec: _throughputItemsPerSec,
                AverageItemProcessingMs: _averageItemProcessingMs,
                ThreadId: _threadId);
        }
    }
}
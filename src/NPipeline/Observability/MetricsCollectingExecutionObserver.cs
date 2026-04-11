using System.Collections.Concurrent;
using System.Diagnostics;
using NPipeline.Execution;

namespace NPipeline.Observability;

/// <summary>
///     Execution observer that collects metrics during pipeline execution.
/// </summary>
public sealed class MetricsCollectingExecutionObserver(IObservabilityCollector collector, bool collectMemoryMetrics = false) : IExecutionObserver, IDisposable
{
    private readonly bool _collectMemoryMetrics = collectMemoryMetrics;
    private readonly IObservabilityCollector _collector = collector ?? throw new ArgumentNullException(nameof(collector));
    private readonly ConcurrentDictionary<string, long> _nodeInitialMemory = new();
    private readonly ConcurrentDictionary<string, double> _nodeInitialProcessorTimeMs = new();
    private readonly ConcurrentDictionary<string, DateTimeOffset> _nodeStartTimes = new();
    private bool _disposed;

    /// <inheritdoc />
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    public void OnNodeStarted(NodeExecutionStarted e)
    {
        ArgumentNullException.ThrowIfNull(e);

        if (_disposed)
            return;

        var threadId = Environment.CurrentManagedThreadId;
        double? initialMemoryMb = null;

        if (_collectMemoryMetrics)
        {
            var initialMemoryBytes = GC.GetTotalMemory(false);
            initialMemoryMb = initialMemoryBytes / (1024.0 * 1024.0);
            _nodeInitialMemory[BuildNodeExecutionKey(e.NodeId, e.PipelineId)] = initialMemoryBytes;
        }

        _collector.RecordNodeStart(e.NodeId, e.StartTime, e.PipelineId, threadId, initialMemoryMb, e.PipelineName);
        _nodeStartTimes[BuildNodeExecutionKey(e.NodeId, e.PipelineId)] = e.StartTime;

        var initialProcessorTimeMs = Process.GetCurrentProcess().TotalProcessorTime.TotalMilliseconds;
        _nodeInitialProcessorTimeMs[BuildNodeExecutionKey(e.NodeId, e.PipelineId)] = initialProcessorTimeMs;
    }

    /// <inheritdoc />
    public void OnNodeCompleted(NodeExecutionCompleted e)
    {
        ArgumentNullException.ThrowIfNull(e);

        if (_disposed)
            return;

        // Only record completion if node was started
        var executionKey = BuildNodeExecutionKey(e.NodeId, e.PipelineId);

        if (!_nodeStartTimes.TryRemove(executionKey, out var startTime))
            return;

        var endTime = startTime + e.Duration;

        double? memoryDeltaMb = null;

        if (_collectMemoryMetrics)
        {
            var finalMemoryBytes = GC.GetTotalMemory(false);

            if (_nodeInitialMemory.TryRemove(executionKey, out var initialMemoryBytes))
            {
                var deltaBytes = finalMemoryBytes - initialMemoryBytes;
                memoryDeltaMb = deltaBytes / (1024.0 * 1024.0);
            }
        }

        double? processorTimeMs = null;

        var finalProcessorTimeMs = Process.GetCurrentProcess().TotalProcessorTime.TotalMilliseconds;

        if (_nodeInitialProcessorTimeMs.TryRemove(executionKey, out var initialProcessorTimeMs))
            processorTimeMs = Math.Max(0, finalProcessorTimeMs - initialProcessorTimeMs);

        _collector.RecordNodeEnd(
            e.NodeId,
            endTime,
            e.Success,
            e.PipelineId,
            e.Error,
            memoryDeltaMb,
            processorTimeMs,
            e.PipelineName);

        var nodeMetrics = _collector.GetNodeMetrics(e.NodeId, e.PipelineId);

        if (nodeMetrics != null && nodeMetrics.ItemsProcessed > 0 && nodeMetrics.DurationMs.HasValue)
        {
            var durationSec = nodeMetrics.DurationMs.Value / 1000.0;

            if (durationSec > 0)
            {
                var throughput = nodeMetrics.ItemsProcessed / durationSec;
                var averageItemProcessingMs = nodeMetrics.DurationMs.Value / (double)nodeMetrics.ItemsProcessed;
                _collector.RecordPerformanceMetrics(e.NodeId, throughput, averageItemProcessingMs, e.PipelineId, e.PipelineName);
            }
        }
    }

    /// <inheritdoc />
    public void OnRetry(NodeRetryEvent e)
    {
        ArgumentNullException.ThrowIfNull(e);

        if (_disposed)
            return;

        var reason = e.LastException?.Message;
        _collector.RecordRetry(e.NodeId, e.Attempt, e.PipelineId, reason, e.PipelineName);
    }

    /// <inheritdoc />
    public void OnDrop(QueueDropEvent e)
    {
        ArgumentNullException.ThrowIfNull(e);
    }

    /// <inheritdoc />
    public void OnQueueMetrics(QueueMetricsEvent e)
    {
        ArgumentNullException.ThrowIfNull(e);
    }

    private void Dispose(bool disposing)
    {
        if (_disposed)
            return;

        if (disposing)
        {
            _nodeInitialMemory.Clear();
            _nodeInitialProcessorTimeMs.Clear();
            _nodeStartTimes.Clear();
        }

        _disposed = true;
    }

    private static string BuildNodeExecutionKey(string nodeId, Guid pipelineId)
    {
        return string.Concat(pipelineId.ToString("N"), "::", nodeId);
    }
}

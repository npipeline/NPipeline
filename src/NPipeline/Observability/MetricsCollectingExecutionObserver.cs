using System.Collections.Concurrent;
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
            _nodeInitialMemory[e.NodeId] = initialMemoryBytes;
        }

        _collector.RecordNodeStart(e.NodeId, e.StartTime, threadId, initialMemoryMb);
        _nodeStartTimes[e.NodeId] = e.StartTime;
    }

    /// <inheritdoc />
    public void OnNodeCompleted(NodeExecutionCompleted e)
    {
        ArgumentNullException.ThrowIfNull(e);

        if (_disposed)
            return;

        // Only record completion if node was started
        if (!_nodeStartTimes.TryRemove(e.NodeId, out var startTime))
            return;

        var endTime = startTime + e.Duration;

        double? memoryDeltaMb = null;

        if (_collectMemoryMetrics)
        {
            var finalMemoryBytes = GC.GetTotalMemory(false);

            if (_nodeInitialMemory.TryRemove(e.NodeId, out var initialMemoryBytes))
            {
                var deltaBytes = finalMemoryBytes - initialMemoryBytes;
                memoryDeltaMb = deltaBytes / (1024.0 * 1024.0);
            }
        }

        long? processorTimeMs = null;

        _collector.RecordNodeEnd(
            e.NodeId,
            endTime,
            e.Success,
            e.Error,
            memoryDeltaMb,
            processorTimeMs);

        var nodeMetrics = _collector.GetNodeMetrics(e.NodeId);

        if (nodeMetrics != null && nodeMetrics.ItemsProcessed > 0 && nodeMetrics.DurationMs.HasValue)
        {
            var durationSec = nodeMetrics.DurationMs.Value / 1000.0;

            if (durationSec > 0)
            {
                var throughput = nodeMetrics.ItemsProcessed / durationSec;
                var averageItemProcessingMs = nodeMetrics.DurationMs.Value / (double)nodeMetrics.ItemsProcessed;
                _collector.RecordPerformanceMetrics(e.NodeId, throughput, averageItemProcessingMs);
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
        _collector.RecordRetry(e.NodeId, e.Attempt, reason);
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
            _nodeStartTimes.Clear();
        }

        _disposed = true;
    }
}

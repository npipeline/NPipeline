using System.Collections.Concurrent;
using NPipeline.Execution;
using NPipeline.Observability;

namespace NPipeline.Extensions.Observability;

/// <summary>
///     Execution observer that collects metrics during pipeline execution.
/// </summary>
public sealed class MetricsCollectingExecutionObserver(IObservabilityCollector collector, bool collectMemoryMetrics = false) : IExecutionObserver
{
    private readonly IObservabilityCollector _collector = collector ?? throw new ArgumentNullException(nameof(collector));
    private readonly ConcurrentDictionary<string, DateTimeOffset> _nodeStartTimes = new();
    private readonly ConcurrentDictionary<string, long> _nodeInitialMemory = new();
    private readonly bool _collectMemoryMetrics = collectMemoryMetrics;

    /// <summary>
    ///     Called when a node starts execution.
    /// </summary>
    /// <param name="e">The event containing node execution start information.</param>
    public void OnNodeStarted(NodeExecutionStarted e)
    {
        var threadId = Environment.CurrentManagedThreadId;
        long? initialMemoryMb = null;

        if (_collectMemoryMetrics)
        {
            var initialMemoryBytes = GC.GetTotalMemory(false);
            initialMemoryMb = initialMemoryBytes / (1024 * 1024);
            _nodeInitialMemory[e.NodeId] = initialMemoryBytes;
        }

        _collector.RecordNodeStart(e.NodeId, e.StartTime, threadId, initialMemoryMb);
        _nodeStartTimes[e.NodeId] = e.StartTime;
    }

    /// <summary>
    ///     Called when a node completes execution (successfully or with failure).
    /// </summary>
    /// <param name="e">The event containing node execution completion information.</param>
    public void OnNodeCompleted(NodeExecutionCompleted e)
    {
        var endTime = DateTimeOffset.UtcNow;
        if (_nodeStartTimes.TryRemove(e.NodeId, out var startTime))
        {
            endTime = startTime + e.Duration;
        }

        long? peakMemoryMb = null;
        if (_collectMemoryMetrics)
        {
            var finalMemoryBytes = GC.GetTotalMemory(false);
            peakMemoryMb = finalMemoryBytes / (1024 * 1024);
            _nodeInitialMemory.TryRemove(e.NodeId, out _);
        }

        var processorTimeMs = 0L; // CPU time is not available per-node, skipping

        _collector.RecordNodeEnd(
            e.NodeId,
            endTime,
            e.Success,
            e.Error,
            peakMemoryMb,
            processorTimeMs);

        // Calculate and record performance metrics if items were processed
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

    /// <summary>
    ///     Called when a retry operation occurs.
    /// </summary>
    /// <param name="e">The event containing retry information.</param>
    public void OnRetry(NodeRetryEvent e)
    {
        var reason = e.LastException?.Message;
        _collector.RecordRetry(e.NodeId, e.Attempt, reason);
    }

    /// <summary>
    ///     Called when items are dropped from a queue due to backpressure.
    /// </summary>
    /// <param name="e">The event containing queue drop information.</param>
    public void OnDrop(QueueDropEvent e)
    {
        // Queue drops are not directly tracked in node metrics
        // This could be extended to track backpressure metrics
    }

    /// <summary>
    ///     Called with queue metrics information.
    /// </summary>
    /// <param name="e">The event containing queue metrics.</param>
    public void OnQueueMetrics(QueueMetricsEvent e)
    {
        // Queue metrics are not directly tracked in node metrics
        // This could be extended to track queue depth metrics
    }
}
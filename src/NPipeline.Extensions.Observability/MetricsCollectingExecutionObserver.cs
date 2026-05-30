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
    private readonly ConcurrentDictionary<string, byte> _dataflowCompletedBeforeExecution = new();
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

        if (_dataflowCompletedBeforeExecution.TryRemove(executionKey, out _))
        {
            var dataflowEndTime = _collector.GetNodeMetrics(e.NodeId, e.PipelineId)?.EndTime ?? endTime;

            _collector.RecordNodeEnd(
                e.NodeId,
                dataflowEndTime,
                e.Success,
                e.PipelineId,
                e.Error,
                memoryDeltaMb,
                processorTimeMs,
                e.PipelineName);

            // Preserve dataflow end-time, but still derive metrics if they were not finalized earlier.
            RecordDerivedPerformanceMetrics(e.NodeId, e.PipelineId, e.PipelineName);

            return;
        }

        _collector.RecordNodeEnd(
            e.NodeId,
            endTime,
            e.Success,
            e.PipelineId,
            e.Error,
            memoryDeltaMb,
            processorTimeMs,
            e.PipelineName);

        RecordDerivedPerformanceMetrics(e.NodeId, e.PipelineId, e.PipelineName);
    }

    /// <inheritdoc />
    public void OnNodeDataflowCompleted(NodeDataflowCompleted e)
    {
        ArgumentNullException.ThrowIfNull(e);

        if (_disposed)
            return;

        var executionKey = BuildNodeExecutionKey(e.NodeId, e.PipelineId);

        if (_nodeStartTimes.ContainsKey(executionKey))
            _dataflowCompletedBeforeExecution[executionKey] = 0;

        var skipCollectorWrites = e.MetricsAlreadyCaptured && _collector.HasTimingBreakdown(e.NodeId, e.PipelineId);

        if (!skipCollectorWrites)
        {
            var timingBreakdown = e.TimingBreakdown;
            if (timingBreakdown.WorkDuration <= TimeSpan.Zero &&
                timingBreakdown.InputWaitDuration <= TimeSpan.Zero &&
                timingBreakdown.OutputBlockDuration <= TimeSpan.Zero &&
                timingBreakdown.WallDuration <= TimeSpan.Zero)
            {
                var fallbackWall = e.EndTime >= e.StartTime
                    ? e.EndTime - e.StartTime
                    : TimeSpan.Zero;

                timingBreakdown = new NodeTimingBreakdown(fallbackWall, TimeSpan.Zero, TimeSpan.Zero, fallbackWall);
            }

            _collector.RecordTimingBreakdown(e.NodeId, timingBreakdown, e.PipelineId, e.PipelineName);

            _collector.RecordNodeEnd(
                e.NodeId,
                e.EndTime,
                e.Success,
                e.PipelineId,
                e.Error,
                pipelineName: e.PipelineName);
        }

        RecordDerivedPerformanceMetrics(e.NodeId, e.PipelineId, e.PipelineName, forceUpdate: !skipCollectorWrites);
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
            _dataflowCompletedBeforeExecution.Clear();
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

    private void RecordDerivedPerformanceMetrics(string nodeId, Guid pipelineId, string? pipelineName, bool forceUpdate = false)
    {
        var nodeMetrics = _collector.GetNodeMetrics(nodeId, pipelineId);
        var workDurationMs = nodeMetrics?.WorkDurationMs ?? nodeMetrics?.DurationMs;

        if (nodeMetrics is null || nodeMetrics.ItemsProcessed <= 0 || !workDurationMs.HasValue)
            return;

        if (!forceUpdate && nodeMetrics.AverageItemProcessingMs.HasValue)
            return;

        var durationSec = workDurationMs.Value / 1000.0;

        if (durationSec <= 0)
            return;

        var throughput = nodeMetrics.ItemsProcessed / durationSec;
        var averageItemProcessingMs = workDurationMs.Value / nodeMetrics.ItemsProcessed;
        _collector.RecordPerformanceMetrics(nodeId, throughput, averageItemProcessingMs, pipelineId, pipelineName);
    }
}

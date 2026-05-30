using System.Diagnostics;
using NPipeline.Observability.Configuration;

namespace NPipeline.Observability;

/// <summary>
///     A disposable scope that automatically records observability metrics for a node execution.
/// </summary>
public sealed class AutoObservabilityScope : IAutoObservabilityScope
{
    private readonly IObservabilityCollector _collector;
    private readonly long _initialMemoryBytes;
    private readonly string _nodeId;
    private readonly ObservabilityOptions _options;
    private readonly Guid _pipelineId;
    private readonly string? _pipelineName;
    private readonly Stopwatch _stopwatch;
    private readonly int? _threadId;
    private int _disposed;
    private Exception? _exception;
    private long _itemsEmitted;
    private long _itemsProcessed;
    private long _inputWaitTicks;
    private long _outputBlockTicks;
    private int _explicitWorkTrackingUsed;
    private bool _success;
    private long _workTicks;

    /// <summary>
    ///     Initializes a new instance of the <see cref="AutoObservabilityScope" /> class.
    /// </summary>
    /// <param name="collector">The observability collector to record metrics to.</param>
    /// <param name="nodeId">The unique identifier of node being observed.</param>
    /// <param name="options">The observability options controlling what metrics to record.</param>
    /// <param name="pipelineId">The unique pipeline identity for this node execution context.</param>
    /// <param name="pipelineName">The logical pipeline name for this node execution context.</param>
    public AutoObservabilityScope(
        IObservabilityCollector collector,
        string nodeId,
        ObservabilityOptions options,
        Guid pipelineId,
        string? pipelineName = null)
    {
        _collector = collector ?? throw new ArgumentNullException(nameof(collector));
        _nodeId = nodeId ?? throw new ArgumentNullException(nameof(nodeId));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _pipelineId = pipelineId;
        _pipelineName = pipelineName;
        _stopwatch = Stopwatch.StartNew();

        _initialMemoryBytes = options.RecordMemoryUsage
            ? GC.GetTotalMemory(false)
            : 0;

        _threadId = options.RecordThreadInfo
            ? Environment.CurrentManagedThreadId
            : null;

        _success = true;
        _itemsProcessed = 0;
        _itemsEmitted = 0;

        collector.RecordNodeStart(
            nodeId,
            DateTimeOffset.UtcNow,
            _pipelineId,
            _threadId,
            options.RecordMemoryUsage
                ? _initialMemoryBytes / (1024.0 * 1024.0)
                : null,
            _pipelineName);
    }

    /// <inheritdoc />
    public void RecordItemCount(long processed, long emitted)
    {
        if (Volatile.Read(ref _disposed) == 1)
            return;

        _itemsProcessed = processed;
        _itemsEmitted = emitted;
    }

    /// <inheritdoc />
    public void IncrementProcessed()
    {
        if (Volatile.Read(ref _disposed) == 1)
            return;

        _ = Interlocked.Increment(ref _itemsProcessed);
    }

    /// <inheritdoc />
    public void IncrementEmitted()
    {
        if (Volatile.Read(ref _disposed) == 1)
            return;

        _ = Interlocked.Increment(ref _itemsEmitted);
    }

    /// <inheritdoc />
    public void RecordFailure(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        if (Volatile.Read(ref _disposed) == 1)
            return;

        _success = false;
        _exception = exception;
    }

    /// <inheritdoc />
    public Exception? GetFailureException()
    {
        return _exception;
    }

    /// <inheritdoc />
    public void AddWork(TimeSpan duration)
    {
        if (Volatile.Read(ref _disposed) == 1)
            return;

        _ = Interlocked.Exchange(ref _explicitWorkTrackingUsed, 1);
        AddDurationTicks(ref _workTicks, duration);
    }

    /// <inheritdoc />
    public void AddInputWait(TimeSpan duration)
    {
        if (Volatile.Read(ref _disposed) == 1)
            return;

        AddDurationTicks(ref _inputWaitTicks, duration);
    }

    /// <inheritdoc />
    public void AddOutputBlock(TimeSpan duration)
    {
        if (Volatile.Read(ref _disposed) == 1)
            return;

        AddDurationTicks(ref _outputBlockTicks, duration);
    }

    /// <inheritdoc />
    public NodeTimingBreakdown GetTimingBreakdown()
    {
        // This remains lock-free; sample wall-time before/after bucket reads to reduce skew.
        var wallTicksBefore = _stopwatch.Elapsed.Ticks;
        var inputWaitTicks = Interlocked.Read(ref _inputWaitTicks);
        var outputBlockTicks = Interlocked.Read(ref _outputBlockTicks);
        var workTicks = Interlocked.Read(ref _workTicks);
        var wallTicksAfter = _stopwatch.Elapsed.Ticks;
        var wallTicks = Math.Max(wallTicksBefore, wallTicksAfter);
        var explicitWorkTrackingUsed = Volatile.Read(ref _explicitWorkTrackingUsed) == 1;

        if (inputWaitTicks < 0)
            inputWaitTicks = 0;

        if (outputBlockTicks < 0)
            outputBlockTicks = 0;

        if (workTicks < 0)
            workTicks = 0;

        // Fallback applies only for nodes that never reported explicit work timing.
        if (workTicks == 0 && !explicitWorkTrackingUsed)
            workTicks = wallTicks - inputWaitTicks - outputBlockTicks;

        if (workTicks < 0)
            workTicks = 0;

        // Keep wall duration coherent with observed buckets to avoid losing accounted time.
        var bucketSumTicks = SaturatingAddTicks(inputWaitTicks, SaturatingAddTicks(outputBlockTicks, workTicks));
        if (wallTicks < bucketSumTicks)
            wallTicks = bucketSumTicks;

        return new NodeTimingBreakdown(
            TimeSpan.FromTicks(workTicks),
            TimeSpan.FromTicks(inputWaitTicks),
            TimeSpan.FromTicks(outputBlockTicks),
            TimeSpan.FromTicks(wallTicks));
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        _stopwatch.Stop();
        var timingBreakdown = GetTimingBreakdown();

        if (_options.RecordTiming)
        {
            var endTime = DateTimeOffset.UtcNow;
            double? memoryDeltaMb = null;

            if (_options.RecordMemoryUsage)
            {
                var finalMemoryBytes = GC.GetTotalMemory(false);
                var deltaBytes = finalMemoryBytes - _initialMemoryBytes;
                memoryDeltaMb = deltaBytes / (1024.0 * 1024.0);
            }

            _collector.RecordNodeEnd(
                _nodeId,
                endTime,
                _success,
                _pipelineId,
                _exception,
                memoryDeltaMb,
                pipelineName: _pipelineName);

            _collector.RecordTimingBreakdown(_nodeId, timingBreakdown, _pipelineId, _pipelineName);
        }

        if (_options.RecordItemCounts)
            _collector.RecordItemMetrics(_nodeId, _itemsProcessed, _itemsEmitted, _pipelineId, _pipelineName);

        var workDurationMs = timingBreakdown.WorkDuration.TotalMilliseconds;

        if (_options.RecordPerformanceMetrics && _itemsProcessed > 0 && workDurationMs > 0)
        {
            var throughput = _itemsProcessed / (workDurationMs / 1000.0);
            var avgTimeMs = workDurationMs / _itemsProcessed;
            _collector.RecordPerformanceMetrics(_nodeId, throughput, avgTimeMs, _pipelineId, _pipelineName);
        }
    }

    private static void AddDurationTicks(ref long accumulator, TimeSpan duration)
    {
        if (duration <= TimeSpan.Zero)
            return;

        _ = Interlocked.Add(ref accumulator, duration.Ticks);
    }

    private static long SaturatingAddTicks(long left, long right)
    {
        if (left >= long.MaxValue - right)
            return long.MaxValue;

        return left + right;
    }
}

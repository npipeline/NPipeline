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
    private readonly string? _pipelineName;
    private readonly Stopwatch _stopwatch;
    private readonly int? _threadId;
    private bool _disposed;
    private Exception? _exception;
    private long _itemsEmitted;
    private long _itemsProcessed;
    private bool _success;

    /// <summary>
    ///     Initializes a new instance of the <see cref="AutoObservabilityScope" /> class.
    /// </summary>
    /// <param name="collector">The observability collector to record metrics to.</param>
    /// <param name="nodeId">The unique identifier of node being observed.</param>
    /// <param name="options">The observability options controlling what metrics to record.</param>
    /// <param name="pipelineName">The logical pipeline name for this node execution context.</param>
    public AutoObservabilityScope(
        IObservabilityCollector collector,
        string nodeId,
        ObservabilityOptions options,
        string? pipelineName = null)
    {
        _collector = collector ?? throw new ArgumentNullException(nameof(collector));
        _nodeId = nodeId ?? throw new ArgumentNullException(nameof(nodeId));
        _options = options ?? throw new ArgumentNullException(nameof(options));
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
            _threadId,
            options.RecordMemoryUsage
                ? _initialMemoryBytes / (1024.0 * 1024.0)
                : null,
            _pipelineName);
    }

    /// <inheritdoc />
    public void RecordItemCount(long processed, long emitted)
    {
        _itemsProcessed = processed;
        _itemsEmitted = emitted;
    }

    /// <inheritdoc />
    public void IncrementProcessed()
    {
        _ = Interlocked.Increment(ref _itemsProcessed);
    }

    /// <inheritdoc />
    public void IncrementEmitted()
    {
        _ = Interlocked.Increment(ref _itemsEmitted);
    }

    /// <inheritdoc />
    public void RecordFailure(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        _success = false;
        _exception = exception;
    }

    /// <inheritdoc />
    public Exception? GetFailureException()
    {
        return _exception;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _stopwatch.Stop();

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
                _exception,
                memoryDeltaMb,
                pipelineName: _pipelineName);
        }

        if (_options.RecordItemCounts)
            _collector.RecordItemMetrics(_nodeId, _itemsProcessed, _itemsEmitted, _pipelineName);

        if (_options.RecordPerformanceMetrics && _itemsProcessed > 0 && _stopwatch.ElapsedMilliseconds > 0)
        {
            var throughput = _itemsProcessed / _stopwatch.Elapsed.TotalSeconds;
            var avgTimeMs = _stopwatch.ElapsedMilliseconds / (double)_itemsProcessed;
            _collector.RecordPerformanceMetrics(_nodeId, throughput, avgTimeMs, _pipelineName);
        }
    }
}

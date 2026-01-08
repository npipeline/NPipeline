using System.Diagnostics;
using NPipeline.Observability;
using NPipeline.Observability.Configuration;

namespace NPipeline.Extensions.Observability;

/// <summary>
///     A disposable scope that automatically records observability metrics for a node execution.
/// </summary>
/// <remarks>
///     <para>
///         This struct is used internally by the framework to wrap node executions when
///         observability is configured via <see cref="ObservabilityOptions" />.
///     </para>
///     <para>
///         On construction, it records a node start event (if timing is enabled).
///         On disposal, it records all configured metrics including timing, item counts,
///         and performance metrics.
///     </para>
/// </remarks>
public struct AutoObservabilityScope : IAutoObservabilityScope
{
    private readonly IObservabilityCollector _collector;
    private readonly string _nodeId;
    private readonly ObservabilityOptions _options;
    private readonly Stopwatch _stopwatch;
    private readonly long _initialMemoryBytes;
    private readonly int? _threadId;
    private long _itemsProcessed;
    private long _itemsEmitted;
    private bool _success;
    private Exception? _exception;
    private bool _disposed;

    /// <summary>
    ///     Initializes a new instance of the <see cref="AutoObservabilityScope" /> struct.
    /// </summary>
    /// <param name="collector">The observability collector to record metrics to.</param>
    /// <param name="nodeId">The unique identifier of the node being observed.</param>
    /// <param name="options">The observability options controlling what metrics to record.</param>
    public AutoObservabilityScope(
        IObservabilityCollector collector,
        string nodeId,
        ObservabilityOptions options)
    {
        _collector = collector ?? throw new ArgumentNullException(nameof(collector));
        _nodeId = nodeId ?? throw new ArgumentNullException(nameof(nodeId));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _stopwatch = Stopwatch.StartNew();

        _initialMemoryBytes = options.RecordMemoryUsage
            ? GC.GetTotalMemory(false)
            : 0;

        _threadId = options.RecordThreadInfo
            ? Environment.CurrentManagedThreadId
            : null;

        _itemsProcessed = 0;
        _itemsEmitted = 0;
        _success = true;
        _exception = null;
        _disposed = false;

        // Always record node start with actual timestamp to enable duration calculation
        // The RecordTiming flag controls whether we call RecordNodeEnd, not whether we record start
        collector.RecordNodeStart(
            nodeId,
            DateTimeOffset.UtcNow,
            _threadId,
            options.RecordMemoryUsage
                ? _initialMemoryBytes / 1024 / 1024
                : null);
    }

    /// <summary>
    ///     Records the count of items processed and emitted by the node.
    /// </summary>
    /// <param name="processed">The number of items processed (received as input).</param>
    /// <param name="emitted">The number of items emitted (sent as output).</param>
    public void RecordItemCount(long processed, long emitted)
    {
        _itemsProcessed = processed;
        _itemsEmitted = emitted;
    }

    /// <summary>
    ///     Increments the count of items processed by one.
    /// </summary>
    public void IncrementProcessed()
    {
        _ = Interlocked.Increment(ref _itemsProcessed);
    }

    /// <summary>
    ///     Increments the count of items emitted by one.
    /// </summary>
    public void IncrementEmitted()
    {
        _ = Interlocked.Increment(ref _itemsEmitted);
    }

    /// <summary>
    ///     Records that the node execution failed with an exception.
    /// </summary>
    /// <param name="exception">The exception that caused the failure.</param>
    public void RecordFailure(Exception exception)
    {
        _success = false;
        _exception = exception;
    }

    /// <summary>
    ///     Disposes of the scope and records all configured metrics.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _stopwatch.Stop();

        // Record item metrics if enabled
        if (_options.RecordItemCounts)
            _collector.RecordItemMetrics(_nodeId, _itemsProcessed, _itemsEmitted);

        // Record performance metrics if enabled and we have data
        if (_options.RecordPerformanceMetrics && _itemsProcessed > 0 && _stopwatch.ElapsedMilliseconds > 0)
        {
            var throughput = _itemsProcessed / _stopwatch.Elapsed.TotalSeconds;
            var avgTimeMs = _stopwatch.ElapsedMilliseconds / (double)_itemsProcessed;
            _collector.RecordPerformanceMetrics(_nodeId, throughput, avgTimeMs);
        }

        // Always record node end when timing is enabled to ensure DurationMs is set
        if (_options.RecordTiming)
        {
            long? peakMemoryMb = null;

            if (_options.RecordMemoryUsage)
            {
                var currentMemory = GC.GetTotalMemory(false);
                peakMemoryMb = Math.Max(currentMemory, _initialMemoryBytes) / 1024 / 1024;
            }

            _collector.RecordNodeEnd(
                _nodeId,
                DateTimeOffset.UtcNow,
                _success,
                _exception,
                peakMemoryMb);
        }
    }
}

using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_05_ParallelProcessing.Nodes;

/// <summary>
///     Transform node that monitors and tracks performance metrics for processed work items.
///     This node demonstrates thread-safe metrics collection and performance analysis.
/// </summary>
public class PerformanceMonitoringTransform : TransformNode<ProcessedWorkItem, ProcessedWorkItem>
{
    private static readonly object MetricsLock = new();
    private static readonly List<PerformanceMetric> Metrics = new();
    private static int _processedCount;
    private static long _totalProcessingTimeMs;
    private static readonly Dictionary<int, int> ThreadUsageCount = new();

    /// <summary>
    ///     Tracks performance metrics for the processed work item and passes it through.
    /// </summary>
    /// <param name="item">The processed work item to monitor.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task containing the processed work item with recorded metrics.</returns>
    public override async Task<ProcessedWorkItem> ExecuteAsync(ProcessedWorkItem item, PipelineContext context, CancellationToken cancellationToken)
    {
        var startTime = DateTime.UtcNow;
        var threadId = Thread.CurrentThread.ManagedThreadId;

        // Create performance metric for this work item
        var metric = new PerformanceMetric(
            item.Id,
            nameof(PerformanceMonitoringTransform),
            startTime,
            startTime.AddMilliseconds(item.ProcessingTimeMs),
            item.ProcessingTimeMs,
            threadId
        );

        // Thread-safe metrics collection
        lock (MetricsLock)
        {
            Metrics.Add(metric);
            _processedCount++;
            _totalProcessingTimeMs += item.ProcessingTimeMs;

            ThreadUsageCount.TryAdd(threadId, 0);
            ThreadUsageCount[threadId]++;
        }

        Console.WriteLine($"[Monitor] {item.Id}: {item.ProcessingTimeMs}ms on thread {threadId}");

        // Small delay to simulate monitoring overhead
        await Task.Delay(1, cancellationToken);

        return item;
    }

    /// <summary>
    ///     Gets a summary of collected performance metrics.
    /// </summary>
    /// <returns>A formatted string containing performance statistics.</returns>
    public static string GetPerformanceSummary()
    {
        lock (MetricsLock)
        {
            if (_processedCount == 0)
                return "No performance metrics available.";

            var avgProcessingTime = _totalProcessingTimeMs / (double)_processedCount;
            var minProcessingTime = Metrics.Min(m => m.DurationMs);
            var maxProcessingTime = Metrics.Max(m => m.DurationMs);
            var threadsUsed = ThreadUsageCount.Count;
            var mostUsedThread = ThreadUsageCount.OrderByDescending(kvp => kvp.Value).First();

            return $@"
=== Performance Metrics Summary ===
Total Items Processed: {_processedCount}
Average Processing Time: {avgProcessingTime:F2}ms
Min Processing Time: {minProcessingTime}ms
Max Processing Time: {maxProcessingTime}ms
Total Processing Time: {_totalProcessingTimeMs}ms
Threads Used: {threadsUsed}
Most Active Thread: {mostUsedThread.Key} ({mostUsedThread.Value} items)
Thread Distribution: {string.Join(", ", ThreadUsageCount.OrderBy(kvp => kvp.Key).Select(kvp => $"T{kvp.Key}:{kvp.Value}"))}
";
        }
    }

    /// <summary>
    ///     Resets all collected metrics.
    /// </summary>
    public static void ResetMetrics()
    {
        lock (MetricsLock)
        {
            Metrics.Clear();
            _processedCount = 0;
            _totalProcessingTimeMs = 0;
            ThreadUsageCount.Clear();
        }
    }
}

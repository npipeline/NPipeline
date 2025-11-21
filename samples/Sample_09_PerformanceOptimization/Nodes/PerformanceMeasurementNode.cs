using System.Diagnostics;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_09_PerformanceOptimization.Nodes;

/// <summary>
///     Sink node that measures and reports performance metrics for benchmarking.
///     This node collects performance data and generates comparison reports.
/// </summary>
public class PerformanceMeasurementNode : SinkNode<ProcessedPerformanceItem>
{
    private readonly Dictionary<string, List<PerformanceMetrics>> _metricsByApproach = new();
    private readonly List<ProcessedPerformanceItem> _processedItems = new();
    private readonly Stopwatch _totalProcessingTime = new();
    private long _totalMemoryBefore;

    /// <summary>
    ///     Gets all collected processed items.
    /// </summary>
    public IReadOnlyList<ProcessedPerformanceItem> ProcessedItems => _processedItems.AsReadOnly();

    /// <summary>
    ///     Gets performance metrics grouped by approach.
    /// </summary>
    public IReadOnlyDictionary<string, List<PerformanceMetrics>> MetricsByApproach => _metricsByApproach.ToDictionary(
        kvp => kvp.Key,
        kvp => new List<PerformanceMetrics>(kvp.Value));

    /// <summary>
    ///     Processes the input items and collects performance metrics.
    /// </summary>
    public override async Task ExecuteAsync(IDataPipe<ProcessedPerformanceItem> input, PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine("Starting performance measurement...");
        _totalMemoryBefore = GC.GetTotalMemory(false);
        _totalProcessingTime.Start();

        var itemCount = 0;

        // Use await foreach to consume all messages from the input pipe
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            _processedItems.Add(item);
            itemCount++;

            // Group metrics by approach for analysis
            var approachName = GetApproachName(item);

            if (!_metricsByApproach.TryGetValue(approachName, out var metricsList))
            {
                metricsList = new List<PerformanceMetrics>();
                _metricsByApproach[approachName] = metricsList;
            }

            metricsList.Add(CreatePerformanceMetrics(item, approachName));

            // Report progress every 100 items
            if (itemCount % 100 == 0)
                Console.WriteLine($"Processed {itemCount} items...");
        }

        _totalProcessingTime.Stop();
        var totalMemoryAfter = GC.GetTotalMemory(false);

        Console.WriteLine($"Performance measurement completed. Processed {itemCount} items in {_totalProcessingTime.ElapsedMilliseconds}ms");
        Console.WriteLine($"Total memory change: {totalMemoryAfter - _totalMemoryBefore:N0} bytes");

        // Generate performance report
        await GeneratePerformanceReportAsync(cancellationToken);
    }

    /// <summary>
    ///     Generates a comprehensive performance report.
    /// </summary>
    private async Task GeneratePerformanceReportAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("\n=== PERFORMANCE OPTIMIZATION REPORT ===\n");

        // Generate comparison benchmarks
        var benchmark = GenerateBenchmarkComparison();
        Console.WriteLine(benchmark.ToString());

        // Generate detailed statistics
        await GenerateDetailedStatisticsAsync(cancellationToken);

        // Generate recommendations
        GenerateRecommendations();
    }

    /// <summary>
    ///     Generates benchmark comparison between different approaches.
    /// </summary>
    private BenchmarkComparison GenerateBenchmarkComparison()
    {
        var taskMetrics = GetAverageMetrics("Task");
        var valueTaskMetrics = GetAverageMetrics("ValueTask");
        var syncMetrics = GetAverageMetrics("Sync");
        var memoryOptimizedMetrics = GetAverageMetrics("MemoryOptimized");

        return new BenchmarkComparison
        {
            TestName = "Performance Optimization Comparison",
            TaskBasedMetrics = taskMetrics,
            ValueTaskBasedMetrics = valueTaskMetrics,
            SynchronousFastPathMetrics = syncMetrics,
            MemoryOptimizedMetrics = memoryOptimizedMetrics,
        };
    }

    /// <summary>
    ///     Gets average performance metrics for a specific approach.
    /// </summary>
    private PerformanceMetrics GetAverageMetrics(string approachName)
    {
        if (!_metricsByApproach.TryGetValue(approachName, out var metrics) || metrics.Count == 0)
        {
            return new PerformanceMetrics
            {
                OperationName = approachName,
                ItemsProcessed = 0,
                ElapsedMilliseconds = 0,
                MemoryBeforeBytes = 0,
                MemoryAfterBytes = 0,
            };
        }

        return new PerformanceMetrics
        {
            OperationName = approachName,
            ItemsProcessed = metrics.Sum(m => m.ItemsProcessed),
            ElapsedMilliseconds = (long)metrics.Average(m => m.ElapsedMilliseconds),
            ElapsedTicks = (long)metrics.Average(m => m.ElapsedTicks),
            MemoryBeforeBytes = (long)metrics.Average(m => m.MemoryBeforeBytes),
            MemoryAfterBytes = (long)metrics.Average(m => m.MemoryAfterBytes),
            IsSynchronousPath = metrics.Any(m => m.IsSynchronousPath),
            UsesValueTask = metrics.Any(m => m.UsesValueTask),
        };
    }

    /// <summary>
    ///     Generates detailed statistics for each approach.
    /// </summary>
    private async Task GenerateDetailedStatisticsAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("\n--- DETAILED STATISTICS ---\n");

        foreach (var kvp in _metricsByApproach)
        {
            var approach = kvp.Key;
            var metrics = kvp.Value;

            if (metrics.Count == 0)
                continue;

            var avgTime = metrics.Average(m => m.ElapsedMilliseconds);
            var avgMemory = metrics.Average(m => m.MemoryDeltaBytes);
            var totalItems = metrics.Sum(m => m.ItemsProcessed);
            var syncOperations = metrics.Count(m => m.IsSynchronousPath);
            var valueTaskOperations = metrics.Count(m => m.UsesValueTask);

            Console.WriteLine($"{approach} Approach:");
            Console.WriteLine($"  Items processed: {totalItems:N0}");
            Console.WriteLine($"  Average time: {avgTime:F2}ms");
            Console.WriteLine($"  Average memory: {avgMemory:N0} bytes");
            Console.WriteLine($"  Synchronous operations: {syncOperations} ({(double)syncOperations / metrics.Count * 100:F1}%)");
            Console.WriteLine($"  ValueTask operations: {valueTaskOperations} ({(double)valueTaskOperations / metrics.Count * 100:F1}%)");
            Console.WriteLine();

            // Simulate async work for report generation
            await Task.Delay(10, cancellationToken);
        }
    }

    /// <summary>
    ///     Generates performance optimization recommendations.
    /// </summary>
    private void GenerateRecommendations()
    {
        Console.WriteLine("--- PERFORMANCE RECOMMENDATIONS ---\n");

        var taskMetrics = GetAverageMetrics("Task");
        var valueTaskMetrics = GetAverageMetrics("ValueTask");
        var syncMetrics = GetAverageMetrics("Sync");
        var memoryOptimizedMetrics = GetAverageMetrics("MemoryOptimized");

        // ValueTask recommendations
        if (valueTaskMetrics.AverageMicrosecondsPerItem < taskMetrics.AverageMicrosecondsPerItem)
        {
            var improvement = taskMetrics.AverageMicrosecondsPerItem - valueTaskMetrics.AverageMicrosecondsPerItem;
            Console.WriteLine($"✓ ValueTask optimization shows {improvement:F2}μs improvement per item");
            Console.WriteLine("  Recommendation: Use ValueTask for operations that may complete synchronously");
        }

        // Synchronous fast path recommendations
        if (syncMetrics.AverageMicrosecondsPerItem < taskMetrics.AverageMicrosecondsPerItem)
        {
            var improvement = taskMetrics.AverageMicrosecondsPerItem - syncMetrics.AverageMicrosecondsPerItem;
            Console.WriteLine($"✓ Synchronous fast path shows {improvement:F2}μs improvement per item");
            Console.WriteLine("  Recommendation: Implement synchronous paths for simple operations");
        }

        // Memory optimization recommendations
        if (Math.Abs(memoryOptimizedMetrics.MemoryDeltaBytes) < Math.Abs(taskMetrics.MemoryDeltaBytes))
        {
            var memoryImprovement = Math.Abs(taskMetrics.MemoryDeltaBytes) - Math.Abs(memoryOptimizedMetrics.MemoryDeltaBytes);
            Console.WriteLine($"✓ Memory optimization saves {memoryImprovement:N0} bytes per operation");
            Console.WriteLine("  Recommendation: Use array pooling and span-based operations");
        }

        Console.WriteLine("\n--- GENERAL OPTIMIZATION TIPS ---\n");
        Console.WriteLine("1. Use ValueTask for methods that may complete synchronously");
        Console.WriteLine("2. Implement synchronous fast paths for simple operations");
        Console.WriteLine("3. Use ArrayPool<T>.Shared for temporary buffer allocations");
        Console.WriteLine("4. Use Span<T> and Memory<T> for zero-allocation operations");
        Console.WriteLine("5. Cache frequently used results to avoid recomputation");
        Console.WriteLine("6. Consider stack allocation for small, temporary buffers");
        Console.WriteLine("7. Profile memory allocations to identify optimization opportunities");
        Console.WriteLine();
    }

    /// <summary>
    ///     Gets the approach name for a processed item.
    /// </summary>
    private static string GetApproachName(ProcessedPerformanceItem item)
    {
        if (item.UsedSynchronousPath && item.UsedValueTask)
            return "Sync";

        if (item.UsedValueTask)
            return "ValueTask";

        if (item.ProcessingComplexity <= 3)
            return "MemoryOptimized";

        return "Task";
    }

    /// <summary>
    ///     Creates performance metrics from a processed item.
    /// </summary>
    private PerformanceMetrics CreatePerformanceMetrics(ProcessedPerformanceItem item, string approachName)
    {
        return new PerformanceMetrics
        {
            OperationName = approachName,
            ElapsedMilliseconds = (long)item.ProcessingTime.TotalMilliseconds,
            ElapsedTicks = item.ProcessingTime.Ticks,
            MemoryBeforeBytes = 0, // We don't track this per-item
            MemoryAfterBytes = item.MemoryAllocatedBytes,
            ItemsProcessed = 1,
            IsSynchronousPath = item.UsedSynchronousPath,
            UsesValueTask = item.UsedValueTask,
        };
    }

    /// <summary>
    ///     Resets all collected metrics for fresh benchmarking.
    /// </summary>
    public void ResetMetrics()
    {
        _processedItems.Clear();
        _metricsByApproach.Clear();
        _totalProcessingTime.Reset();
        _totalMemoryBefore = 0;
    }
}

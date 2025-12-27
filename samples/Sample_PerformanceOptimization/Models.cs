namespace Sample_PerformanceOptimization;

/// <summary>
///     Represents performance measurement data for benchmarking operations.
/// </summary>
public record PerformanceMetrics
{
    public string OperationName { get; init; } = string.Empty;
    public long ElapsedMilliseconds { get; init; }
    public long ElapsedTicks { get; init; }
    public long MemoryBeforeBytes { get; init; }
    public long MemoryAfterBytes { get; init; }
    public long MemoryDeltaBytes => MemoryAfterBytes - MemoryBeforeBytes;
    public int ItemsProcessed { get; init; }

    public double AverageMicrosecondsPerItem => ItemsProcessed > 0
        ? ElapsedMilliseconds * 1000.0 / ItemsProcessed
        : 0;

    public bool IsSynchronousPath { get; init; }
    public bool UsesValueTask { get; init; }

    public override string ToString()
    {
        return $"{OperationName}: {ElapsedMilliseconds}ms, {MemoryDeltaBytes} bytes, {ItemsProcessed} items, " +
               $"{AverageMicrosecondsPerItem:F2}μs/item, {(IsSynchronousPath ? "Sync" : "Async")}, " +
               $"{(UsesValueTask ? "ValueTask" : "Task")}";
    }
}

/// <summary>
///     Represents a data item for processing in the performance optimization pipeline.
/// </summary>
public record PerformanceDataItem
{
    public int Id { get; init; }
    public string Data { get; init; } = string.Empty;
    public int ProcessingComplexity { get; init; } // 1-10, higher means more complex
    public DateTime CreatedAt { get; init; } = DateTime.UtcNow;
    public bool ShouldUseSynchronousPath { get; init; }
    public bool ShouldUseValueTask { get; init; }

    public static PerformanceDataItem CreateRandom(int id, Random? random = null)
    {
        random ??= new Random();
        var complexity = random.Next(1, 11);
        var data = new string('A', random.Next(10, 100));

        return new PerformanceDataItem
        {
            Id = id,
            Data = data,
            ProcessingComplexity = complexity,
            ShouldUseSynchronousPath = complexity <= 3, // Simple items use sync path
            ShouldUseValueTask = random.NextDouble() > 0.5, // Randomly choose ValueTask or Task
        };
    }
}

/// <summary>
///     Represents the result of processing a performance data item.
/// </summary>
public record ProcessedPerformanceItem
{
    public int OriginalId { get; init; }
    public string ProcessedData { get; init; } = string.Empty;
    public TimeSpan ProcessingTime { get; init; }
    public bool UsedSynchronousPath { get; init; }
    public bool UsedValueTask { get; init; }
    public int ProcessingComplexity { get; init; }
    public long MemoryAllocatedBytes { get; init; }
}

/// <summary>
///     Represents benchmark comparison results between different optimization approaches.
/// </summary>
public record BenchmarkComparison
{
    public string TestName { get; init; } = string.Empty;
    public PerformanceMetrics TaskBasedMetrics { get; init; } = new();
    public PerformanceMetrics ValueTaskBasedMetrics { get; init; } = new();
    public PerformanceMetrics SynchronousFastPathMetrics { get; init; } = new();
    public PerformanceMetrics MemoryOptimizedMetrics { get; init; } = new();

    public double TaskVsValueTaskSpeedImprovement => CalculateSpeedImprovement(TaskBasedMetrics, ValueTaskBasedMetrics);
    public double TaskVsSyncSpeedImprovement => CalculateSpeedImprovement(TaskBasedMetrics, SynchronousFastPathMetrics);
    public double TaskVsMemoryOptimizedSpeedImprovement => CalculateSpeedImprovement(TaskBasedMetrics, MemoryOptimizedMetrics);

    public long TaskVsValueTaskMemoryImprovement => CalculateMemoryImprovement(TaskBasedMetrics, ValueTaskBasedMetrics);
    public long TaskVsSyncMemoryImprovement => CalculateMemoryImprovement(TaskBasedMetrics, SynchronousFastPathMetrics);
    public long TaskVsMemoryOptimizedMemoryImprovement => CalculateMemoryImprovement(TaskBasedMetrics, MemoryOptimizedMetrics);

    private static double CalculateSpeedImprovement(PerformanceMetrics baseline, PerformanceMetrics optimized)
    {
        if (baseline.AverageMicrosecondsPerItem <= 0)
            return 0;

        return (baseline.AverageMicrosecondsPerItem - optimized.AverageMicrosecondsPerItem) / baseline.AverageMicrosecondsPerItem * 100;
    }

    private static long CalculateMemoryImprovement(PerformanceMetrics baseline, PerformanceMetrics optimized)
    {
        return baseline.MemoryDeltaBytes - optimized.MemoryDeltaBytes;
    }

    public override string ToString()
    {
        return $"""
                Benchmark Results for {TestName}:
                Task-based: {TaskBasedMetrics}
                ValueTask-based: {ValueTaskBasedMetrics} ({TaskVsValueTaskSpeedImprovement:F1}% faster, {TaskVsValueTaskMemoryImprovement} bytes saved)
                Synchronous Fast Path: {SynchronousFastPathMetrics} ({TaskVsSyncSpeedImprovement:F1}% faster, {TaskVsSyncMemoryImprovement} bytes saved)
                Memory Optimized: {MemoryOptimizedMetrics} ({TaskVsMemoryOptimizedSpeedImprovement:F1}% faster, {TaskVsMemoryOptimizedMemoryImprovement} bytes saved)
                """;
    }
}

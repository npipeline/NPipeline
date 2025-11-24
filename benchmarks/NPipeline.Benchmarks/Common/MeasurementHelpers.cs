using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace NPipeline.Benchmarks.Common;

/// <summary>
///     Helper class for custom metrics collection in benchmarks.
/// </summary>
public static class MeasurementHelpers
{
    /// <summary>
    ///     Measures the time to first item in a pipeline.
    /// </summary>
    public static async Task<TimeSpan> MeasureTimeToFirstItem<T>(
        Func<Task<IAsyncEnumerable<T>>> pipelineFactory,
        CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();
        var enumerable = await pipelineFactory();

        await foreach (var _ in enumerable.WithCancellation(cancellationToken))
        {
            stopwatch.Stop();
            return stopwatch.Elapsed;
        }

        stopwatch.Stop();
        return stopwatch.Elapsed;
    }

    /// <summary>
    ///     Measures the p99 latency (99th percentile) of item processing.
    /// </summary>
    public static async Task<TimeSpan> MeasureP99Latency<T>(
        Func<Task<IAsyncEnumerable<T>>> pipelineFactory,
        CancellationToken cancellationToken = default)
    {
        var latencies = new List<long>();
        var stopwatch = new Stopwatch();

        var enumerable = await pipelineFactory();

        await foreach (var _ in enumerable.WithCancellation(cancellationToken))
        {
            stopwatch.Stop();
            latencies.Add(stopwatch.ElapsedTicks);
            stopwatch.Restart();
        }

        if (latencies.Count == 0)
            return TimeSpan.Zero;

        // Calculate p99 (99th percentile)
        var sortedLatencies = latencies.OrderBy(x => x).ToList();
        var p99Index = (int)Math.Ceiling(sortedLatencies.Count * 0.99) - 1;
        p99Index = Math.Max(0, Math.Min(p99Index, sortedLatencies.Count - 1));

        return TimeSpan.FromTicks(sortedLatencies[p99Index]);
    }

    /// <summary>
    ///     Measures throughput (items per second) for a pipeline.
    /// </summary>
    public static async Task<double> MeasureThroughput<T>(
        Func<Task<IAsyncEnumerable<T>>> pipelineFactory,
        CancellationToken cancellationToken = default)
    {
        var itemCount = 0;
        var stopwatch = Stopwatch.StartNew();

        var enumerable = await pipelineFactory();

        await foreach (var _ in enumerable.WithCancellation(cancellationToken))
        {
            itemCount++;
        }

        stopwatch.Stop();

        return stopwatch.Elapsed.TotalSeconds > 0
            ? itemCount / stopwatch.Elapsed.TotalSeconds
            : 0;
    }

    /// <summary>
    ///     Measures memory allocation during pipeline execution.
    /// </summary>
    public static async Task<MemoryMetrics> MeasureMemoryUsage<T>(
        Func<Task<IAsyncEnumerable<T>>> pipelineFactory,
        CancellationToken cancellationToken = default)
    {
        // Force GC before measurement
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        var initialMemory = GC.GetTotalMemory(false);
        var initialGen0 = GC.CollectionCount(0);
        var initialGen1 = GC.CollectionCount(1);
        var initialGen2 = GC.CollectionCount(2);

        var enumerable = await pipelineFactory();

        await foreach (var _ in enumerable.WithCancellation(cancellationToken))
        {
            // Process items
        }

        // Force GC after measurement
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        var finalMemory = GC.GetTotalMemory(false);
        var finalGen0 = GC.CollectionCount(0);
        var finalGen1 = GC.CollectionCount(1);
        var finalGen2 = GC.CollectionCount(2);

        return new MemoryMetrics
        {
            AllocatedBytes = Math.Max(0, finalMemory - initialMemory),
            Gen0Collections = finalGen0 - initialGen0,
            Gen1Collections = finalGen1 - initialGen1,
            Gen2Collections = finalGen2 - initialGen2,
            PeakMemoryUsage = initialMemory, // This would need more sophisticated tracking in real scenarios
        };
    }

    /// <summary>
    ///     Measures sustained performance over multiple iterations.
    /// </summary>
    public static async Task<SustainedPerformanceMetrics> MeasureSustainedPerformance<T>(
        Func<Task<IAsyncEnumerable<T>>> pipelineFactory,
        int iterations = 5,
        CancellationToken cancellationToken = default)
    {
        var throughputs = new List<double>();
        var memoryUsages = new List<MemoryMetrics>();

        for (var i = 0; i < iterations; i++)
        {
            // Measure throughput
            var throughput = await MeasureThroughput(pipelineFactory, cancellationToken);
            throughputs.Add(throughput);

            // Measure memory usage
            var memoryUsage = await MeasureMemoryUsage(pipelineFactory, cancellationToken);
            memoryUsages.Add(memoryUsage);

            // Small delay between iterations
            await Task.Delay(100, cancellationToken);
        }

        return new SustainedPerformanceMetrics
        {
            AverageThroughput = throughputs.Average(),
            MinThroughput = throughputs.Min(),
            MaxThroughput = throughputs.Max(),
            ThroughputStandardDeviation = CalculateStandardDeviation(throughputs),
            AverageMemoryAllocation = memoryUsages.Average(m => m.AllocatedBytes),
            TotalGen0Collections = memoryUsages.Sum(m => m.Gen0Collections),
            TotalGen1Collections = memoryUsages.Sum(m => m.Gen1Collections),
            TotalGen2Collections = memoryUsages.Sum(m => m.Gen2Collections),
        };
    }

    /// <summary>
    ///     Creates a latency measurement wrapper for individual items.
    /// </summary>
    public static async IAsyncEnumerable<LatencyMeasuredItem<T>> MeasureItemLatency<T>(
        IAsyncEnumerable<T> source,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var stopwatch = new Stopwatch();

        await foreach (var item in source.WithCancellation(cancellationToken))
        {
            stopwatch.Restart();
            yield return new LatencyMeasuredItem<T>(item, stopwatch.Elapsed);

            stopwatch.Stop();
        }
    }

    private static double CalculateStandardDeviation(IEnumerable<double> values)
    {
        var valuesList = values.ToList();

        if (valuesList.Count <= 1)
            return 0;

        var mean = valuesList.Average();
        var sumOfSquares = valuesList.Sum(x => Math.Pow(x - mean, 2));
        return Math.Sqrt(sumOfSquares / (valuesList.Count - 1));
    }
}

/// <summary>
///     Represents memory usage metrics during pipeline execution.
/// </summary>
public record MemoryMetrics
{
    public long AllocatedBytes { get; init; }
    public int Gen0Collections { get; init; }
    public int Gen1Collections { get; init; }
    public int Gen2Collections { get; init; }
    public long PeakMemoryUsage { get; init; }

    public double AllocatedMB => AllocatedBytes / (1024.0 * 1024.0);
    public double PeakMemoryMB => PeakMemoryUsage / (1024.0 * 1024.0);
}

/// <summary>
///     Represents sustained performance metrics over multiple iterations.
/// </summary>
public record SustainedPerformanceMetrics
{
    public double AverageThroughput { get; init; }
    public double MinThroughput { get; init; }
    public double MaxThroughput { get; init; }
    public double ThroughputStandardDeviation { get; init; }
    public double AverageMemoryAllocation { get; init; }
    public int TotalGen0Collections { get; init; }
    public int TotalGen1Collections { get; init; }
    public int TotalGen2Collections { get; init; }

    public double AverageMemoryAllocationMB => AverageMemoryAllocation / (1024.0 * 1024.0);
    public double ThroughputVariability => ThroughputStandardDeviation / AverageThroughput;
}

/// <summary>
///     Represents an item with measured latency.
/// </summary>
public record LatencyMeasuredItem<T>(T Item, TimeSpan Latency);

using System.Diagnostics;
using BenchmarkDotNet.Attributes;
using NPipeline.Execution.RetryDelay;

namespace Sample_RetryDelay;

/// <summary>
///     Compares performance characteristics of different retry delay strategies.
/// </summary>
public static class PerformanceComparison
{
    private static CompositeRetryDelayStrategy? _exponentialFullJitter;
    private static CompositeRetryDelayStrategy? _linearEqualJitter;
    private static CompositeRetryDelayStrategy? _fixedNoJitter;
    private static CompositeRetryDelayStrategy? _exponentialEqualJitter;
    private static CompositeRetryDelayStrategy? _linearFullJitter;

    /// <summary>
    ///     Runs all performance comparison examples.
    /// </summary>
    public static async Task RunAllExamples()
    {
        await CompareDelayDistributions();
        await CompareMemoryUsage();
        await CompareExecutionSpeed();
        await CompareScalability();
        await AnalyzeDistributionPatterns();
    }

    /// <summary>
    ///     Runs performance benchmarks.
    /// </summary>
    public static void StrategyPerformanceBenchmarks()
    {
        Console.WriteLine("=== Performance Benchmarks ===");
        Console.WriteLine("Note: BenchmarkDotNet is not available in this sample project.");
        Console.WriteLine("To run benchmarks, use the NPipeline.Benchmarks project instead.");
    }

    /// <summary>
    ///     Initializes retry strategies for comparison.
    /// </summary>
    public static void Initialize()
    {
        _exponentialFullJitter = new CompositeRetryDelayStrategy(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(100),
                2.0,
                TimeSpan.FromSeconds(30)),
            JitterStrategies.FullJitter(),
            new Random(42));

        _linearEqualJitter = new CompositeRetryDelayStrategy(
            BackoffStrategies.LinearBackoff(
                TimeSpan.FromMilliseconds(100),
                TimeSpan.FromMilliseconds(50),
                TimeSpan.FromSeconds(30)),
            JitterStrategies.EqualJitter(),
            new Random(42));

        _fixedNoJitter = new CompositeRetryDelayStrategy(
            BackoffStrategies.FixedDelay(TimeSpan.FromMilliseconds(1000)),
            JitterStrategies.NoJitter(),
            new Random(42));

        _exponentialEqualJitter = new CompositeRetryDelayStrategy(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(100),
                2.0,
                TimeSpan.FromSeconds(30)),
            JitterStrategies.EqualJitter(),
            new Random(42));

        _linearFullJitter = new CompositeRetryDelayStrategy(
            BackoffStrategies.LinearBackoff(
                TimeSpan.FromMilliseconds(100),
                TimeSpan.FromMilliseconds(50),
                TimeSpan.FromSeconds(30)),
            JitterStrategies.FullJitter(),
            new Random(42));
    }

    /// <summary>
    ///     Compares delay distributions across different strategies.
    /// </summary>
    public static async Task CompareDelayDistributions()
    {
        Console.WriteLine("=== Delay Distribution Comparison ===");
        Initialize();

        var strategies = new[]
        {
            new { Name = "Exponential + Full Jitter", Strategy = _exponentialFullJitter! },
            new { Name = "Linear + Equal Jitter", Strategy = _linearEqualJitter! },
            new { Name = "Fixed + No Jitter", Strategy = _fixedNoJitter! },
            new { Name = "Exponential + Equal Jitter", Strategy = _exponentialEqualJitter! },
            new { Name = "Linear + Full Jitter", Strategy = _linearFullJitter! },
        };

        foreach (var strategyConfig in strategies)
        {
            Console.WriteLine($"\n{strategyConfig.Name}:");
            var delays = new List<TimeSpan>();

            for (var i = 0; i < 1000; i++)
            {
                delays.Add(await strategyConfig.Strategy.GetDelayAsync(i % 10));
            }

            var (mean, median, stdDev, min, max, p95) = CalculateStatistics(delays);
            Console.WriteLine($"  Mean: {mean.TotalMilliseconds:F2}ms");
            Console.WriteLine($"  Median: {median.TotalMilliseconds:F2}ms");
            Console.WriteLine($"  StdDev: {stdDev.TotalMilliseconds:F2}ms");
            Console.WriteLine($"  Min: {min.TotalMilliseconds:F2}ms");
            Console.WriteLine($"  Max: {max.TotalMilliseconds:F2}ms");
            Console.WriteLine($"  95th percentile: {p95.TotalMilliseconds:F2}ms");
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Compares memory usage of different strategies.
    /// </summary>
    public static async Task CompareMemoryUsage()
    {
        Console.WriteLine("=== Memory Usage Comparison ===");

        const int iterations = 100000;

        var strategies = new[]
        {
            ("Exponential + Full Jitter", CreateExponentialFullJitterStrategy()),
            ("Linear + Equal Jitter", CreateLinearEqualJitterStrategy()),
            ("Fixed + No Jitter", CreateFixedNoJitterStrategy()),
        };

        foreach (var (name, strategy) in strategies)
        {
            // Warm up
            for (var i = 0; i < 100; i++)
            {
                _ = strategy.GetDelayAsync(i).AsTask();
            }

            // Measure memory
            var initialMemory = GC.GetTotalMemory(true);

            for (var i = 0; i < iterations; i++)
            {
                _ = strategy.GetDelayAsync(i % 10).AsTask();
            }

            var finalMemory = GC.GetTotalMemory(true);
            var memoryUsed = finalMemory - initialMemory;

            Console.WriteLine($"{name}:");
            Console.WriteLine($"  Memory per operation: {(double)memoryUsed / iterations:F2} bytes");
            Console.WriteLine($"  Total memory used: {memoryUsed:N0} bytes");
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Compares execution speed of different strategies.
    /// </summary>
    public static async Task CompareExecutionSpeed()
    {
        Console.WriteLine("=== Execution Speed Comparison ===");

        const int iterations = 1000000;

        var strategies = new[]
        {
            ("Exponential + Full Jitter", CreateExponentialFullJitterStrategy()),
            ("Linear + Equal Jitter", CreateLinearEqualJitterStrategy()),
            ("Fixed + No Jitter", CreateFixedNoJitterStrategy()),
        };

        foreach (var (name, strategy) in strategies)
        {
            var stopwatch = Stopwatch.StartNew();

            for (var i = 0; i < iterations; i++)
            {
                _ = strategy.GetDelayAsync(i % 10).AsTask();
            }

            stopwatch.Stop();

            var operationsPerSecond = iterations / stopwatch.Elapsed.TotalSeconds;

            Console.WriteLine($"{name}:");
            Console.WriteLine($"  Operations per second: {operationsPerSecond:N0}");
            Console.WriteLine($"  Average time per operation: {stopwatch.Elapsed.TotalNanoseconds / iterations:F2} ns");
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Compares scalability of different strategies.
    /// </summary>
    public static async Task CompareScalability()
    {
        Console.WriteLine("=== Scalability Comparison ===");

        var attemptCounts = new[] { 10, 100, 1000, 10000 };

        var strategies = new[]
        {
            ("Exponential", CreateExponentialStrategy()),
            ("Linear", CreateLinearStrategy()),
            ("Fixed", CreateFixedStrategy()),
        };

        Console.WriteLine("Strategy\tAttempts\tTime (ms)\tMemory (bytes)");

        foreach (var (strategyName, strategy) in strategies)
        {
            foreach (var attemptCount in attemptCounts)
            {
                var stopwatch = Stopwatch.StartNew();
                var initialMemory = GC.GetTotalMemory(false);

                for (var i = 0; i < attemptCount; i++)
                {
                    _ = strategy(i);
                }

                stopwatch.Stop();
                var finalMemory = GC.GetTotalMemory(false);
                var memoryUsed = finalMemory - initialMemory;

                Console.WriteLine($"{strategyName}\t{attemptCount}\t{stopwatch.Elapsed.TotalMilliseconds:F2}\t{memoryUsed}");
            }
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Analyzes distribution patterns of different strategies.
    /// </summary>
    public static async Task AnalyzeDistributionPatterns()
    {
        Console.WriteLine("=== Distribution Pattern Analysis ===");

        var strategies = new[]
        {
            ("Exponential + Full Jitter", CreateExponentialFullJitterStrategy()),
            ("Linear + Equal Jitter", CreateLinearEqualJitterStrategy()),
            ("Fixed + No Jitter", CreateFixedNoJitterStrategy()),
        };

        foreach (var (name, strategy) in strategies)
        {
            Console.WriteLine($"\n{name}:");
            var delays = new List<TimeSpan>();

            for (var i = 0; i < 10000; i++)
            {
                delays.Add(await strategy.GetDelayAsync(i % 20));
            }

            // Group into buckets
            var buckets = new (string Name, Func<TimeSpan, bool> Predicate)[]
            {
                ("0-100ms", d => d.TotalMilliseconds <= 100),
                ("100-500ms", d => d.TotalMilliseconds > 100 && d.TotalMilliseconds <= 500),
                ("500ms-1s", d => d.TotalMilliseconds > 500 && d.TotalMilliseconds <= 1000),
                ("1s-5s", d => d.TotalMilliseconds > 1000 && d.TotalMilliseconds <= 5000),
                ("5s+", d => d.TotalMilliseconds > 5000),
            };

            foreach (var (bucketName, predicate) in buckets)
            {
                var count = delays.Count(predicate);
                var percentage = (double)count / delays.Count * 100;
                Console.WriteLine($"  {bucketName}: {count:N0} ({percentage:F1}%)");
            }
        }

        Console.WriteLine();
    }

    private static IRetryDelayStrategy CreateExponentialFullJitterStrategy()
    {
        return new CompositeRetryDelayStrategy(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(100),
                2.0,
                TimeSpan.FromSeconds(30)),
            JitterStrategies.FullJitter(),
            new Random(42));
    }

    private static IRetryDelayStrategy CreateLinearEqualJitterStrategy()
    {
        return new CompositeRetryDelayStrategy(
            BackoffStrategies.LinearBackoff(
                TimeSpan.FromMilliseconds(100),
                TimeSpan.FromMilliseconds(50),
                TimeSpan.FromSeconds(30)),
            JitterStrategies.EqualJitter(),
            new Random(42));
    }

    private static IRetryDelayStrategy CreateFixedNoJitterStrategy()
    {
        return new CompositeRetryDelayStrategy(
            BackoffStrategies.FixedDelay(TimeSpan.FromMilliseconds(1000)),
            JitterStrategies.NoJitter(),
            new Random(42));
    }

    private static BackoffStrategy CreateExponentialStrategy()
    {
        return BackoffStrategies.ExponentialBackoff(
            TimeSpan.FromMilliseconds(100),
            2.0,
            TimeSpan.FromSeconds(30));
    }

    private static BackoffStrategy CreateLinearStrategy()
    {
        return BackoffStrategies.LinearBackoff(
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromMilliseconds(50),
            TimeSpan.FromSeconds(30));
    }

    private static BackoffStrategy CreateFixedStrategy()
    {
        return BackoffStrategies.FixedDelay(TimeSpan.FromMilliseconds(1000));
    }

    private static (TimeSpan Mean, TimeSpan Median, TimeSpan StandardDeviation, TimeSpan Min, TimeSpan Max, TimeSpan P95) CalculateStatistics(
        IEnumerable<TimeSpan> values)
    {
        var sortedValues = values.OrderBy(v => v.TotalMilliseconds).ToList();
        var count = sortedValues.Count;

        var mean = TimeSpan.FromMilliseconds(sortedValues.Average(v => v.TotalMilliseconds));

        var median = count % 2 == 0
            ? TimeSpan.FromMilliseconds((sortedValues[count / 2 - 1].TotalMilliseconds + sortedValues[count / 2].TotalMilliseconds) / 2)
            : sortedValues[count / 2];

        var variance = sortedValues.Average(v => Math.Pow(v.TotalMilliseconds - mean.TotalMilliseconds, 2));
        var stdDev = TimeSpan.FromMilliseconds(Math.Sqrt(variance));

        var p95Index = (int)(count * 0.95);
        var p95 = sortedValues[Math.Min(p95Index, count - 1)];

        return (mean, median, stdDev, sortedValues.First(), sortedValues.Last(), p95);
    }
}

/// <summary>
///     BenchmarkDotNet benchmarks for retry delay strategies.
/// </summary>
[MemoryDiagnoser]
[SimpleJob]
public class RetryDelayBenchmarks
{
    private IRetryDelayStrategy _exponentialFullJitter = null!;
    private IRetryDelayStrategy _fixedNoJitter = null!;
    private IRetryDelayStrategy _linearEqualJitter = null!;
    private IRetryDelayStrategy _noOp = null!;

    [GlobalSetup]
    public void Setup()
    {
        _exponentialFullJitter = new CompositeRetryDelayStrategy(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(100),
                2.0,
                TimeSpan.FromSeconds(30)),
            JitterStrategies.FullJitter(),
            Random.Shared);

        _linearEqualJitter = new CompositeRetryDelayStrategy(
            BackoffStrategies.LinearBackoff(
                TimeSpan.FromMilliseconds(100),
                TimeSpan.FromMilliseconds(100),
                TimeSpan.FromSeconds(10)),
            JitterStrategies.EqualJitter(),
            Random.Shared);

        _fixedNoJitter = new CompositeRetryDelayStrategy(
            BackoffStrategies.FixedDelay(TimeSpan.FromMilliseconds(500)),
            JitterStrategies.NoJitter(),
            Random.Shared);

        _noOp = NoOpRetryDelayStrategy.Instance;
    }

    [Benchmark]
    public async Task<TimeSpan> ExponentialFullJitter()
    {
        return await _exponentialFullJitter.GetDelayAsync(5);
    }

    [Benchmark]
    public async Task<TimeSpan> LinearEqualJitter()
    {
        return await _linearEqualJitter.GetDelayAsync(5);
    }

    [Benchmark]
    public async Task<TimeSpan> FixedNoJitter()
    {
        return await _fixedNoJitter.GetDelayAsync(5);
    }

    [Benchmark]
    public async Task<TimeSpan> NoOp()
    {
        return await _noOp.GetDelayAsync(5);
    }

    [Benchmark]
    public async Task<List<TimeSpan>> ExponentialFullJitter_Multiple()
    {
        var delays = new List<TimeSpan>();

        for (var i = 0; i < 10; i++)
        {
            delays.Add(await _exponentialFullJitter.GetDelayAsync(i).AsTask());
        }

        return delays;
    }

    [Benchmark]
    public async Task<List<TimeSpan>> LinearEqualJitter_Multiple()
    {
        var delays = new List<TimeSpan>();

        for (var i = 0; i < 10; i++)
        {
            delays.Add(await _linearEqualJitter.GetDelayAsync(i).AsTask());
        }

        return delays;
    }

    [Benchmark]
    public async Task<List<TimeSpan>> FixedNoJitter_Multiple()
    {
        var delays = new List<TimeSpan>();

        for (var i = 0; i < 10; i++)
        {
            delays.Add(await _fixedNoJitter.GetDelayAsync(i).AsTask());
        }

        return delays;
    }

    [Benchmark]
    public async Task<List<TimeSpan>> NoOp_Multiple()
    {
        var delays = new List<TimeSpan>();

        for (var i = 0; i < 10; i++)
        {
            delays.Add(await _noOp.GetDelayAsync(i).AsTask());
        }

        return delays;
    }
}

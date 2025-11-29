using System.Diagnostics;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Microsoft.Extensions.Logging;
using NPipeline.Execution.RetryDelay;
using NPipeline.Execution.RetryDelay.Backoff;

namespace Sample_RetryDelay;

/// <summary>
///     Performance comparison examples for retry delay strategies.
///     Benchmarks different strategies and analyzes their performance characteristics.
/// </summary>
public static class PerformanceComparison
{
    private static readonly ILogger logger = LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger(typeof(PerformanceComparison));

    /// <summary>
    ///     Example 1: Strategy performance benchmarks.
    ///     Shows how different strategies perform in terms of calculation speed.
    /// </summary>
    public static void StrategyPerformanceBenchmarks()
    {
        Console.WriteLine("=== Strategy Performance Benchmarks ===");
        Console.WriteLine();
        Console.WriteLine("Running benchmarks for different retry delay strategies...");
        Console.WriteLine("This will test the performance of delay calculations only.");
        Console.WriteLine();

        // Run benchmarks
        var summary = BenchmarkRunner.Run<RetryDelayBenchmarks>();

        Console.WriteLine();
        Console.WriteLine("Benchmark completed. Check the results above for detailed performance metrics.");
        Console.WriteLine();
    }

    /// <summary>
    ///     Example 2: Throughput analysis under different load patterns.
    ///     Shows how retry strategies affect system throughput.
    /// </summary>
    public static async Task ThroughputAnalysisExample()
    {
        Console.WriteLine("=== Throughput Analysis Example ===");
        Console.WriteLine();

        var strategies = new[]
        {
            ("Exponential + Full Jitter", CreateExponentialFullJitterStrategy()),
            ("Linear + Equal Jitter", CreateLinearEqualJitterStrategy()),
            ("Fixed + No Jitter", CreateFixedNoJitterStrategy()),
            ("No Delay", NoOpRetryDelayStrategy.Instance),
        };

        var loadPatterns = new[]
        {
            ("Light Load", 10, 100), // 10 operations, 100ms between each
            ("Medium Load", 50, 50), // 50 operations, 50ms between each
            ("Heavy Load", 100, 10), // 100 operations, 10ms between each
        };

        foreach (var (loadName, operationCount, interval) in loadPatterns)
        {
            Console.WriteLine($"Load Pattern: {loadName} ({operationCount} operations, {interval}ms interval)");

            foreach (var (strategyName, strategy) in strategies)
            {
                var throughput = await MeasureThroughput(strategy, operationCount, interval);
                Console.WriteLine($"  {strategyName,-25}: {throughput:F2} ops/sec");
            }

            Console.WriteLine();
        }
    }

    /// <summary>
    ///     Example 3: Latency distribution analysis.
    ///     Shows how different strategies affect latency distribution.
    /// </summary>
    public static async Task LatencyDistributionAnalysis()
    {
        Console.WriteLine("=== Latency Distribution Analysis ===");
        Console.WriteLine();

        var strategies = new[]
        {
            ("Exponential + Full Jitter", CreateExponentialFullJitterStrategy()),
            ("Linear + Equal Jitter", CreateLinearEqualJitterStrategy()),
            ("Fixed + No Jitter", CreateFixedNoJitterStrategy()),
        };

        foreach (var (strategyName, strategy) in strategies)
        {
            Console.WriteLine($"Strategy: {strategyName}");

            var latencies = new List<TimeSpan>();

            // Collect latency data for multiple attempts
            for (var attempt = 0; attempt < 10; attempt++)
            {
                var delay = await strategy.GetDelayAsync(attempt);
                latencies.Add(delay);
            }

            // Calculate statistics
            var avgLatency = TimeSpan.FromTicks((long)latencies.Average(l => l.Ticks));
            var minLatency = latencies.Min();
            var maxLatency = latencies.Max();
            double variance = 0;

            foreach (var latency in latencies)
            {
                var diff = latency.TotalMilliseconds - avgLatency.TotalMilliseconds;
                variance += diff * diff;
            }

            variance /= latencies.Count;
            var stdDev = Math.Sqrt(variance);

            Console.WriteLine($"  Average: {avgLatency.TotalMilliseconds:F1}ms");
            Console.WriteLine($"  Min: {minLatency.TotalMilliseconds:F1}ms");
            Console.WriteLine($"  Max: {maxLatency.TotalMilliseconds:F1}ms");
            Console.WriteLine($"  Std Dev: {stdDev:F1}ms");
            Console.WriteLine($"  Range: {maxLatency.TotalMilliseconds - minLatency.TotalMilliseconds:F1}ms");
            Console.WriteLine();
        }
    }

    /// <summary>
    ///     Example 4: Memory and CPU usage comparison.
    ///     Shows resource consumption of different strategies.
    /// </summary>
    public static async Task ResourceUsageComparison()
    {
        Console.WriteLine("=== Memory and CPU Usage Comparison ===");
        Console.WriteLine();

        var strategies = new[]
        {
            ("Exponential + Full Jitter", CreateExponentialFullJitterStrategy()),
            ("Linear + Equal Jitter", CreateLinearEqualJitterStrategy()),
            ("Fixed + No Jitter", CreateFixedNoJitterStrategy()),
        };

        foreach (var (strategyName, strategy) in strategies)
        {
            // Measure memory before
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            var memoryBefore = GC.GetTotalMemory(false);

            // Perform many delay calculations
            var stopwatch = Stopwatch.StartNew();

            for (var i = 0; i < 10000; i++)
            {
                for (var attempt = 0; attempt < 10; attempt++)
                {
                    await strategy.GetDelayAsync(attempt);
                }
            }

            stopwatch.Stop();

            // Measure memory after
            var memoryAfter = GC.GetTotalMemory(false);
            var memoryUsed = memoryAfter - memoryBefore;

            Console.WriteLine($"Strategy: {strategyName}");
            Console.WriteLine($"  Time for 100,000 calculations: {stopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"  Memory used: {memoryUsed / 1024.0:F2} KB");
            Console.WriteLine($"  Average time per calculation: {(double)stopwatch.ElapsedMilliseconds / 100000:F4}ms");
            Console.WriteLine();
        }
    }

    /// <summary>
    ///     Example 5: Thundering herd simulation.
    ///     Shows how different jitter strategies prevent thundering herd problems.
    /// </summary>
    public static async Task ThunderingHerdSimulation()
    {
        Console.WriteLine("=== Thundering Herd Simulation ===");
        Console.WriteLine();

        var concurrentClients = 50;
        var attemptNumber = 3; // All clients on their 3rd retry

        var strategies = new[]
        {
            ("No Jitter", CreateExponentialNoJitterStrategy()),
            ("Equal Jitter", CreateExponentialEqualJitterStrategy()),
            ("Full Jitter", CreateExponentialFullJitterStrategy()),
            ("Decorrelated Jitter", CreateExponentialDecorrelatedJitterStrategy()),
        };

        foreach (var (strategyName, strategy) in strategies)
        {
            Console.WriteLine($"Strategy: {strategyName}");

            // Simulate concurrent clients all retrying at the same time
            var tasks = new List<Task<TimeSpan>>();

            for (var i = 0; i < concurrentClients; i++)
            {
                tasks.Add(strategy.GetDelayAsync(attemptNumber).AsTask());
            }

            var delays = await Task.WhenAll(tasks);

            // Analyze distribution
            var buckets = new int[10]; // Divide time into 10 buckets
            var minDelay = delays.Min();
            var maxDelay = delays.Max();
            var bucketSize = (maxDelay - minDelay).TotalMilliseconds / 10;

            foreach (var delay in delays)
            {
                var bucketIndex = (int)((delay - minDelay).TotalMilliseconds / bucketSize);

                if (bucketIndex >= 0 && bucketIndex < 10)
                    buckets[bucketIndex]++;
            }

            Console.WriteLine($"  Delay range: {minDelay.TotalMilliseconds:F1}ms - {maxDelay.TotalMilliseconds:F1}ms");
            Console.WriteLine("  Distribution (clients per time bucket):");
            Console.WriteLine($"    {string.Join(" | ", buckets.Select(b => b.ToString().PadLeft(3)))}");

            // Calculate clustering metric (lower is better)
            var expectedPerBucket = concurrentClients / 10.0;
            var clusteringScore = buckets.Sum(b => Math.Pow(b - expectedPerBucket, 2)) / (10 * expectedPerBucket);
            Console.WriteLine($"  Clustering score (lower is better): {clusteringScore:F2}");
            Console.WriteLine();
        }
    }

    /// <summary>
    ///     Example 6: Recovery time analysis.
    ///     Shows how different strategies affect recovery time after service restoration.
    /// </summary>
    public static async Task RecoveryTimeAnalysis()
    {
        Console.WriteLine("=== Recovery Time Analysis ===");
        Console.WriteLine();

        var strategies = new[]
        {
            ("Exponential + Full Jitter", CreateExponentialFullJitterStrategy()),
            ("Linear + Equal Jitter", CreateLinearEqualJitterStrategy()),
            ("Fixed + No Jitter", CreateFixedNoJitterStrategy()),
        };

        var failureDuration = TimeSpan.FromSeconds(30); // Service is down for 30 seconds
        var maxRetries = 10;

        foreach (var (strategyName, strategy) in strategies)
        {
            Console.WriteLine($"Strategy: {strategyName}");

            var totalDelay = TimeSpan.Zero;
            var retryCount = 0;

            // Simulate retries during failure
            for (var attempt = 0; attempt < maxRetries; attempt++)
            {
                var delay = await strategy.GetDelayAsync(attempt);
                totalDelay = totalDelay.Add(delay);
                retryCount++;

                // Check if service would be restored by now
                if (totalDelay >= failureDuration)
                {
                    Console.WriteLine($"  Service restored after {retryCount} retries");
                    Console.WriteLine($"  Total delay before recovery: {failureDuration.TotalSeconds:F1}s");
                    Console.WriteLine($"  Next retry would be in: {delay.TotalSeconds:F1}s");
                    break;
                }
            }

            if (totalDelay < failureDuration)
            {
                Console.WriteLine($"  Would exhaust retries ({maxRetries}) before service recovery");
                Console.WriteLine($"  Total delay after all retries: {totalDelay.TotalSeconds:F1}s");
            }

            Console.WriteLine();
        }
    }

    /// <summary>
    ///     Example 7: Load pattern recommendations.
    ///     Provides recommendations for different load patterns.
    /// </summary>
    public static void LoadPatternRecommendations()
    {
        Console.WriteLine("=== Load Pattern Recommendations ===");
        Console.WriteLine();

        var scenarios = new[]
        {
            ("Burst Traffic", "High volume, short duration", "Exponential + Full Jitter", "Prevents overload during spikes"),
            ("Steady Traffic", "Consistent, moderate volume", "Linear + Equal Jitter", "Predictable recovery with some randomness"),
            ("Background Processing", "Low priority, tolerant of delays", "Exponential + Decorrelated Jitter", "Adaptive to varying conditions"),
            ("Real-time Processing", "Low latency required", "Linear + Minimal Jitter", "Fast recovery with controlled randomness"),
            ("Batch Processing", "High throughput, tolerant of delays", "Fixed + No Jitter", "Predictable timing for batch coordination"),
            ("API Gateway", "Mixed traffic patterns", "Exponential + Full Jitter", "Handles diverse client patterns"),
            ("Database Operations", "Resource constrained", "Linear + Equal Jitter", "Conservative resource usage"),
            ("File Processing", "I/O bound operations", "Fixed + No Jitter", "Simple and predictable"),
        };

        Console.WriteLine("Recommended strategies for different load patterns:");
        Console.WriteLine();

        foreach (var (scenario, description, strategy, reason) in scenarios)
        {
            Console.WriteLine($"Scenario: {scenario}");
            Console.WriteLine($"  Description: {description}");
            Console.WriteLine($"  Recommended: {strategy}");
            Console.WriteLine($"  Reason: {reason}");
            Console.WriteLine();
        }
    }

    /// <summary>
    ///     Example 8: Performance optimization tips.
    ///     Provides tips for optimizing retry strategy performance.
    /// </summary>
    public static void PerformanceOptimizationTips()
    {
        Console.WriteLine("=== Performance Optimization Tips ===");
        Console.WriteLine();

        Console.WriteLine("1. Strategy Selection:");
        Console.WriteLine("   - Use Fixed + No Jitter for highest performance");
        Console.WriteLine("   - Use Linear + Equal Jitter for balanced performance");
        Console.WriteLine("   - Use Exponential + Full Jitter for best resilience");
        Console.WriteLine();

        Console.WriteLine("2. Configuration Tuning:");
        Console.WriteLine("   - Lower base delays for faster recovery");
        Console.WriteLine("   - Lower multipliers for gentler backoff");
        Console.WriteLine("   - Reasonable max delays prevent excessive waits");
        Console.WriteLine();

        Console.WriteLine("3. Resource Management:");
        Console.WriteLine("   - Reuse strategy instances when possible");
        Console.WriteLine("   - Avoid creating new Random instances per call");
        Console.WriteLine("   - Consider async patterns for high concurrency");
        Console.WriteLine();

        Console.WriteLine("4. Monitoring and Adjustment:");
        Console.WriteLine("   - Monitor retry patterns and success rates");
        Console.WriteLine("   - Adjust parameters based on observed behavior");
        Console.WriteLine("   - Use adaptive strategies for varying conditions");
        Console.WriteLine();

        Console.WriteLine("5. Circuit Breaker Integration:");
        Console.WriteLine("   - Combine retry strategies with circuit breakers");
        Console.WriteLine("   - Use faster retries in healthy states");
        Console.WriteLine("   - Use conservative retries during degradation");
        Console.WriteLine();
    }

    /// <summary>
    ///     Run all performance comparison examples.
    /// </summary>
    public static async Task RunAllExamples()
    {
        StrategyPerformanceBenchmarks();
        await ThroughputAnalysisExample();
        await LatencyDistributionAnalysis();
        await ResourceUsageComparison();
        await ThunderingHerdSimulation();
        await RecoveryTimeAnalysis();
        LoadPatternRecommendations();
        PerformanceOptimizationTips();
    }

    // Helper methods for creating strategies
    private static IRetryDelayStrategy CreateExponentialFullJitterStrategy()
    {
        return new CompositeRetryDelayStrategy(
            new ExponentialBackoffStrategy(new ExponentialBackoffConfiguration
            {
                BaseDelay = TimeSpan.FromMilliseconds(100),
                Multiplier = 2.0,
                MaxDelay = TimeSpan.FromSeconds(30),
            }),
            JitterStrategies.FullJitter(),
            Random.Shared);
    }

    private static IRetryDelayStrategy CreateLinearEqualJitterStrategy()
    {
        return new CompositeRetryDelayStrategy(
            new LinearBackoffStrategy(new LinearBackoffConfiguration
            {
                BaseDelay = TimeSpan.FromMilliseconds(100),
                Increment = TimeSpan.FromMilliseconds(100),
                MaxDelay = TimeSpan.FromSeconds(10),
            }),
            JitterStrategies.EqualJitter(),
            Random.Shared);
    }

    private static IRetryDelayStrategy CreateFixedNoJitterStrategy()
    {
        return new CompositeRetryDelayStrategy(
            new FixedDelayStrategy(new FixedDelayConfiguration
            {
                Delay = TimeSpan.FromMilliseconds(500),
            }),
            JitterStrategies.NoJitter(),
            Random.Shared);
    }

    private static IRetryDelayStrategy CreateExponentialNoJitterStrategy()
    {
        return new CompositeRetryDelayStrategy(
            new ExponentialBackoffStrategy(new ExponentialBackoffConfiguration
            {
                BaseDelay = TimeSpan.FromMilliseconds(100),
                Multiplier = 2.0,
                MaxDelay = TimeSpan.FromSeconds(30),
            }),
            JitterStrategies.NoJitter(),
            Random.Shared);
    }

    private static IRetryDelayStrategy CreateExponentialEqualJitterStrategy()
    {
        return new CompositeRetryDelayStrategy(
            new ExponentialBackoffStrategy(new ExponentialBackoffConfiguration
            {
                BaseDelay = TimeSpan.FromMilliseconds(100),
                Multiplier = 2.0,
                MaxDelay = TimeSpan.FromSeconds(30),
            }),
            JitterStrategies.EqualJitter(),
            Random.Shared);
    }

    private static IRetryDelayStrategy CreateExponentialDecorrelatedJitterStrategy()
    {
        return new CompositeRetryDelayStrategy(
            new ExponentialBackoffStrategy(new ExponentialBackoffConfiguration
            {
                BaseDelay = TimeSpan.FromMilliseconds(100),
                Multiplier = 2.0,
                MaxDelay = TimeSpan.FromSeconds(30),
            }),
            JitterStrategies.DecorrelatedJitter(TimeSpan.FromSeconds(30), 3.0),
            Random.Shared);
    }

    private static async Task<double> MeasureThroughput(IRetryDelayStrategy strategy, int operationCount, int intervalMs)
    {
        var stopwatch = Stopwatch.StartNew();
        var tasks = new List<Task>();

        for (var i = 0; i < operationCount; i++)
        {
            tasks.Add(Task.Run(async () =>
            {
                await Task.Delay(intervalMs);
                await strategy.GetDelayAsync(0);
            }));
        }

        await Task.WhenAll(tasks);
        stopwatch.Stop();

        return operationCount / stopwatch.Elapsed.TotalSeconds;
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
            new ExponentialBackoffStrategy(new ExponentialBackoffConfiguration
            {
                BaseDelay = TimeSpan.FromMilliseconds(100),
                Multiplier = 2.0,
                MaxDelay = TimeSpan.FromSeconds(30),
            }),
            JitterStrategies.FullJitter(),
            Random.Shared);

        _linearEqualJitter = new CompositeRetryDelayStrategy(
            new LinearBackoffStrategy(new LinearBackoffConfiguration
            {
                BaseDelay = TimeSpan.FromMilliseconds(100),
                Increment = TimeSpan.FromMilliseconds(100),
                MaxDelay = TimeSpan.FromSeconds(10),
            }),
            JitterStrategies.EqualJitter(),
            Random.Shared);

        _fixedNoJitter = new CompositeRetryDelayStrategy(
            new FixedDelayStrategy(new FixedDelayConfiguration
            {
                Delay = TimeSpan.FromMilliseconds(500),
            }),
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

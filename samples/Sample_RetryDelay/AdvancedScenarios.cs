using Microsoft.Extensions.Logging;
using NPipeline.Execution.RetryDelay;

namespace Sample_RetryDelay;

/// <summary>
///     Demonstrates advanced scenarios for retry delay strategies.
/// </summary>
public static class AdvancedScenarios
{
    private static readonly ILogger logger = LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger(typeof(AdvancedScenarios));

    /// <summary>
    ///     Runs all advanced scenario examples.
    /// </summary>
    public static async Task RunAllExamples()
    {
        await NodeSpecificRetryConfiguration();
        await DynamicStrategySelection();
        await CustomStrategyImplementation();
        await CircuitBreakerIntegration();
        await MonitoringAndObservability();
        await MultiRegionAndCostAwareStrategies();
    }

    /// <summary>
    ///     Demonstrates node-specific retry configurations.
    /// </summary>
    public static async Task NodeSpecificRetryConfiguration()
    {
        Console.WriteLine("Node-Specific Retry Configuration:");
        Console.WriteLine("=================================");

        // Different strategies for different node types
        var sourceStrategy = new CompositeRetryDelayStrategy(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(50),
                2.0,
                TimeSpan.FromSeconds(10)),
            JitterStrategies.FullJitter(),
            new Random(100));

        var transformStrategy = new CompositeRetryDelayStrategy(
            BackoffStrategies.LinearBackoff(
                TimeSpan.FromMilliseconds(100),
                TimeSpan.FromMilliseconds(25),
                TimeSpan.FromSeconds(30)),
            JitterStrategies.EqualJitter(),
            new Random(200));

        var sinkStrategy = new CompositeRetryDelayStrategy(
            BackoffStrategies.FixedDelay(TimeSpan.FromMilliseconds(500)),
            JitterStrategies.NoJitter(),
            new Random(300));

        Console.WriteLine("Source Node Strategy:");

        for (var i = 0; i < 3; i++)
        {
            var delay = await sourceStrategy.GetDelayAsync(i);
            Console.WriteLine($"  Attempt {i + 1}: {delay.TotalMilliseconds:F2}ms");
        }

        Console.WriteLine("\nTransform Node Strategy:");

        for (var i = 0; i < 3; i++)
        {
            var delay = await transformStrategy.GetDelayAsync(i);
            Console.WriteLine($"  Attempt {i + 1}: {delay.TotalMilliseconds:F2}ms");
        }

        Console.WriteLine("\nSink Node Strategy:");

        for (var i = 0; i < 3; i++)
        {
            var delay = await sinkStrategy.GetDelayAsync(i);
            Console.WriteLine($"  Attempt {i + 1}: {delay.TotalMilliseconds:F2}ms");
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Demonstrates dynamic strategy selection.
    /// </summary>
    public static async Task DynamicStrategySelection()
    {
        Console.WriteLine("Dynamic Strategy Selection:");
        Console.WriteLine("=========================");

        // Select strategy based on attempt count
        IRetryDelayStrategy strategy = new CompositeRetryDelayStrategy(
            attempt => attempt switch
            {
                < 2 => BackoffStrategies.ExponentialBackoff(
                    TimeSpan.FromMilliseconds(100),
                    2.0,
                    TimeSpan.FromSeconds(5))(attempt),
                < 5 => BackoffStrategies.LinearBackoff(
                    TimeSpan.FromMilliseconds(200),
                    TimeSpan.FromMilliseconds(100),
                    TimeSpan.FromSeconds(10))(attempt),
                _ => BackoffStrategies.FixedDelay(TimeSpan.FromMilliseconds(1000))(attempt),
            },
            JitterStrategies.NoJitter(),
            new Random(400));

        // Add jitter dynamically
        var jitteredStrategy = new CompositeRetryDelayStrategy(
            attempt => strategy.GetDelayAsync(attempt).AsTask().GetAwaiter().GetResult(),
            JitterStrategies.FullJitter(),
            new Random(400));

        for (var i = 0; i < 8; i++)
        {
            var delay = await jitteredStrategy.GetDelayAsync(i);
            Console.WriteLine($"  Attempt {i + 1}: {delay.TotalMilliseconds:F2}ms");
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Demonstrates custom strategy implementation.
    /// </summary>
    public static async Task CustomStrategyImplementation()
    {
        Console.WriteLine("Custom Strategy Implementation:");
        Console.WriteLine("=============================");

        // Custom backoff: Fibonacci sequence
        BackoffStrategy fibonacciBackoff = attempt =>
        {
            if (attempt < 0)
                return TimeSpan.Zero;

            var a = 1;
            var b = 1;

            for (var i = 0; i < attempt; i++)
            {
                var temp = a + b;
                a = b;
                b = temp;
            }

            return TimeSpan.FromMilliseconds(a * 100);
        };

        var strategy = new CompositeRetryDelayStrategy(
            fibonacciBackoff,
            JitterStrategies.EqualJitter(),
            new Random(500));

        for (var i = 0; i < 6; i++)
        {
            var delay = await strategy.GetDelayAsync(i);
            Console.WriteLine($"  Attempt {i + 1}: {delay.TotalMilliseconds:F2}ms (Fibonacci: {GetFibonacci(i)})");
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Demonstrates circuit breaker integration.
    /// </summary>
    public static async Task CircuitBreakerIntegration()
    {
        Console.WriteLine("Circuit Breaker Integration:");
        Console.WriteLine("===========================");

        // This would integrate with a circuit breaker
        // For demonstration, we'll simulate it
        var circuitOpen = false;
        var failureCount = 0;
        const int failureThreshold = 5;

        var strategy = new CompositeRetryDelayStrategy(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(100),
                2.0,
                TimeSpan.FromSeconds(30)),
            JitterStrategies.DecorrelatedJitter(TimeSpan.FromMinutes(1)),
            new Random(600));

        for (var i = 0; i < 10; i++)
        {
            if (circuitOpen)
            {
                Console.WriteLine($"  Attempt {i + 1}: Circuit OPEN - skipping retry");
                continue;
            }

            var delay = await strategy.GetDelayAsync(i);
            Console.WriteLine($"  Attempt {i + 1}: {delay.TotalMilliseconds:F2}ms");

            // Simulate failure
            if (i % 3 == 0)
            {
                failureCount++;
                Console.WriteLine($"    Failure {failureCount}/{failureThreshold}");

                if (failureCount >= failureThreshold)
                {
                    circuitOpen = true;
                    Console.WriteLine("    Circuit OPENED due to failure threshold");
                }
            }
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Demonstrates monitoring and observability.
    /// </summary>
    public static async Task MonitoringAndObservability()
    {
        Console.WriteLine("Monitoring and Observability:");
        Console.WriteLine("=============================");

        var strategy = new CompositeRetryDelayStrategy(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(100),
                2.0,
                TimeSpan.FromSeconds(30)),
            JitterStrategies.FullJitter(),
            new Random(700));

        var delays = new List<TimeSpan>();
        var timestamps = new List<DateTimeOffset>();

        for (var i = 0; i < 5; i++)
        {
            var start = DateTimeOffset.UtcNow;
            var delay = await strategy.GetDelayAsync(i);
            var end = DateTimeOffset.UtcNow;

            delays.Add(delay);
            timestamps.Add(end);

            Console.WriteLine($"  Attempt {i + 1}: {delay.TotalMilliseconds:F2}ms");
            Console.WriteLine($"    Start: {start:HH:mm:ss.fff}");
            Console.WriteLine($"    End: {end:HH:mm:ss.fff}");
            Console.WriteLine($"    Duration: {(end - start).TotalMilliseconds:F2}ms");
        }

        // Calculate metrics
        var avgDelay = TimeSpan.FromMilliseconds(delays.Average(d => d.TotalMilliseconds));
        var maxDelay = delays.Max();
        var totalDuration = timestamps.Last() - timestamps.First();

        Console.WriteLine("\nMetrics:");
        Console.WriteLine($"  Average Delay: {avgDelay.TotalMilliseconds:F2}ms");
        Console.WriteLine($"  Max Delay: {maxDelay.TotalMilliseconds:F2}ms");
        Console.WriteLine($"  Total Duration: {totalDuration.TotalMilliseconds:F2}ms");
        Console.WriteLine($"  Throughput: {delays.Count / totalDuration.TotalSeconds:F2} attempts/sec");

        Console.WriteLine();
    }

    /// <summary>
    ///     Demonstrates multi-region and cost-aware strategies.
    /// </summary>
    public static async Task MultiRegionAndCostAwareStrategies()
    {
        Console.WriteLine("Multi-Region and Cost-Aware Strategies:");
        Console.WriteLine("=====================================");

        // Simulate different regions with different costs
        var regions = new[]
        {
            ("US-East", TimeSpan.FromMilliseconds(50)),
            ("US-West", TimeSpan.FromMilliseconds(75)),
            ("EU-West", TimeSpan.FromMilliseconds(100)),
            ("AP-Southeast", TimeSpan.FromMilliseconds(125)),
        };

        foreach (var (region, baseDelay) in regions)
        {
            Console.WriteLine($"\n{region} Region:");

            var strategy = new CompositeRetryDelayStrategy(
                BackoffStrategies.ExponentialBackoff(
                    baseDelay,
                    2.0,
                    TimeSpan.FromSeconds(30)),
                JitterStrategies.FullJitter(),
                new Random(region.GetHashCode()));

            for (var i = 0; i < 3; i++)
            {
                var delay = await strategy.GetDelayAsync(i);
                Console.WriteLine($"  Attempt {i + 1}: {delay.TotalMilliseconds:F2}ms");
            }
        }

        Console.WriteLine();
    }

    private static int GetFibonacci(int n)
    {
        if (n <= 0)
            return 0;

        if (n == 1)
            return 1;

        var a = 1;
        var b = 1;

        for (var i = 2; i <= n; i++)
        {
            var temp = a + b;
            a = b;
            b = temp;
        }

        return a;
    }
}

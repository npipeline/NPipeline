using Microsoft.Extensions.Logging;
using NPipeline.Execution.RetryDelay;
using NPipeline.Execution.RetryDelay.Backoff;

namespace Sample_RetryDelay;

/// <summary>
///     Advanced scenarios for retry delay strategies.
///     Demonstrates complex configurations and integration patterns.
/// </summary>
public static class AdvancedScenarios
{
    private static readonly ILogger logger = LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger(typeof(AdvancedScenarios));

    /// <summary>
    ///     Example 1: Node-specific retry configurations.
    ///     Shows how different nodes can have different retry strategies.
    /// </summary>
    public static void NodeSpecificRetryConfigurationExample()
    {
        Console.WriteLine("=== Node-Specific Retry Configuration Example ===");

        Console.WriteLine("Different nodes can have different retry strategies based on their characteristics:");
        Console.WriteLine();
        Console.WriteLine("1. External API Node - Aggressive retry with exponential backoff:");
        Console.WriteLine("   - Strategy: Exponential backoff with full jitter");
        Console.WriteLine("   - Base delay: 500ms");
        Console.WriteLine("   - Multiplier: 2.0");
        Console.WriteLine("   - Max delay: 30s");
        Console.WriteLine("   - Max attempts: 5");
        Console.WriteLine("   - Reason: External services may have transient issues");
        Console.WriteLine();
        Console.WriteLine("2. Database Node - Conservative retry with linear backoff:");
        Console.WriteLine("   - Strategy: Linear backoff with equal jitter");
        Console.WriteLine("   - Base delay: 100ms");
        Console.WriteLine("   - Increment: 100ms");
        Console.WriteLine("   - Max delay: 5s");
        Console.WriteLine("   - Max attempts: 3");
        Console.WriteLine("   - Reason: Database overload requires careful handling");
        Console.WriteLine();
        Console.WriteLine("3. File Processing Node - Simple fixed delay:");
        Console.WriteLine("   - Strategy: Fixed delay with no jitter");
        Console.WriteLine("   - Delay: 200ms");
        Console.WriteLine("   - Max attempts: 3");
        Console.WriteLine("   - Reason: File system issues are usually quick to resolve");
        Console.WriteLine();
    }

    /// <summary>
    ///     Example 2: Dynamic strategy selection based on error types.
    ///     Shows how to choose different retry strategies based on the error.
    /// </summary>
    public static async Task DynamicStrategySelectionExample()
    {
        Console.WriteLine("=== Dynamic Strategy Selection Example ===");

        // Simulate different error types
        var networkError = new HttpRequestException("Network timeout");
        var databaseError = new InvalidOperationException("Database connection failed");
        var fileError = new IOException("File is locked");
        var validationError = new ArgumentException("Invalid data format");

        Console.WriteLine("Different retry strategies for different error types:");
        Console.WriteLine();

        // Create strategies for different error types
        var networkStrategy = CreateNetworkRetryStrategy();
        var databaseStrategy = CreateDatabaseRetryStrategy();
        var fileStrategy = CreateFileRetryStrategy();
        var validationStrategy = NoOpRetryDelayStrategy.Instance; // No retry for validation errors

        Console.WriteLine($"Network error ({networkError.GetType().Name}):");

        for (var i = 0; i < 3; i++)
        {
            var delay = await networkStrategy.GetDelayAsync(i);
            Console.WriteLine($"  Attempt {i}: {delay.TotalMilliseconds:F0}ms");
        }

        Console.WriteLine($"\nDatabase error ({databaseError.GetType().Name}):");

        for (var i = 0; i < 3; i++)
        {
            var delay = await databaseStrategy.GetDelayAsync(i);
            Console.WriteLine($"  Attempt {i}: {delay.TotalMilliseconds:F0}ms");
        }

        Console.WriteLine($"\nFile error ({fileError.GetType().Name}):");

        for (var i = 0; i < 3; i++)
        {
            var delay = await fileStrategy.GetDelayAsync(i);
            Console.WriteLine($"  Attempt {i}: {delay.TotalMilliseconds:F0}ms");
        }

        Console.WriteLine($"\nValidation error ({validationError.GetType().Name}):");

        for (var i = 0; i < 3; i++)
        {
            var delay = await validationStrategy.GetDelayAsync(i);
            Console.WriteLine($"  Attempt {i}: {delay.TotalMilliseconds:F0}ms");
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Example 3: Custom backoff implementation.
    ///     Shows how to implement a custom backoff strategy.
    /// </summary>
    public static async Task CustomBackoffImplementationExample()
    {
        Console.WriteLine("=== Custom Backoff Implementation Example ===");

        // Custom strategy: Fibonacci backoff
        var fibonacciStrategy = new FibonacciBackoffStrategy(
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromSeconds(10));

        Console.WriteLine("Fibonacci backoff strategy:");
        Console.WriteLine("Sequence: 100ms, 100ms, 200ms, 300ms, 500ms, 800ms, 1300ms, 2100ms, 3400ms, 5500ms, 8900ms");
        Console.WriteLine();

        for (var attempt = 0; attempt < 8; attempt++)
        {
            var delay = await fibonacciStrategy.GetDelayAsync(attempt);
            Console.WriteLine($"Attempt {attempt}: {delay.TotalMilliseconds:F0}ms");
        }

        Console.WriteLine();

        // Custom strategy: Step backoff (increases in steps)
        var stepStrategy = new StepBackoffStrategy([
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromMilliseconds(500),
            TimeSpan.FromMilliseconds(500),
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(2),
            TimeSpan.FromSeconds(5),
        ]);

        Console.WriteLine("Step backoff strategy:");
        Console.WriteLine("Pattern: 100ms, 100ms, 500ms, 500ms, 1s, 1s, 2s, 5s");
        Console.WriteLine();

        for (var attempt = 0; attempt < 8; attempt++)
        {
            var delay = await stepStrategy.GetDelayAsync(attempt);
            Console.WriteLine($"Attempt {attempt}: {delay.TotalMilliseconds:F0}ms");
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Example 4: Custom jitter implementation.
    ///     Shows how to implement a custom jitter strategy.
    /// </summary>
    public static async Task CustomJitterImplementationExample()
    {
        Console.WriteLine("=== Custom Jitter Implementation Example ===");

        // Custom jitter: Bounded jitter (keeps delay within a range)
        var boundedJitterStrategy = new CompositeRetryDelayStrategy(
            new ExponentialBackoffStrategy(new ExponentialBackoffConfiguration
            {
                BaseDelay = TimeSpan.FromSeconds(1),
                Multiplier = 2.0,
                MaxDelay = TimeSpan.FromMinutes(1),
            }),
            JitterStrategies.FullJitter(),
            Random.Shared);

        Console.WriteLine("Bounded jitter strategy (50% to 150% of base delay):");

        for (var attempt = 0; attempt < 5; attempt++)
        {
            var delay = await boundedJitterStrategy.GetDelayAsync(attempt);
            Console.WriteLine($"Attempt {attempt}: {delay.TotalSeconds:F1}s");
        }

        Console.WriteLine();

        // Custom jitter: Time-based jitter (different jitter at different times of day)
        var timeBasedJitterStrategy = new CompositeRetryDelayStrategy(
            new FixedDelayStrategy(new FixedDelayConfiguration { Delay = TimeSpan.FromSeconds(5) }),
            JitterStrategies.FullJitter(),
            Random.Shared);

        Console.WriteLine("Time-based jitter (more jitter during business hours):");
        Console.WriteLine($"Current time: {DateTime.Now:HH:mm}");

        for (var attempt = 0; attempt < 3; attempt++)
        {
            var delay = await timeBasedJitterStrategy.GetDelayAsync(attempt);
            Console.WriteLine($"Attempt {attempt}: {delay.TotalSeconds:F1}s");
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Example 5: Integration with circuit breakers.
    ///     Shows how retry delays work with circuit breaker patterns.
    /// </summary>
    public static void CircuitBreakerIntegrationExample()
    {
        Console.WriteLine("=== Circuit Breaker Integration Example ===");

        Console.WriteLine("Combining retry delays with circuit breakers:");
        Console.WriteLine();
        Console.WriteLine("1. Fast retry strategy for healthy state:");
        Console.WriteLine("   - Strategy: Linear backoff with minimal jitter");
        Console.WriteLine("   - Base delay: 50ms");
        Console.WriteLine("   - Increment: 50ms");
        Console.WriteLine("   - Max delay: 500ms");
        Console.WriteLine("   - Circuit breaker: Open after 10 failures in 1 minute");
        Console.WriteLine();
        Console.WriteLine("2. Conservative retry strategy for degraded state:");
        Console.WriteLine("   - Strategy: Exponential backoff with full jitter");
        Console.WriteLine("   - Base delay: 1s");
        Console.WriteLine("   - Multiplier: 2.0");
        Console.WriteLine("   - Max delay: 30s");
        Console.WriteLine("   - Circuit breaker: Open after 5 failures in 30 seconds");
        Console.WriteLine();
        Console.WriteLine("3. Minimal retry strategy for unhealthy state:");
        Console.WriteLine("   - Strategy: Fixed delay with no jitter");
        Console.WriteLine("   - Delay: 5s");
        Console.WriteLine("   - Max attempts: 2");
        Console.WriteLine("   - Circuit breaker: Open after 3 failures in 10 seconds");
        Console.WriteLine();
    }

    /// <summary>
    ///     Example 6: Monitoring and observability.
    ///     Shows how to monitor retry behavior and collect metrics.
    /// </summary>
    public static async Task MonitoringAndObservabilityExample()
    {
        Console.WriteLine("=== Monitoring and Observability Example ===");

        var strategy = new CompositeRetryDelayStrategy(
            new ExponentialBackoffStrategy(new ExponentialBackoffConfiguration
            {
                BaseDelay = TimeSpan.FromSeconds(1),
                Multiplier = 2.0,
                MaxDelay = TimeSpan.FromMinutes(1),
            }),
            JitterStrategies.FullJitter(),
            Random.Shared);

        Console.WriteLine("Simulating retry attempts with metrics collection:");
        Console.WriteLine();

        // Simulate multiple retry scenarios
        for (var scenario = 1; scenario <= 3; scenario++)
        {
            Console.WriteLine($"Scenario {scenario}:");
            var scenarioMetrics = new RetryMetrics();

            for (var attempt = 0; attempt < 5; attempt++)
            {
                var delay = await strategy.GetDelayAsync(attempt);
                scenarioMetrics.RecordAttempt(attempt, delay);

                Console.WriteLine($"  Attempt {attempt}: {delay.TotalSeconds:F1}s");
            }

            Console.WriteLine($"  Total attempts: {scenarioMetrics.TotalAttempts}");
            Console.WriteLine($"  Average delay: {scenarioMetrics.AverageDelay.TotalSeconds:F1}s");
            Console.WriteLine($"  Max delay: {scenarioMetrics.MaxDelay.TotalSeconds:F1}s");
            Console.WriteLine();
        }
    }

    /// <summary>
    ///     Example 7: Adaptive retry strategies.
    ///     Shows how to adapt retry behavior based on success/failure patterns.
    /// </summary>
    public static async Task AdaptiveRetryStrategiesExample()
    {
        Console.WriteLine("=== Adaptive Retry Strategies Example ===");

        var adaptiveStrategy = new AdaptiveRetryStrategy();

        Console.WriteLine("Adaptive strategy that adjusts based on success rate:");
        Console.WriteLine();

        // Simulate different success rates
        var successRates = new[] { 0.9, 0.5, 0.1 }; // High, medium, low success rates

        foreach (var successRate in successRates)
        {
            Console.WriteLine($"Success rate: {successRate:P0}");
            adaptiveStrategy.UpdateSuccessRate(successRate);

            for (var attempt = 0; attempt < 4; attempt++)
            {
                var delay = await adaptiveStrategy.GetDelayAsync(attempt);
                Console.WriteLine($"  Attempt {attempt}: {delay.TotalMilliseconds:F0}ms");
            }

            Console.WriteLine();
        }
    }

    /// <summary>
    ///     Example 8: Multi-region retry strategies.
    ///     Shows how to implement retry strategies for distributed systems across regions.
    /// </summary>
    public static async Task MultiRegionRetryStrategiesExample()
    {
        Console.WriteLine("=== Multi-Region Retry Strategies Example ===");

        Console.WriteLine("Different retry strategies for different regions:");
        Console.WriteLine();

        // Primary region (low latency, high reliability)
        var primaryStrategy = new CompositeRetryDelayStrategy(
            new ExponentialBackoffStrategy(new ExponentialBackoffConfiguration
            {
                BaseDelay = TimeSpan.FromMilliseconds(100),
                Multiplier = 2.0,
                MaxDelay = TimeSpan.FromSeconds(30),
            }),
            JitterStrategies.EqualJitter(),
            Random.Shared);

        Console.WriteLine("Primary region (low latency):");

        for (var attempt = 0; attempt < 4; attempt++)
        {
            var delay = await primaryStrategy.GetDelayAsync(attempt);
            Console.WriteLine($"  Attempt {attempt}: {delay.TotalMilliseconds:F0}ms");
        }

        Console.WriteLine();

        // Secondary region (higher latency, lower reliability)
        var secondaryStrategy = new CompositeRetryDelayStrategy(
            new ExponentialBackoffStrategy(new ExponentialBackoffConfiguration
            {
                BaseDelay = TimeSpan.FromMilliseconds(200),
                Multiplier = 1.5,
                MaxDelay = TimeSpan.FromSeconds(45),
            }),
            JitterStrategies.FullJitter(),
            Random.Shared);

        Console.WriteLine("Secondary region (higher latency):");

        for (var attempt = 0; attempt < 4; attempt++)
        {
            var delay = await secondaryStrategy.GetDelayAsync(attempt);
            Console.WriteLine($"  Attempt {attempt}: {delay.TotalMilliseconds:F0}ms");
        }

        Console.WriteLine();

        // Disaster recovery region (high latency, unknown reliability)
        var drStrategy = new CompositeRetryDelayStrategy(
            new LinearBackoffStrategy(new LinearBackoffConfiguration
            {
                BaseDelay = TimeSpan.FromMilliseconds(50),
                Increment = TimeSpan.FromMilliseconds(30),
                MaxDelay = TimeSpan.FromSeconds(8),
            }),
            JitterStrategies.FullJitter(),
            Random.Shared);

        Console.WriteLine("DR region (high latency):");

        for (var attempt = 0; attempt < 4; attempt++)
        {
            var delay = await drStrategy.GetDelayAsync(attempt);
            Console.WriteLine($"  Attempt {attempt}: {delay.TotalSeconds:F1}s");
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Example 9: Cost-aware retry strategies.
    ///     Shows how to consider cost implications in retry decisions.
    /// </summary>
    public static async Task CostAwareRetryStrategiesExample()
    {
        Console.WriteLine("=== Cost-Aware Retry Strategies Example ===");

        Console.WriteLine("Different retry strategies based on cost considerations:");
        Console.WriteLine();

        // Free tier (limited retries)
        var freeTierStrategy = new CompositeRetryDelayStrategy(
            new LinearBackoffStrategy(new LinearBackoffConfiguration
            {
                BaseDelay = TimeSpan.FromMilliseconds(100),
                Increment = TimeSpan.FromMilliseconds(50),
                MaxDelay = TimeSpan.FromSeconds(5),
            }),
            JitterStrategies.EqualJitter(),
            Random.Shared);

        Console.WriteLine("Free tier (cost-conscious):");

        for (var attempt = 0; attempt < 3; attempt++)
        {
            var delay = await freeTierStrategy.GetDelayAsync(attempt);
            Console.WriteLine($"  Attempt {attempt}: {delay.TotalSeconds:F1}s");
        }

        Console.WriteLine();

        // Standard tier (balanced approach)
        var standardTierStrategy = new CompositeRetryDelayStrategy(
            new ExponentialBackoffStrategy(new ExponentialBackoffConfiguration
            {
                BaseDelay = TimeSpan.FromMilliseconds(200),
                Multiplier = 2.0,
                MaxDelay = TimeSpan.FromSeconds(30),
            }),
            JitterStrategies.FullJitter(),
            Random.Shared);

        Console.WriteLine("Standard tier (balanced):");

        for (var attempt = 0; attempt < 5; attempt++)
        {
            var delay = await standardTierStrategy.GetDelayAsync(attempt);
            Console.WriteLine($"  Attempt {attempt}: {delay.TotalSeconds:F1}s");
        }

        Console.WriteLine();

        // Premium tier (aggressive retry)
        var premiumTierStrategy = new CompositeRetryDelayStrategy(
            new ExponentialBackoffStrategy(new ExponentialBackoffConfiguration
            {
                BaseDelay = TimeSpan.FromMilliseconds(150),
                Multiplier = 1.8,
                MaxDelay = TimeSpan.FromSeconds(25),
            }),
            JitterStrategies.FullJitter(),
            Random.Shared);

        Console.WriteLine("Premium tier (aggressive):");

        for (var attempt = 0; attempt < 7; attempt++)
        {
            var delay = await premiumTierStrategy.GetDelayAsync(attempt);
            Console.WriteLine($"  Attempt {attempt}: {delay.TotalMilliseconds:F0}ms");
        }

        Console.WriteLine();
    }

    // Helper methods for creating strategies
    private static IRetryDelayStrategy CreateNetworkRetryStrategy()
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

    private static IRetryDelayStrategy CreateDatabaseRetryStrategy()
    {
        return new CompositeRetryDelayStrategy(
            new LinearBackoffStrategy(new LinearBackoffConfiguration
            {
                BaseDelay = TimeSpan.FromMilliseconds(200),
                Increment = TimeSpan.FromMilliseconds(25),
                MaxDelay = TimeSpan.FromSeconds(10),
            }),
            JitterStrategies.EqualJitter(),
            Random.Shared);
    }

    private static IRetryDelayStrategy CreateFileRetryStrategy()
    {
        return new CompositeRetryDelayStrategy(
            new FixedDelayStrategy(new FixedDelayConfiguration
            {
                Delay = TimeSpan.FromMilliseconds(300),
            }),
            JitterStrategies.NoJitter(),
            Random.Shared);
    }

    /// <summary>
    ///     Run all advanced scenario examples.
    /// </summary>
    public static async Task RunAllExamples()
    {
        NodeSpecificRetryConfigurationExample();
        await DynamicStrategySelectionExample();
        await CustomBackoffImplementationExample();
        await CustomJitterImplementationExample();
        CircuitBreakerIntegrationExample();
        await MonitoringAndObservabilityExample();
        await AdaptiveRetryStrategiesExample();
        await MultiRegionRetryStrategiesExample();
        await CostAwareRetryStrategiesExample();
    }
}

// Custom implementations for advanced examples

/// <summary>
///     Custom Fibonacci backoff strategy.
/// </summary>
public class FibonacciBackoffStrategy : IRetryDelayStrategy
{
    private readonly TimeSpan _baseDelay;
    private readonly List<long> _fibonacciSequence;
    private readonly TimeSpan _maxDelay;

    public FibonacciBackoffStrategy(TimeSpan baseDelay, TimeSpan maxDelay)
    {
        _baseDelay = baseDelay;
        _maxDelay = maxDelay;
        _fibonacciSequence = GenerateFibonacciSequence(20);
    }

    public ValueTask<TimeSpan> GetDelayAsync(int attemptNumber, CancellationToken cancellationToken = default)
    {
        if (attemptNumber < 0 || attemptNumber >= _fibonacciSequence.Count)
            return ValueTask.FromResult(TimeSpan.Zero);

        var multiplier = _fibonacciSequence[attemptNumber];
        var delay = TimeSpan.FromTicks(_baseDelay.Ticks * multiplier);

        if (delay > _maxDelay)
            delay = _maxDelay;

        return ValueTask.FromResult(delay);
    }

    private static List<long> GenerateFibonacciSequence(int count)
    {
        var sequence = new List<long> { 0, 1 };

        for (var i = 2; i < count; i++)
        {
            sequence.Add(sequence[i - 1] + sequence[i - 2]);
        }

        return sequence;
    }
}

/// <summary>
///     Custom step backoff strategy.
/// </summary>
public class StepBackoffStrategy : IRetryDelayStrategy
{
    private readonly TimeSpan[] _steps;

    public StepBackoffStrategy(TimeSpan[] steps)
    {
        _steps = steps ?? throw new ArgumentNullException(nameof(steps));
    }

    public ValueTask<TimeSpan> GetDelayAsync(int attemptNumber, CancellationToken cancellationToken = default)
    {
        if (attemptNumber < 0)
            return ValueTask.FromResult(TimeSpan.Zero);

        if (attemptNumber >= _steps.Length)
            return ValueTask.FromResult(_steps[_steps.Length - 1]);

        return ValueTask.FromResult(_steps[attemptNumber]);
    }
}

/// <summary>
///     Custom bounded jitter strategy.
/// </summary>
public class BoundedJitterStrategy
{
    private readonly double _maxRatio;
    private readonly double _minRatio;

    public BoundedJitterStrategy(double minRatio, double maxRatio)
    {
        _minRatio = minRatio;
        _maxRatio = maxRatio;
    }

    public JitterStrategy ToJitterStrategy()
    {
        return (baseDelay, random) =>
        {
            var ratio = _minRatio + random.NextDouble() * (_maxRatio - _minRatio);
            return TimeSpan.FromTicks((long)(baseDelay.Ticks * ratio));
        };
    }
}

/// <summary>
///     Custom time-based jitter strategy.
/// </summary>
public class TimeBasedJitterStrategy
{
    public JitterStrategy ToJitterStrategy()
    {
        return (baseDelay, random) =>
        {
            var hour = DateTime.Now.Hour;

            // More jitter during business hours (9 AM - 5 PM)
            var jitterRatio = hour >= 9 && hour <= 17
                ? 0.5
                : 0.2;

            var jitterAmount = baseDelay.TotalMilliseconds * jitterRatio;
            var jitter = random.NextDouble() * jitterAmount;

            return TimeSpan.FromMilliseconds(baseDelay.TotalMilliseconds + jitter);
        };
    }
}

/// <summary>
///     Adaptive retry strategy that adjusts based on success rate.
/// </summary>
public class AdaptiveRetryStrategy : IRetryDelayStrategy
{
    private double _successRate = 0.8; // Default success rate

    public ValueTask<TimeSpan> GetDelayAsync(int attemptNumber, CancellationToken cancellationToken = default)
    {
        if (attemptNumber < 0)
            return ValueTask.FromResult(TimeSpan.Zero);

        // Adjust base delay based on success rate
        var baseDelay = _successRate switch
        {
            >= 0.8 => TimeSpan.FromMilliseconds(100), // High success rate - fast retry
            >= 0.5 => TimeSpan.FromMilliseconds(500), // Medium success rate - moderate retry
            _ => TimeSpan.FromMilliseconds(1000), // Low success rate - slow retry
        };

        // Use exponential backoff
        var delay = TimeSpan.FromTicks((long)(baseDelay.Ticks * Math.Pow(1.5, attemptNumber)));

        // Cap at reasonable maximum
        if (delay > TimeSpan.FromSeconds(30))
            delay = TimeSpan.FromSeconds(30);

        return ValueTask.FromResult(delay);
    }

    public void UpdateSuccessRate(double successRate)
    {
        _successRate = Math.Clamp(successRate, 0.0, 1.0);
    }
}

/// <summary>
///     Simple metrics collector for retry operations.
/// </summary>
public class RetryMetrics
{
    private long _totalDelayTicks;

    public int TotalAttempts { get; private set; }

    public TimeSpan AverageDelay => TotalAttempts > 0
        ? TimeSpan.FromTicks(_totalDelayTicks / TotalAttempts)
        : TimeSpan.Zero;

    public TimeSpan MaxDelay { get; private set; }

    public void RecordAttempt(int attemptNumber, TimeSpan delay)
    {
        TotalAttempts++;
        _totalDelayTicks += delay.Ticks;

        if (delay > MaxDelay)
            MaxDelay = delay;
    }
}

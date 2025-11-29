using Microsoft.Extensions.Logging;
using NPipeline.Execution.RetryDelay;

namespace Sample_RetryDelay;

/// <summary>
///     Demonstrates basic usage patterns for retry delay strategies.
/// </summary>
public static class BasicUsageExamples
{
    private static readonly ILogger logger = LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger(typeof(BasicUsageExamples));

    /// <summary>
    ///     Runs all basic usage examples.
    /// </summary>
    public static async Task RunAllExamples()
    {
        await ExponentialBackoffWithFullJitter();
        await LinearBackoffWithEqualJitter();
        await FixedDelayWithNoJitter();
        await CustomConfigurationExample();
        await ErrorHandlingExample();
        await BestPracticesExample();
    }

    /// <summary>
    ///     Demonstrates exponential backoff with full jitter.
    /// </summary>
    public static async Task ExponentialBackoffWithFullJitter()
    {
        Console.WriteLine("Exponential Backoff with Full Jitter:");
        Console.WriteLine("=====================================");

        var strategy = new CompositeRetryDelayStrategy(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(100),
                2.0,
                TimeSpan.FromSeconds(30)),
            JitterStrategies.FullJitter(),
            new Random(42));

        for (var i = 0; i < 5; i++)
        {
            var delay = await strategy.GetDelayAsync(i);
            Console.WriteLine($"  Attempt {i + 1}: {delay.TotalMilliseconds:F2}ms");
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Demonstrates linear backoff with equal jitter.
    /// </summary>
    public static async Task LinearBackoffWithEqualJitter()
    {
        Console.WriteLine("Linear Backoff with Equal Jitter:");
        Console.WriteLine("=================================");

        var strategy = new CompositeRetryDelayStrategy(
            BackoffStrategies.LinearBackoff(
                TimeSpan.FromMilliseconds(100),
                TimeSpan.FromMilliseconds(50),
                TimeSpan.FromSeconds(30)),
            JitterStrategies.EqualJitter(),
            new Random(42));

        for (var i = 0; i < 5; i++)
        {
            var delay = await strategy.GetDelayAsync(i);
            Console.WriteLine($"  Attempt {i + 1}: {delay.TotalMilliseconds:F2}ms");
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Demonstrates fixed delay with no jitter.
    /// </summary>
    public static async Task FixedDelayWithNoJitter()
    {
        Console.WriteLine("Fixed Delay with No Jitter:");
        Console.WriteLine("===========================");

        var strategy = new CompositeRetryDelayStrategy(
            BackoffStrategies.FixedDelay(TimeSpan.FromMilliseconds(1000)),
            JitterStrategies.NoJitter(),
            new Random(42));

        for (var i = 0; i < 5; i++)
        {
            var delay = await strategy.GetDelayAsync(i);
            Console.WriteLine($"  Attempt {i + 1}: {delay.TotalMilliseconds:F2}ms");
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Demonstrates custom configuration.
    /// </summary>
    public static async Task CustomConfigurationExample()
    {
        Console.WriteLine("Custom Configuration Example:");
        Console.WriteLine("=============================");

        var strategy = new CompositeRetryDelayStrategy(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(50),
                1.5,
                TimeSpan.FromMinutes(2)),
            JitterStrategies.DecorrelatedJitter(TimeSpan.FromMinutes(1), 2.5),
            new Random(123));

        for (var i = 0; i < 5; i++)
        {
            var delay = await strategy.GetDelayAsync(i);
            Console.WriteLine($"  Attempt {i + 1}: {delay.TotalMilliseconds:F2}ms");
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Demonstrates error handling.
    /// </summary>
    public static async Task ErrorHandlingExample()
    {
        Console.WriteLine("Error Handling Example:");
        Console.WriteLine("======================");

        try
        {
            // This should throw due to invalid parameters
            var strategy = new CompositeRetryDelayStrategy(
                BackoffStrategies.ExponentialBackoff(
                    TimeSpan.FromMilliseconds(-100), // Invalid
                    2.0,
                    TimeSpan.FromSeconds(30)),
                JitterStrategies.FullJitter(),
                new Random(42));

            Console.WriteLine("Strategy created successfully (unexpected)");
        }
        catch (ArgumentException ex)
        {
            Console.WriteLine($"Caught expected exception: {ex.Message}");
        }

        try
        {
            // This should throw due to invalid jitter
            var strategy = new CompositeRetryDelayStrategy(
                BackoffStrategies.ExponentialBackoff(
                    TimeSpan.FromMilliseconds(100),
                    2.0,
                    TimeSpan.FromSeconds(30)),
                null!, // Invalid
                new Random(42));

            Console.WriteLine("Strategy created successfully (unexpected)");
        }
        catch (ArgumentNullException ex)
        {
            Console.WriteLine($"Caught expected exception: {ex.Message}");
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Demonstrates best practices.
    /// </summary>
    public static async Task BestPracticesExample()
    {
        Console.WriteLine("Best Practices Example:");
        Console.WriteLine("======================");

        // Use reasonable defaults
        var strategy = new CompositeRetryDelayStrategy(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(100),
                2.0,
                TimeSpan.FromMinutes(1)),
            JitterStrategies.FullJitter(),
            Random.Shared);

        Console.WriteLine("Strategy created with best practices:");
        Console.WriteLine("- Used reasonable base delay (100ms)");
        Console.WriteLine("- Used standard multiplier (2.0)");
        Console.WriteLine("- Set reasonable max delay (1 minute)");
        Console.WriteLine("- Added jitter for thundering herd prevention");
        Console.WriteLine("- Used shared random instance");

        for (var i = 0; i < 3; i++)
        {
            var delay = await strategy.GetDelayAsync(i);
            Console.WriteLine($"  Attempt {i + 1}: {delay.TotalMilliseconds:F2}ms");
        }

        Console.WriteLine();
    }
}

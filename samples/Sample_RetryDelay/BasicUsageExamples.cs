using NPipeline.Configuration.RetryDelay;
using NPipeline.Execution.RetryDelay;

namespace Sample_RetryDelay;

/// <summary>
///     Demonstrates basic usage patterns for retry delay strategies.
/// </summary>
public static class BasicUsageExamples
{
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

        var factory = new DefaultRetryDelayStrategyFactory();

        var configuration = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(100),
                2.0,
                TimeSpan.FromSeconds(30)),
            JitterStrategies.FullJitter());

        var strategy = factory.CreateStrategy(configuration);

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

        var factory = new DefaultRetryDelayStrategyFactory();

        var configuration = new RetryDelayStrategyConfiguration(
            BackoffStrategies.LinearBackoff(
                TimeSpan.FromMilliseconds(100),
                TimeSpan.FromMilliseconds(50),
                TimeSpan.FromSeconds(30)),
            JitterStrategies.EqualJitter());

        var strategy = factory.CreateStrategy(configuration);

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

        var factory = new DefaultRetryDelayStrategyFactory();

        var configuration = new RetryDelayStrategyConfiguration(
            BackoffStrategies.FixedDelay(TimeSpan.FromMilliseconds(1000)),
            JitterStrategies.NoJitter());

        var strategy = factory.CreateStrategy(configuration);

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

        var factory = new DefaultRetryDelayStrategyFactory();

        var configuration = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(50),
                1.5,
                TimeSpan.FromMinutes(2)),
            JitterStrategies.DecorrelatedJitter(TimeSpan.FromMinutes(1), 2.5));

        var strategy = factory.CreateStrategy(configuration);

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

        var factory = new DefaultRetryDelayStrategyFactory();

        try
        {
            // This should throw due to invalid parameters
            var configuration = new RetryDelayStrategyConfiguration(
                BackoffStrategies.ExponentialBackoff(
                    TimeSpan.FromMilliseconds(-100), // Invalid
                    2.0,
                    TimeSpan.FromSeconds(30)),
                JitterStrategies.FullJitter());

            var strategy = factory.CreateStrategy(configuration);

            Console.WriteLine("Strategy created successfully (unexpected)");
        }
        catch (ArgumentException ex)
        {
            Console.WriteLine($"Caught expected exception: {ex.Message}");
        }

        try
        {
            // This should work fine - null jitter is valid
            var configuration = new RetryDelayStrategyConfiguration(
                BackoffStrategies.ExponentialBackoff(
                    TimeSpan.FromMilliseconds(100),
                    2.0,
                    TimeSpan.FromSeconds(30))); // Valid - jitter is optional

            var strategy = factory.CreateStrategy(configuration);

            Console.WriteLine("Strategy created successfully with null jitter (expected)");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Caught unexpected exception: {ex.Message}");
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

        var factory = new DefaultRetryDelayStrategyFactory();

        // Use reasonable defaults
        var configuration = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(100),
                2.0,
                TimeSpan.FromMinutes(1)),
            JitterStrategies.FullJitter());

        var strategy = factory.CreateStrategy(configuration);

        Console.WriteLine("Strategy created with best practices:");
        Console.WriteLine("- Used reasonable base delay (100ms)");
        Console.WriteLine("- Used standard multiplier (2.0)");
        Console.WriteLine("- Set reasonable max delay (1 minute)");
        Console.WriteLine("- Added jitter for thundering herd prevention");
        Console.WriteLine("- Used factory pattern for strategy creation");

        for (var i = 0; i < 3; i++)
        {
            var delay = await strategy.GetDelayAsync(i);
            Console.WriteLine($"  Attempt {i + 1}: {delay.TotalMilliseconds:F2}ms");
        }

        Console.WriteLine();
    }
}

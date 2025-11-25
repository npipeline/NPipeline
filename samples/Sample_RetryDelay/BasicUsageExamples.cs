using NPipeline.Configuration;
using NPipeline.Configuration.RetryDelay;
using NPipeline.Execution.RetryDelay;

namespace Sample_RetryDelay;

/// <summary>
///     Basic usage examples for retry delay strategies.
///     Demonstrates common patterns and configurations.
/// </summary>
public static class BasicUsageExamples
{
    /// <summary>
    ///     Example 1: Simple exponential backoff with full jitter.
    ///     This is most common and recommended configuration for distributed systems.
    /// </summary>
    public static async Task ExponentialBackoffWithFullJitterExample()
    {
        Console.WriteLine("=== Exponential Backoff with Full Jitter Example ===");

        // Create strategy using extension method
        var retryOptions = PipelineRetryOptions.Default
            .WithExponentialBackoffAndFullJitter(
                TimeSpan.FromSeconds(1),
                2.0,
                TimeSpan.FromMinutes(1));

        // Create strategy from configuration
        var factory = new DefaultRetryDelayStrategyFactory();
        var strategy = factory.CreateStrategy(retryOptions.DelayStrategyConfiguration!);

        Console.WriteLine("Delay progression for 6 attempts:");

        for (var attempt = 0; attempt < 6; attempt++)
        {
            var delay = await strategy.GetDelayAsync(attempt);
            Console.WriteLine($"Attempt {attempt}: {delay.TotalSeconds:F1}s");
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Example 2: Linear backoff with equal jitter.
    ///     Good for predictable, gradual recovery scenarios.
    /// </summary>
    public static async Task LinearBackoffWithEqualJitterExample()
    {
        Console.WriteLine("=== Linear Backoff with Equal Jitter Example ===");

        // Create strategy using extension method
        var retryOptions = PipelineRetryOptions.Default
            .WithLinearBackoffAndEqualJitter(
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(2),
                TimeSpan.FromSeconds(30));

        // Create strategy from configuration
        var factory = new DefaultRetryDelayStrategyFactory();
        var strategy = factory.CreateStrategy(retryOptions.DelayStrategyConfiguration!);

        Console.WriteLine("Delay progression for 10 attempts:");

        for (var attempt = 0; attempt < 10; attempt++)
        {
            var delay = await strategy.GetDelayAsync(attempt);
            Console.WriteLine($"Attempt {attempt}: {delay.TotalSeconds:F1}s");
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Example 3: Fixed delay with no jitter.
    ///     Suitable for testing or scenarios requiring predictable behavior.
    /// </summary>
    public static async Task FixedDelayNoJitterExample()
    {
        Console.WriteLine("=== Fixed Delay with No Jitter Example ===");

        // Create strategy using extension method
        var retryOptions = PipelineRetryOptions.Default
            .WithFixedDelayNoJitter(TimeSpan.FromSeconds(5));

        // Create strategy from configuration
        var factory = new DefaultRetryDelayStrategyFactory();
        var strategy = factory.CreateStrategy(retryOptions.DelayStrategyConfiguration!);

        Console.WriteLine("Delay progression for 5 attempts:");

        for (var attempt = 0; attempt < 5; attempt++)
        {
            var delay = await strategy.GetDelayAsync(attempt);
            Console.WriteLine($"Attempt {attempt}: {delay.TotalSeconds:F1}s");
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Example 4: Custom configuration with exponential backoff and equal jitter.
    ///     Shows how to combine different strategies.
    /// </summary>
    public static async Task CustomConfigurationExample()
    {
        Console.WriteLine("=== Custom Configuration Example ===");

        // Create custom backoff and jitter configurations
        var backoffConfig = new ExponentialBackoffConfiguration(
            TimeSpan.FromMilliseconds(500),
            1.5, // Slower growth than default 2.0
            TimeSpan.FromSeconds(30));

        var jitterConfig = new EqualJitterConfiguration();

        // Combine using extension method
        var retryOptions = PipelineRetryOptions.Default
            .WithDelayStrategy(backoffConfig, jitterConfig);

        // Create strategy from configuration
        var factory = new DefaultRetryDelayStrategyFactory();
        var strategy = factory.CreateStrategy(retryOptions.DelayStrategyConfiguration!);

        Console.WriteLine("Delay progression for 8 attempts:");

        for (var attempt = 0; attempt < 8; attempt++)
        {
            var delay = await strategy.GetDelayAsync(attempt);
            Console.WriteLine($"Attempt {attempt}: {delay.TotalMilliseconds:F0}ms");
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Example 5: Using factory to create strategies.
    ///     Shows how to use the factory for strategy creation.
    /// </summary>
    public static async Task FactoryStrategyCreationExample()
    {
        Console.WriteLine("=== Factory Strategy Creation Example ===");

        // Create exponential backoff with full jitter configuration
        var backoffConfig = new ExponentialBackoffConfiguration(
            TimeSpan.FromSeconds(2),
            3.0,
            TimeSpan.FromMinutes(2));

        var jitterConfig = new FullJitterConfiguration();

        // Create composite configuration
        var compositeConfig = new RetryDelayStrategyConfiguration(
            backoffConfig, jitterConfig);

        // Create strategy using factory
        var factory = new DefaultRetryDelayStrategyFactory();
        var strategy = factory.CreateStrategy(compositeConfig);

        Console.WriteLine("Delay progression for 5 attempts:");

        for (var attempt = 0; attempt < 5; attempt++)
        {
            var delay = await strategy.GetDelayAsync(attempt);
            Console.WriteLine($"Attempt {attempt}: {delay.TotalSeconds:F1}s");
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Example 6: Comparing different strategies.
    ///     Shows how different strategies affect delay distribution.
    /// </summary>
    public static async Task StrategyComparisonExample()
    {
        Console.WriteLine("=== Strategy Comparison Example ===");

        // Create different configurations
        var exponentialConfig = new RetryDelayStrategyConfiguration(
            new ExponentialBackoffConfiguration(
                TimeSpan.FromSeconds(1),
                2.0,
                TimeSpan.FromSeconds(16)),
            new NoJitterConfiguration());

        var linearConfig = new RetryDelayStrategyConfiguration(
            new LinearBackoffConfiguration(
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(2),
                TimeSpan.FromSeconds(16)),
            new NoJitterConfiguration());

        var fixedConfig = new RetryDelayStrategyConfiguration(
            new FixedDelayConfiguration(
                TimeSpan.FromSeconds(8)),
            new NoJitterConfiguration());

        // Create strategies using factory
        var factory = new DefaultRetryDelayStrategyFactory();

        var exponentialStrategy = factory.CreateStrategy(exponentialConfig);
        var linearStrategy = factory.CreateStrategy(linearConfig);
        var fixedStrategy = factory.CreateStrategy(fixedConfig);

        Console.WriteLine("Comparing strategies for attempt 3:");

        var exponentialDelay = await exponentialStrategy.GetDelayAsync(3);
        var linearDelay = await linearStrategy.GetDelayAsync(3);
        var fixedDelay = await fixedStrategy.GetDelayAsync(3);

        Console.WriteLine($"Exponential: {exponentialDelay.TotalSeconds:F1}s");
        Console.WriteLine($"Linear:      {linearDelay.TotalSeconds:F1}s");
        Console.WriteLine($"Fixed:       {fixedDelay.TotalSeconds:F1}s");

        Console.WriteLine();
    }

    /// <summary>
    ///     Example 7: Integration with existing pipeline setup.
    ///     Shows how to use retry delay strategies in a real pipeline.
    /// </summary>
    public static void PipelineIntegrationExample()
    {
        Console.WriteLine("=== Pipeline Integration Example ===");

        // This shows how you would configure retry delay in a pipeline definition
        Console.WriteLine("Example pipeline configuration:");
        Console.WriteLine();
        Console.WriteLine("public class MyPipelineDefinition : IPipelineDefinition");
        Console.WriteLine("{");
        Console.WriteLine("    public void Define(PipelineBuilder builder, PipelineContext context)");
        Console.WriteLine("    {");
        Console.WriteLine("        var source = builder.AddSource<MySource, MyData>(\"Source\");");
        Console.WriteLine();
        Console.WriteLine("        var transform = builder.AddTransform<MyTransform, MyData, ProcessedData>(\"Transform\")");
        Console.WriteLine("            .WithRetryOptions(options => options");
        Console.WriteLine("                .WithExponentialBackoffAndFullJitter(");
        Console.WriteLine("                    baseDelay: TimeSpan.FromMilliseconds(500),");
        Console.WriteLine("                    multiplier: 2.0,");
        Console.WriteLine("                    maxDelay: TimeSpan.FromSeconds(30))");
        Console.WriteLine("                .WithMaxAttempts(5));");
        Console.WriteLine();
        Console.WriteLine("        var sink = builder.AddSink<MySink, ProcessedData>(\"Sink\")");
        Console.WriteLine("            .WithRetryOptions(options => options");
        Console.WriteLine("                .WithLinearBackoffAndEqualJitter(");
        Console.WriteLine("                    baseDelay: TimeSpan.FromSeconds(1),");
        Console.WriteLine("                    increment: TimeSpan.FromSeconds(1),");
        Console.WriteLine("                    maxDelay: TimeSpan.FromSeconds(10))");
        Console.WriteLine("                .WithMaxAttempts(3));");
        Console.WriteLine();
        Console.WriteLine("        builder.Connect(source, transform);");
        Console.WriteLine("        builder.Connect(transform, sink);");
        Console.WriteLine("    }");
        Console.WriteLine("}");

        Console.WriteLine();
    }

    /// <summary>
    ///     Example 8: Error handling and validation.
    ///     Shows how to handle configuration errors and edge cases.
    /// </summary>
    public static void ErrorHandlingExample()
    {
        Console.WriteLine("=== Error Handling and Validation Example ===");

        // Example of invalid configuration handling
        Console.WriteLine("1. Invalid base delay (must be > TimeSpan.Zero):");

        try
        {
            var config = new ExponentialBackoffConfiguration(
                TimeSpan.Zero, // Invalid
                2.0,
                TimeSpan.FromMinutes(1));
        }
        catch (ArgumentException ex)
        {
            Console.WriteLine($"   Caught: {ex.Message}");
        }

        Console.WriteLine("\n2. Invalid multiplier (must be > 1.0):");

        try
        {
            var config = new ExponentialBackoffConfiguration(
                TimeSpan.FromSeconds(1),
                0.5, // Invalid
                TimeSpan.FromMinutes(1));
        }
        catch (ArgumentException ex)
        {
            Console.WriteLine($"   Caught: {ex.Message}");
        }

        Console.WriteLine("\n3. Invalid max delay (must be >= base delay):");

        try
        {
            var config = new ExponentialBackoffConfiguration(
                TimeSpan.FromSeconds(10),
                2.0,
                TimeSpan.FromSeconds(5)); // Invalid
        }
        catch (ArgumentException ex)
        {
            Console.WriteLine($"   Caught: {ex.Message}");
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Example 9: Best practices for common scenarios.
    ///     Shows recommended configurations for different use cases.
    /// </summary>
    public static void BestPracticesExample()
    {
        Console.WriteLine("=== Best Practices Example ===");

        Console.WriteLine("1. Web API Calls (Recommended):");
        Console.WriteLine("   - Strategy: Exponential backoff with full jitter");
        Console.WriteLine("   - Base delay: 1 second");
        Console.WriteLine("   - Multiplier: 2.0");
        Console.WriteLine("   - Max delay: 1-5 minutes");
        Console.WriteLine("   - Max attempts: 3-5");
        Console.WriteLine();

        Console.WriteLine("2. Database Operations:");
        Console.WriteLine("   - Strategy: Linear backoff with equal jitter");
        Console.WriteLine("   - Base delay: 100ms");
        Console.WriteLine("   - Increment: 200ms");
        Console.WriteLine("   - Max delay: 5-10 seconds");
        Console.WriteLine("   - Max attempts: 3-5");
        Console.WriteLine();

        Console.WriteLine("3. File System Operations:");
        Console.WriteLine("   - Strategy: Fixed delay with no jitter");
        Console.WriteLine("   - Delay: 100-500ms");
        Console.WriteLine("   - Max attempts: 3-5");
        Console.WriteLine();

        Console.WriteLine("4. Message Queue Processing:");
        Console.WriteLine("   - Strategy: Exponential backoff with decorrelated jitter");
        Console.WriteLine("   - Base delay: 100ms");
        Console.WriteLine("   - Multiplier: 2.0");
        Console.WriteLine("   - Max delay: 30 seconds");
        Console.WriteLine("   - Max attempts: 5-10");
        Console.WriteLine();

        Console.WriteLine("5. Testing and Debugging:");
        Console.WriteLine("   - Strategy: Fixed delay with no jitter");
        Console.WriteLine("   - Delay: 50-100ms");
        Console.WriteLine("   - Max attempts: 2-3");
        Console.WriteLine("   - Purpose: Predictable behavior for debugging");
        Console.WriteLine();
    }

    /// <summary>
    ///     Run all basic usage examples.
    /// </summary>
    public static async Task RunAllExamples()
    {
        await ExponentialBackoffWithFullJitterExample();
        await LinearBackoffWithEqualJitterExample();
        await FixedDelayNoJitterExample();
        await CustomConfigurationExample();
        await FactoryStrategyCreationExample();
        await StrategyComparisonExample();
        PipelineIntegrationExample();
        ErrorHandlingExample();
        BestPracticesExample();
    }
}

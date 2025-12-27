using Microsoft.Extensions.Logging;

namespace Sample_RetryDelay;

/// <summary>
///     Entry point for retry delay examples.
///     Demonstrates various retry delay strategies and their usage patterns.
/// </summary>
internal sealed class Program
{
    private static readonly ILogger logger = LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger<Program>();

    public static async Task Main(string[] args)
    {
        Console.WriteLine("NPipeline Retry Delay Strategies Examples");
        Console.WriteLine("========================================");
        Console.WriteLine();

        if (args.Length == 0)
        {
            ShowMenu();
            return;
        }

        var command = args[0].ToLowerInvariant();

        try
        {
            switch (command)
            {
                case "basic":
                case "1":
                    Console.WriteLine("Running Basic Usage Examples...");
                    Console.WriteLine();
                    await BasicUsageExamples.RunAllExamples();
                    break;

                case "advanced":
                case "2":
                    Console.WriteLine("Running Advanced Scenarios...");
                    Console.WriteLine();
                    await AdvancedScenarios.RunAllExamples();
                    break;

                case "performance":
                case "3":
                    Console.WriteLine("Running Performance Comparison...");
                    Console.WriteLine();
                    await PerformanceComparison.RunAllExamples();
                    break;

                case "benchmarks":
                case "4":
                    Console.WriteLine("Running Performance Benchmarks...");
                    Console.WriteLine();
                    PerformanceComparison.StrategyPerformanceBenchmarks();
                    break;

                case "all":
                    Console.WriteLine("Running All Examples...");
                    Console.WriteLine();

                    Console.WriteLine("1. Basic Usage Examples:");
                    Console.WriteLine("=========================");
                    await BasicUsageExamples.RunAllExamples();

                    Console.WriteLine("\nPress any key to continue to Advanced Scenarios...");
                    Console.ReadKey();

                    Console.WriteLine("\n2. Advanced Scenarios:");
                    Console.WriteLine("=======================");
                    await AdvancedScenarios.RunAllExamples();

                    Console.WriteLine("\nPress any key to continue to Performance Comparison...");
                    Console.ReadKey();

                    Console.WriteLine("\n3. Performance Comparison:");
                    Console.WriteLine("============================");
                    await PerformanceComparison.RunAllExamples();
                    break;

                case "help":
                case "-h":
                case "--help":
                    ShowHelp();
                    break;

                default:
                    Console.WriteLine($"Unknown command: {command}");
                    Console.WriteLine();
                    ShowHelp();
                    break;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error running examples: {ex.Message}");
            Console.WriteLine($"Stack trace: {ex.StackTrace}");
        }

        Console.WriteLine();
        Console.WriteLine("Examples completed. Press any key to exit...");
        Console.ReadKey();
    }

    private static void ShowMenu()
    {
        Console.WriteLine("Choose which examples to run:");
        Console.WriteLine();
        Console.WriteLine("1. Basic Usage Examples     - Demonstrates common retry delay patterns");
        Console.WriteLine("2. Advanced Scenarios        - Shows complex configurations and integrations");
        Console.WriteLine("3. Performance Comparison    - Benchmarks and analyzes performance");
        Console.WriteLine("4. Performance Benchmarks    - Runs BenchmarkDotNet benchmarks");
        Console.WriteLine("5. All Examples              - Runs all example categories sequentially");
        Console.WriteLine();
        Console.WriteLine("Usage:");
        Console.WriteLine("  dotnet run -- [command]");
        Console.WriteLine();
        Console.WriteLine("Commands:");
        Console.WriteLine("  basic      Run basic usage examples");
        Console.WriteLine("  advanced    Run advanced scenarios");
        Console.WriteLine("  performance Run performance comparison");
        Console.WriteLine("  benchmarks  Run performance benchmarks");
        Console.WriteLine("  all         Run all examples");
        Console.WriteLine("  help        Show this help message");
        Console.WriteLine();
        Console.WriteLine("Examples:");
        Console.WriteLine("  dotnet run -- basic");
        Console.WriteLine("  dotnet run -- advanced");
        Console.WriteLine("  dotnet run -- performance");
        Console.WriteLine("  dotnet run -- benchmarks");
        Console.WriteLine("  dotnet run -- all");
        Console.WriteLine();
        Console.WriteLine("Or run without arguments to see this menu.");
    }

    private static void ShowHelp()
    {
        Console.WriteLine("NPipeline Retry Delay Strategies Examples - Help");
        Console.WriteLine("==================================================");
        Console.WriteLine();
        Console.WriteLine("This sample project demonstrates the retry delay functionality in NPipeline.");
        Console.WriteLine("It includes examples of different backoff and jitter strategies, their");
        Console.WriteLine("configurations, and performance characteristics.");
        Console.WriteLine();
        Console.WriteLine("Available Example Categories:");
        Console.WriteLine();
        Console.WriteLine("1. Basic Usage Examples:");
        Console.WriteLine("   - Exponential backoff with full jitter");
        Console.WriteLine("   - Linear backoff with equal jitter");
        Console.WriteLine("   - Fixed delay with no jitter");
        Console.WriteLine("   - Custom configurations");
        Console.WriteLine("   - Error handling and validation");
        Console.WriteLine("   - Best practices");
        Console.WriteLine();
        Console.WriteLine("2. Advanced Scenarios:");
        Console.WriteLine("   - Node-specific retry configurations");
        Console.WriteLine("   - Dynamic strategy selection");
        Console.WriteLine("   - Custom strategy implementations");
        Console.WriteLine("   - Circuit breaker integration");
        Console.WriteLine("   - Monitoring and observability");
        Console.WriteLine("   - Multi-region and cost-aware strategies");
        Console.WriteLine();
        Console.WriteLine("3. Performance Comparison:");
        Console.WriteLine("   - Strategy performance benchmarks");
        Console.WriteLine("   - Throughput analysis");
        Console.WriteLine("   - Latency distribution");
        Console.WriteLine("   - Resource usage comparison");
        Console.WriteLine("   - Thundering herd simulation");
        Console.WriteLine("   - Recovery time analysis");
        Console.WriteLine();
        Console.WriteLine("Key Concepts Demonstrated:");
        Console.WriteLine();
        Console.WriteLine("- Backoff Strategies:");
        Console.WriteLine("  * Exponential: delay = baseDelay * Math.Pow(multiplier, attemptNumber)");
        Console.WriteLine("  * Linear: delay = baseDelay + (increment * attemptNumber)");
        Console.WriteLine("  * Fixed: delay = constantDelay");
        Console.WriteLine();
        Console.WriteLine("- Jitter Strategies:");
        Console.WriteLine("  * Full Jitter: random between 0 and baseDelay");
        Console.WriteLine("  * Equal Jitter: baseDelay/2 + random(0, baseDelay/2)");
        Console.WriteLine("  * Decorrelated Jitter: random(baseDelay, previousDelay * 3)");
        Console.WriteLine("  * No Jitter: deterministic delays");
        Console.WriteLine();
        Console.WriteLine("- Integration Patterns:");
        Console.WriteLine("  * Extension methods for easy configuration");
        Console.WriteLine("  * Factory pattern for strategy creation");
        Console.WriteLine("  * Pipeline context integration");
        Console.WriteLine("  * Error handling and validation");
        Console.WriteLine();
        Console.WriteLine("For more information, see:");
        Console.WriteLine("- ../docs/RetryDelayStrategies.md - Comprehensive guide");
        Console.WriteLine("- ../docs/api/RetryDelay.md - API reference");
        Console.WriteLine("- ../docs/MigrationGuide.md - Migration from immediate retries");
        Console.WriteLine();
    }
}

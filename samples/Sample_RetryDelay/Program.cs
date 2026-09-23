namespace Sample_RetryDelay;

/// <summary>
///     Retry backoff examples for NPipeline.
/// </summary>
/// <remarks>
///     Run with no arguments to see every example, or pass <c>basic</c>, <c>advanced</c>, or <c>performance</c>.
///     Delays are kept to milliseconds so the whole sample finishes in a few seconds.
/// </remarks>
internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        Console.WriteLine("NPipeline Retry Backoff Examples");
        Console.WriteLine("================================");
        Console.WriteLine();

        var command = args.Length > 0 ? args[0].ToLowerInvariant() : "all";

        switch (command)
        {
            case "basic":
                await BasicUsageExamples.RunAllExamples();
                break;

            case "advanced":
                await AdvancedScenarios.RunAllExamples();
                break;

            case "performance":
                await PerformanceComparison.RunAllExamples();
                break;

            case "all":
                Section("1. Basic usage");
                await BasicUsageExamples.RunAllExamples();
                Section("2. Advanced scenarios");
                await AdvancedScenarios.RunAllExamples();
                Section("3. Performance comparison");
                await PerformanceComparison.RunAllExamples();
                break;

            default:
                Console.WriteLine($"Unknown command '{args[0]}'. Use basic, advanced, performance, or all (the default).");
                return 1;
        }

        Console.WriteLine("Examples completed.");
        return 0;
    }

    private static void Section(string title)
    {
        Console.WriteLine($"=== {title} ===");
        Console.WriteLine();
    }
}

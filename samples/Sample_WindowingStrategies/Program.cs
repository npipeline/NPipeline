using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Pipeline;

namespace Sample_WindowingStrategies;

/// <summary>
///     Entry point for the Advanced Windowing Strategies sample demonstrating sophisticated windowing techniques with NPipeline.
///     This sample shows how to implement session-based, dynamic, and custom trigger windowing strategies
///     for comprehensive user behavior analytics and pattern detection.
/// </summary>
public sealed class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: Advanced Windowing Strategies ===");
        Console.WriteLine();

        try
        {
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) =>
                {
                    // Add logging
                    _ = services.AddLogging(builder => builder.AddConsole());

                    // Add NPipeline services with assembly scanning
                    _ = services.AddNPipeline(Assembly.GetExecutingAssembly());
                })
                .Build();

            Console.WriteLine("Registered NPipeline services and scanned assemblies for nodes.");
            Console.WriteLine();

            // Display pipeline description
            Console.WriteLine("Pipeline Description:");
            Console.WriteLine(WindowingStrategiesPipeline.GetDescription());
            Console.WriteLine();

            // Configure pipeline parameters
            var pipelineParameters = new Dictionary<string, object>
            {
                ["UserEventCount"] = 200,
                ["EventGenerationInterval"] = TimeSpan.FromMilliseconds(75),
                ["SessionTimeout"] = TimeSpan.FromSeconds(2),
                ["MinWindowSize"] = 2,
                ["MaxWindowSize"] = 10,
                ["MaxWindowDuration"] = TimeSpan.FromHours(2),
                ["ActivityThreshold"] = 0.3,
                ["DiversityThreshold"] = 0.2,
                ["ConversionTriggerThreshold"] = 1,
                ["HighValueTriggerThreshold"] = 50.0,
                ["TimeBasedTriggerInterval"] = TimeSpan.FromSeconds(30),
                ["PatternConfidenceThreshold"] = 0.6,
                ["EnableDetailedOutput"] = true,
                ["EnablePatternAnalysis"] = true,
                ["EnablePerformanceMetrics"] = true,
            };

            Console.WriteLine();
            Console.WriteLine("Pipeline Configuration:");
            Console.WriteLine($"  User Event Count: {pipelineParameters["UserEventCount"]}");
            Console.WriteLine($"  Event Generation Interval: {pipelineParameters["EventGenerationInterval"]}");
            Console.WriteLine($"  Session Timeout: {pipelineParameters["SessionTimeout"]}");
            Console.WriteLine($"  Min Window Size: {pipelineParameters["MinWindowSize"]}");
            Console.WriteLine($"  Max Window Size: {pipelineParameters["MaxWindowSize"]}");
            Console.WriteLine($"  Activity Threshold: {pipelineParameters["ActivityThreshold"]}");
            Console.WriteLine($"  Diversity Threshold: {pipelineParameters["DiversityThreshold"]}");
            Console.WriteLine();

            Console.WriteLine("Starting advanced windowing strategies pipeline execution...");
            Console.WriteLine();

            var context = PipelineContext.Default;

            foreach (var parameter in pipelineParameters)
            {
                context.Parameters[parameter.Key] = parameter.Value;
            }

            var runner = host.Services.GetRequiredService<IPipelineRunner>();
            await runner.RunAsync<WindowingStrategiesPipeline>(context);

            Console.WriteLine();
            Console.WriteLine("Pipeline execution completed successfully!");
            Console.WriteLine();
            Console.WriteLine("Key Windowing Strategies Demonstrated:");
            Console.WriteLine("✓ Session-based windowing grouped user events into meaningful sessions");
            Console.WriteLine("✓ Dynamic windowing adapted window sizes based on data characteristics");
            Console.WriteLine("✓ Custom trigger windowing used complex business rules for window boundaries");
            Console.WriteLine("✓ Multiple parallel processing paths provided comprehensive analytics");
            Console.WriteLine("✓ Advanced pattern detection identified user behavior patterns");
            Console.WriteLine("✓ Comprehensive analytics provided deep insights into user behavior");
            Console.WriteLine();
            Console.WriteLine("Windowing Strategy Benefits:");
            Console.WriteLine("• Session-based: Traditional approach with timeout management");
            Console.WriteLine("• Dynamic: Adaptive sizing based on activity and diversity");
            Console.WriteLine("• Custom trigger: Business-rule-driven window boundaries");
            Console.WriteLine("• Multi-strategy: Comprehensive insights from different approaches");
            Console.WriteLine("• Pattern detection: Advanced behavioral analysis");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error executing pipeline: {ex.Message}");
            Console.WriteLine();
            Console.WriteLine("Full error details:");
            Console.WriteLine(ex.ToString());
            Environment.ExitCode = 1;
        }
    }
}

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NPipeline.Extensions.DependencyInjection;

namespace Sample_16_TypeConversionNode;

/// <summary>
///     Entry point for the TypeConversionNode sample demonstrating comprehensive type conversion functionality.
///     This sample shows how to use TypeConversionNode for various data transformation scenarios.
/// </summary>
public sealed class Program
{
    /// <summary>
    ///     Main entry point for the TypeConversionNode sample application.
    /// </summary>
    /// <param name="args">Command line arguments.</param>
    /// <returns>Exit code indicating success or failure.</returns>
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: Type Conversion Node ===");
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
            Console.WriteLine(TypeConversionPipeline.GetDescription());
            Console.WriteLine();

            // Demonstrate different conversion scenarios
            await DemonstrateConversionScenarios(host);

            Console.WriteLine();
            Console.WriteLine("All pipeline executions completed successfully!");
            Console.WriteLine();
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

    /// <summary>
    ///     Demonstrates different type conversion scenarios by running the pipeline with various configurations.
    /// </summary>
    /// <param name="host">The configured host with DI container.</param>
    private static async Task DemonstrateConversionScenarios(IHost host)
    {
        var scenarios = new[]
        {
            new { Name = "Basic String Conversion", EnableErrorHandling = false, RecordCount = 8 },
            new { Name = "String Conversion with Error Handling", EnableErrorHandling = true, RecordCount = 10 },
            new { Name = "JSON Data Processing", EnableErrorHandling = false, RecordCount = 6 },
            new { Name = "Legacy System Integration", EnableErrorHandling = false, RecordCount = 5 },
            new { Name = "Comprehensive Conversion Demo", EnableErrorHandling = true, RecordCount = 12 },
        };

        foreach (var scenario in scenarios)
        {
            Console.WriteLine("========================================");
            Console.WriteLine($"Scenario: {scenario.Name}");
            Console.WriteLine("========================================");
            Console.WriteLine();

            Console.WriteLine("Configuration:");
            Console.WriteLine($"  - Error Handling: {(scenario.EnableErrorHandling ? "Enabled" : "Disabled")}");
            Console.WriteLine($"  - Record Count: {scenario.RecordCount}");
            Console.WriteLine();

            Console.WriteLine("Starting pipeline execution...");
            Console.WriteLine();

            // Create pipeline with specific configuration
            var pipeline = new TypeConversionPipeline(scenario.EnableErrorHandling, scenario.RecordCount);

            // Execute the pipeline using DI container with parameters
            var parameters = new Dictionary<string, object>
            {
                ["EnableErrorHandling"] = scenario.EnableErrorHandling,
                ["RecordCount"] = scenario.RecordCount,
            };

            await host.Services.RunPipelineAsync<TypeConversionPipeline>(parameters);

            Console.WriteLine();
            Console.WriteLine("Press any key to continue to next scenario...");

            try
            {
                Console.ReadKey();
            }
            catch (InvalidOperationException)
            {
                // Console input not available (e.g., when running in CI/CD or redirected output)
                // Continue automatically after a short delay
                await Task.Delay(2000);
            }

            Console.WriteLine();
        }
    }
}

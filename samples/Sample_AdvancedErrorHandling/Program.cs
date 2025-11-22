using System.Reflection;
using Microsoft.Extensions.Hosting;
using NPipeline.Extensions.DependencyInjection;

namespace Sample_AdvancedErrorHandling;

/// <summary>
///     Entry point for the Advanced Error Handling sample demonstrating production-grade resilience patterns.
///     This sample shows circuit breaker patterns, dead letter queues, comprehensive retry strategies, and error recovery mechanisms.
/// </summary>
public sealed class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: Advanced Error Handling ===");
        Console.WriteLine();

        try
        {
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) =>
                {
                    // Add NPipeline services with assembly scanning
                    _ = services.AddNPipeline(Assembly.GetExecutingAssembly());
                })
                .Build();

            Console.WriteLine("Registered NPipeline services and scanned assemblies for nodes.");
            Console.WriteLine();

            // Display pipeline description
            Console.WriteLine("Pipeline Description:");
            Console.WriteLine(AdvancedErrorHandlingPipeline.GetDescription());
            Console.WriteLine();

            // Execute the pipeline using the DI container
            Console.WriteLine("Starting pipeline execution...");
            Console.WriteLine();

            await host.Services.RunPipelineAsync<AdvancedErrorHandlingPipeline>();

            Console.WriteLine();
            Console.WriteLine("Pipeline execution completed successfully!");
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

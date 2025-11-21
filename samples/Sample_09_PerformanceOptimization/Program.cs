using System.Reflection;
using Microsoft.Extensions.Hosting;
using NPipeline.Extensions.DependencyInjection;

namespace Sample_09_PerformanceOptimization;

/// <summary>
///     Entry point for the Performance Optimization Pipeline sample demonstrating advanced NPipeline performance concepts.
///     This sample shows ValueTask optimization, memory allocation reduction, synchronous fast paths, and performance measurement.
/// </summary>
public sealed class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: Performance Optimization Pipeline ===");
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

            Console.WriteLine("Registered NPipeline services and scanned assemblies for performance optimization nodes.");
            Console.WriteLine();

            // Display pipeline description
            Console.WriteLine("Pipeline Description:");
            Console.WriteLine(PerformanceOptimizationPipeline.GetDescription());
            Console.WriteLine();

            // Execute the pipeline using the DI container
            Console.WriteLine("Starting performance optimization pipeline execution...");
            Console.WriteLine("This will demonstrate various optimization techniques and benchmark their effectiveness.");
            Console.WriteLine();

            await host.Services.RunPipelineAsync<PerformanceOptimizationPipeline>();

            Console.WriteLine();
            Console.WriteLine("Performance optimization pipeline execution completed successfully!");
            Console.WriteLine("Check the output above for detailed performance analysis and recommendations.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error executing performance optimization pipeline: {ex.Message}");
            Console.WriteLine();
            Console.WriteLine("Full error details:");
            Console.WriteLine(ex.ToString());
            Environment.ExitCode = 1;
        }
    }
}

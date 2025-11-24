using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NPipeline.Extensions.DependencyInjection;
using Sample_WatermarkHandling.Strategies;

namespace Sample_WatermarkHandling;

/// <summary>
///     Entry point for the WatermarkHandling sample demonstrating advanced event-time processing for IoT manufacturing platforms.
///     This sample shows how to handle watermarks, late data, and temporal alignment in complex IoT scenarios.
/// </summary>
public sealed class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: WatermarkHandling for IoT Manufacturing Platform ===");
        Console.WriteLine();

        try
        {
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) =>
                {
                    // Add NPipeline services with assembly scanning
                    _ = services.AddNPipeline(Assembly.GetExecutingAssembly());

                    // Register strategy classes for dependency injection
                    services.AddSingleton<NetworkAwareWatermarkStrategy>();
                    services.AddSingleton<DeviceSpecificLatenessStrategy>();
                    services.AddSingleton<DynamicAdjustmentStrategy>();
                })
                .Build();

            Console.WriteLine("Registered NPipeline services and scanned assemblies for nodes.");
            Console.WriteLine();

            // Display pipeline description
            Console.WriteLine("Pipeline Description:");
            Console.WriteLine(WatermarkHandlingPipeline.GetDescription());
            Console.WriteLine();

            // Execute the pipeline using the DI container
            Console.WriteLine("Starting pipeline execution...");
            Console.WriteLine("Watch for advanced watermark handling across multiple IoT sensor networks!");
            Console.WriteLine();

            await host.Services.RunPipelineAsync<WatermarkHandlingPipeline>();

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

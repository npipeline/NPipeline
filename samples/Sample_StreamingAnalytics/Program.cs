using System.Reflection;
using Microsoft.Extensions.Hosting;
using NPipeline.Extensions.DependencyInjection;

namespace Sample_StreamingAnalytics;

/// <summary>
///     Entry point for the Streaming Analytics Pipeline sample demonstrating windowed processing and real-time aggregations.
///     This sample shows tumbling windows, sliding windows, and stream analytics capabilities.
/// </summary>
public sealed class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: Streaming Analytics Pipeline ===");
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
            Console.WriteLine(StreamingAnalyticsPipeline.GetDescription());
            Console.WriteLine();

            // Execute the pipeline using the DI container
            Console.WriteLine("Starting pipeline execution...");
            Console.WriteLine();

            await host.Services.RunPipelineAsync<StreamingAnalyticsPipeline>();

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

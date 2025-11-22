using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NPipeline.Extensions.DependencyInjection;

namespace Sample_14_TimeWindowedJoinNode;

/// <summary>
///     Entry point for Time-Windowed Join Node sample demonstrating time-windowed join functionality with NPipeline.
///     This sample shows how to use time-windowed joins to correlate data streams based on time windows,
///     demonstrating different window strategies and temporal data enrichment patterns.
/// </summary>
public sealed class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: Time-Windowed Join Node ===");
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
            Console.WriteLine(TimeWindowedJoinPipeline.GetDescription());
            Console.WriteLine();

            // Demonstrate time-windowed join
            await DemonstrateTimeWindowedJoin(host);

            Console.WriteLine();
            Console.WriteLine("Pipeline execution completed successfully!");
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
    ///     Demonstrates time-windowed join functionality by running pipeline.
    /// </summary>
    /// <param name="host">The configured host with DI container.</param>
    private static async Task DemonstrateTimeWindowedJoin(IHost host)
    {
        Console.WriteLine("========================================");
        Console.WriteLine("Demonstrating Time-Windowed Join");
        Console.WriteLine("========================================");
        Console.WriteLine();

        // Create pipeline
        var pipeline = new TimeWindowedJoinPipeline();
        Console.WriteLine("Time-Windowed Join Pipeline");
        Console.WriteLine("This pipeline correlates sensor readings with maintenance events within time windows.");
        Console.WriteLine();

        Console.WriteLine("Starting pipeline execution...");
        Console.WriteLine();

        // Execute the pipeline using the service provider extension method
        await host.Services.RunPipelineAsync<TimeWindowedJoinPipeline>(CancellationToken.None);

        Console.WriteLine();
    }
}

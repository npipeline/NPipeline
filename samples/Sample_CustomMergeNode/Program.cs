using System;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using NPipeline.Extensions.DependencyInjection;

namespace Sample_CustomMergeNode;

/// <summary>
///     Entry point for Custom Merge Node sample demonstrating financial trading system scenarios.
///     This sample shows how to merge market data from multiple exchanges with priority-based processing.
/// </summary>
public sealed class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: Custom Merge Node for Financial Trading System ===");
        Console.WriteLine();

        try
        {
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) =>
                {
                    // Add NPipeline services with assembly scanning
                    services.AddNPipeline(Assembly.GetExecutingAssembly());
                })
                .Build();

            Console.WriteLine("Registered NPipeline services and scanned assemblies for nodes.");
            Console.WriteLine();

            // Display pipeline description
            Console.WriteLine("Pipeline Description:");
            Console.WriteLine(CustomMergeNodePipeline.GetDescription());
            Console.WriteLine();

            // Execute pipeline using the DI container
            Console.WriteLine("Starting pipeline execution...");
            Console.WriteLine("Watch for priority-based merging of market data from NYSE, NASDAQ, and International exchanges!");
            Console.WriteLine();

            await host.Services.RunPipelineAsync<CustomMergeNodePipeline>();

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

        Console.WriteLine();
        Console.WriteLine("Sample execution complete.");
    }
}

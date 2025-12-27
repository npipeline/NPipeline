using System.Reflection;
using Microsoft.Extensions.Hosting;
using NPipeline.Extensions.DependencyInjection;

namespace Sample_AdvancedAggregateNode;

/// <summary>
///     Entry point for the AdvancedAggregateNode sample demonstrating financial risk analysis scenarios.
///     This sample shows complex aggregation patterns with different accumulator and result types for risk calculations.
/// </summary>
public sealed class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: AdvancedAggregateNode for Financial Risk Analysis ===");
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
            Console.WriteLine(AdvancedAggregateNodePipeline.GetDescription());
            Console.WriteLine();

            // Execute the pipeline using the DI container
            Console.WriteLine("Starting financial risk analysis pipeline...");
            Console.WriteLine("Press Ctrl+C to stop the pipeline.");
            Console.WriteLine();

            await host.Services.RunPipelineAsync<AdvancedAggregateNodePipeline>();

            Console.WriteLine();
            Console.WriteLine("Financial risk analysis pipeline completed successfully!");
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

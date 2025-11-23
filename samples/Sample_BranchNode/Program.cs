using System.Reflection;
using Microsoft.Extensions.Hosting;
using NPipeline.Extensions.DependencyInjection;

namespace Sample_BranchNode;

/// <summary>
///     Entry point for BranchNode sample demonstrating e-commerce order processing scenarios.
///     This sample shows how to branch data flow to multiple processing paths for different business operations.
/// </summary>
public sealed class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: BranchNode for E-Commerce Order Processing ===");
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
            Console.WriteLine(BranchNodePipeline.GetDescription());
            Console.WriteLine();

            // Execute pipeline using the DI container
            Console.WriteLine("Starting pipeline execution...");
            Console.WriteLine("Watch for parallel processing of inventory, analytics, and notifications!");
            Console.WriteLine();

            await host.Services.RunPipelineAsync<BranchNodePipeline>();

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
        Console.WriteLine("Press any key to exit...");
        Console.ReadKey();
    }
}

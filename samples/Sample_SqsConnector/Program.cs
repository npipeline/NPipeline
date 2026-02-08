using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NPipeline.Extensions.DependencyInjection;

namespace Sample_SqsConnector;

/// <summary>
///     Entry point for SQS Connector sample demonstrating message processing with SQS queues.
///     This sample shows how to use SqsSourceNode and SqsSinkNode for reliable message processing.
/// </summary>
public sealed class Program
{
    /// <summary>
    ///     Runs the SQS connector sample.
    /// </summary>
    /// <param name="args">Command-line arguments.</param>
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: SQS Connector for Order Processing ===");
        Console.WriteLine();

        try
        {
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) =>
                {
                    // Add NPipeline services with assembly scanning
                    _ = services.AddNPipeline(Assembly.GetExecutingAssembly());

                    // Register SQS configuration for node construction
                    services.AddSingleton(SqsConnectorPipeline.CreateConfiguration());
                })
                .Build();

            Console.WriteLine("Registered NPipeline services and scanned assemblies for nodes.");
            Console.WriteLine();

            // Display pipeline description
            Console.WriteLine("Pipeline Description:");
            Console.WriteLine(SqsConnectorPipeline.GetDescription());
            Console.WriteLine();

            // Execute the pipeline using DI container
            Console.WriteLine("Starting pipeline execution...");
            Console.WriteLine("Press Ctrl+C to stop.");
            Console.WriteLine();

            await host.Services.RunPipelineAsync<SqsConnectorPipeline>();

            Console.WriteLine();
            Console.WriteLine("Pipeline execution completed successfully!");
        }
        catch (Exception ex)
        {
            Console.WriteLine();
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Error executing pipeline: {ex.Message}");
            Console.ResetColor();
            Console.WriteLine();
            Console.WriteLine("Full error details:");
            Console.WriteLine(ex.ToString());
            Environment.ExitCode = 1;
        }
    }
}

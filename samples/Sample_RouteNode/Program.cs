using System.Reflection;
using Microsoft.Extensions.Hosting;
using NPipeline.Extensions.DependencyInjection;

namespace Sample_RouteNode;

/// <summary>
///     Entry point for the RouteNode sample demonstrating conditional routing with named outputs.
/// </summary>
public sealed class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: RouteNode ===");
        Console.WriteLine();

        try
        {
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) =>
                {
                    _ = services.AddNPipeline(Assembly.GetExecutingAssembly());
                })
                .Build();

            Console.WriteLine("Registered NPipeline services and scanned assemblies for nodes.");
            Console.WriteLine();

            Console.WriteLine("Pipeline Description:");
            Console.WriteLine(RouteNodePipeline.GetDescription());
            Console.WriteLine();

            Console.WriteLine("Starting pipeline execution...");
            Console.WriteLine();

            await host.Services.RunPipelineAsync<RouteNodePipeline>();

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

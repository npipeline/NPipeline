using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NPipeline.Extensions.DependencyInjection;

namespace Sample_UnbatchingNode;

/// <summary>
///     Entry point for the Unbatching Node sample demonstrating unbatching functionality with NPipeline.
///     This sample shows how to convert batched analytics results back to individual item streams for real-time processing.
/// </summary>
public sealed class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: Unbatching Node ===");
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
            Console.WriteLine(UnbatchingStreamConversionPipeline.GetDescription());
            Console.WriteLine();

            // Configure pipeline parameters
            var pipelineParameters = new Dictionary<string, object>
            {
                ["BatchSize"] = 10, // Smaller batches for more frequent processing
                ["BatchTimeout"] = TimeSpan.FromSeconds(1.5),
                ["MarketDataEventCount"] = 75, // Reduced for clearer demonstration
                ["MarketDataInterval"] = TimeSpan.FromMilliseconds(100),
                ["PriceAnomalyThreshold"] = 1.5m, // More sensitive for demonstration
                ["VolatilityThreshold"] = 3.0m, // More sensitive for demonstration
                ["AnomalyScoreThreshold"] = 0.6, // More sensitive for demonstration
            };

            Console.WriteLine("Pipeline Configuration:");
            Console.WriteLine($"  Batch Size: {pipelineParameters["BatchSize"]}");
            Console.WriteLine($"  Batch Timeout: {pipelineParameters["BatchTimeout"]}");
            Console.WriteLine($"  Market Data Events: {pipelineParameters["MarketDataEventCount"]}");
            Console.WriteLine($"  Market Data Interval: {pipelineParameters["MarketDataInterval"]}");
            Console.WriteLine($"  Price Anomaly Threshold: {pipelineParameters["PriceAnomalyThreshold"]}%");
            Console.WriteLine($"  Volatility Threshold: {pipelineParameters["VolatilityThreshold"]}%");
            Console.WriteLine($"  Anomaly Score Threshold: {pipelineParameters["AnomalyScoreThreshold"]}");
            Console.WriteLine();

            // Execute the pipeline using the DI container with parameters
            Console.WriteLine("Starting pipeline execution...");
            Console.WriteLine();

            await host.Services.RunPipelineAsync<UnbatchingStreamConversionPipeline>(pipelineParameters);

            Console.WriteLine();
            Console.WriteLine("Pipeline execution completed successfully!");
            Console.WriteLine();
            Console.WriteLine("Key UnbatchingNode Demonstrations:");
            Console.WriteLine("✓ Individual market data events were batched for efficient analytics processing");
            Console.WriteLine("✓ Batch analytics provided comprehensive insights across multiple events");
            Console.WriteLine("✓ UnbatchingNode converted batch results back to individual market data events");
            Console.WriteLine("✓ Individual events were processed for real-time alerting");
            Console.WriteLine("✓ Pipeline demonstrated efficient batch processing + individual event processing");
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

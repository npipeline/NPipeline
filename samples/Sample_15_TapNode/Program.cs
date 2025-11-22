using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NPipeline.Extensions.DependencyInjection;

namespace Sample_15_TapNode;

/// <summary>
///     Entry point for the TapNode sample demonstrating TapNode functionality with NPipeline.
///     This sample shows how to use TapNode for non-intrusive monitoring, audit logging,
///     metrics collection, and alert generation without affecting the main data processing flow.
/// </summary>
public sealed class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: TapNode ===");
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

                    // Add TapNode-specific services
                    TapNodePipeline.ConfigureServices(services);
                })
                .Build();

            Console.WriteLine("Registered NPipeline services and configured TapNode pipeline.");
            Console.WriteLine();

            // Display pipeline description
            Console.WriteLine("Pipeline Description:");
            Console.WriteLine(TapNodePipeline.GetDescription());
            Console.WriteLine();

            // Demonstrate TapNode functionality
            await DemonstrateTapNode(host);

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
    ///     Demonstrates TapNode functionality by running the pipeline.
    /// </summary>
    /// <param name="host">The configured host with DI container.</param>
    private static async Task DemonstrateTapNode(IHost host)
    {
        Console.WriteLine("========================================");
        Console.WriteLine("Demonstrating TapNode Functionality");
        Console.WriteLine("========================================");
        Console.WriteLine();

        Console.WriteLine("This demonstration shows:");
        Console.WriteLine("1. Main transaction processing pipeline");
        Console.WriteLine("2. Non-intrusive monitoring at multiple tap points");
        Console.WriteLine("3. Audit logging without affecting main flow");
        Console.WriteLine("4. Metrics collection for observability");
        Console.WriteLine("5. Alert generation for suspicious activities");
        Console.WriteLine();

        Console.WriteLine("Key TapNode Benefits:");
        Console.WriteLine("✓ Non-intrusive monitoring - main flow continues uninterrupted");
        Console.WriteLine("✓ Multiple tap points - monitor at different pipeline stages");
        Console.WriteLine("✓ Side effects - audit, metrics, alerts without core logic changes");
        Console.WriteLine("✓ Error isolation - tap failures don't affect main pipeline");
        Console.WriteLine("✓ Performance monitoring - track processing times and bottlenecks");
        Console.WriteLine();

        Console.WriteLine("Pipeline Architecture:");

        Console.WriteLine(
            "TransactionSource → [Tap: Audit/Metrics/Alerts] → Validation → [Tap: Audit/Metrics/Alerts] → RiskAssessment → [Tap: Audit/Metrics/Alerts] → ConsoleSink");

        Console.WriteLine();

        Console.WriteLine("Starting pipeline execution...");
        Console.WriteLine();

        // Execute the pipeline using the service provider extension method
        await host.Services.RunPipelineAsync<TapNodePipeline>(CancellationToken.None);

        Console.WriteLine();
        Console.WriteLine("TapNode demonstration completed!");
        Console.WriteLine();
        Console.WriteLine("What you observed:");
        Console.WriteLine("• Main transaction processing results in the console output");
        Console.WriteLine("• AUDIT log entries showing transaction flow through pipeline stages");
        Console.WriteLine("• METRICS summaries for each pipeline stage");
        Console.WriteLine("• ALERT notifications for suspicious transactions and performance issues");
        Console.WriteLine("• All monitoring happened without affecting the main processing flow");
        Console.WriteLine();
    }
}

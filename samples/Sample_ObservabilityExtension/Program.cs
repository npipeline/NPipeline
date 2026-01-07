using System.Diagnostics;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Observability;
using NPipeline.Observability.DependencyInjection;
using NPipeline.Pipeline;
using Sample_ObservabilityExtension;

await RunPipelineAsync();

async Task RunPipelineAsync()
{
    Console.WriteLine("=== NPipeline Sample: Observability Extension ===");
    Console.WriteLine();

    try
    {
        var host = Host.CreateDefaultBuilder()
            .ConfigureServices((context, services) =>
            {
                // Add NPipeline services with assembly scanning
                _ = services.AddNPipeline(Assembly.GetExecutingAssembly());

                // Add observability services - this registers:
                // - IObservabilityCollector (scoped)
                // - IExecutionObserver (scoped, connected to the collector)
                // - IObservablePipelineContextFactory (scoped, for creating contexts with observability)
                //
                // By default, memory metrics are disabled for optimal performance. Enable them with:
                // services.AddNPipelineObservability(ObservabilityExtensionOptions.WithMemoryMetrics);
                // or: services.AddNPipelineObservability(new ObservabilityExtensionOptions { EnableMemoryMetrics = true });
                _ = services.AddNPipelineObservability();
            })
            .Build();

        Console.WriteLine("Registered NPipeline services with observability extension.");
        Console.WriteLine();

        // Display pipeline description
        Console.WriteLine("Pipeline Description:");
        Console.WriteLine(ObservabilityDemoPipeline.GetDescription());
        Console.WriteLine();

        // Execute the pipeline
        Console.WriteLine("Starting pipeline execution with automatic metrics collection...");
        Console.WriteLine();

        var stopwatch = Stopwatch.StartNew();

        // Create an async scope for proper async disposal of nodes with IAsyncDisposable
        await using (var scope = host.Services.CreateAsyncScope())
        {
            var runner = scope.ServiceProvider.GetRequiredService<IPipelineRunner>();

            // Use the context factory to create a context with observability automatically configured
            // This eliminates the need to manually wire up the execution observer
            var contextFactory = scope.ServiceProvider.GetRequiredService<IObservablePipelineContextFactory>();
            await using var context = contextFactory.Create();

            // Get the collector from this scope's DI container
            var collector = scope.ServiceProvider.GetRequiredService<IObservabilityCollector>();

            // Run the pipeline - metrics are automatically collected via the execution observer
            await runner.RunAsync<ObservabilityDemoPipeline>(context);

            stopwatch.Stop();

            Console.WriteLine();
            Console.WriteLine("=== EXECUTION SUMMARY ===");
            Console.WriteLine($"Total Execution Time: {stopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine();

            // Display collected metrics from this scope's collector instance
            Console.WriteLine("=== NODE METRICS ===");
            var nodeMetrics = collector.GetNodeMetrics();

            if (nodeMetrics.Count == 0)
            {
                Console.WriteLine("No metrics collected.");
            }
            else
            {
                foreach (var metrics in nodeMetrics)
                {
                    Console.WriteLine();
                    Console.WriteLine($"Node: {metrics.NodeId}");
                    Console.WriteLine($"  Duration: {metrics.DurationMs}ms");
                    Console.WriteLine($"  Items Processed: {metrics.ItemsProcessed}");
                    Console.WriteLine($"  Items Emitted: {metrics.ItemsEmitted}");
                    Console.WriteLine($"  Success: {metrics.Success}");
                    if (metrics.ThreadId.HasValue)
                    {
                        Console.WriteLine($"  Thread ID: {metrics.ThreadId}");
                    }
                    if (metrics.ThroughputItemsPerSec.HasValue)
                    {
                        Console.WriteLine($"  Throughput: {metrics.ThroughputItemsPerSec:F2} items/sec");
                    }
                    if (metrics.AverageItemProcessingMs.HasValue)
                    {
                        Console.WriteLine($"  Avg Processing Time: {metrics.AverageItemProcessingMs:F2}ms per item");
                    }
                    if (metrics.RetryCount > 0)
                    {
                        Console.WriteLine($"  Retry Count: {metrics.RetryCount}");
                    }
                    if (metrics.PeakMemoryUsageMb.HasValue)
                    {
                        Console.WriteLine($"  Peak Memory: {metrics.PeakMemoryUsageMb}MB");
                    }
                }
            }
        }

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

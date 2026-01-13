// See https://aka.ms/new-console-template for more information

using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Lineage.DependencyInjection;
using Sample_LineageExtension.Nodes;

namespace Sample_LineageExtension;

/// <summary>
///     Entry point for the Lineage Extension sample demonstrating NPipeline lineage tracking capabilities.
///     This sample shows various lineage tracking features including basic tracking, sampling strategies,
///     complex pipelines with joins, branching, error handling, and custom lineage sinks.
/// </summary>
public sealed class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: Lineage Extension ===");
        Console.WriteLine();

        try
        {
            // Parse command line arguments
            var scenario = args.Length > 0 && Enum.TryParse<DemoScenario>(args[0], true, out var parsedScenario)
                ? parsedScenario
                : DemoScenario.BasicLineageTracking;

            // Build the host with DI
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) =>
                {
                    // Add NPipeline services with assembly scanning
                    _ = services.AddNPipeline(Assembly.GetExecutingAssembly());

                    // Add lineage services with appropriate configuration based on scenario
                    ConfigureLineageServices(services, scenario);
                })
                .Build();

            Console.WriteLine("Registered NPipeline services and scanned assemblies for nodes.");
            Console.WriteLine($"Running scenario: {scenario}");
            Console.WriteLine();

            // Execute the pipeline based on the selected scenario
            await RunScenarioAsync(host.Services, scenario);

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

    /// <summary>
    ///     Configures lineage services based on the selected scenario.
    /// </summary>
    private static void ConfigureLineageServices(IServiceCollection services, DemoScenario scenario)
    {
        switch (scenario)
        {
            case DemoScenario.BasicLineageTracking:
                _ = services.AddNPipelineLineage();
                Console.WriteLine("Lineage: Basic tracking enabled (all items tracked)");
                break;

            case DemoScenario.DeterministicSampling:
                // Note: Deterministic sampling is configured via LineageOptions in the LineageCollector
                // For this sample, we use a custom sink with a descriptive output path
                _ = services.AddNPipelineLineage(sp => new CustomLineageSink("lineage-deterministic.json"));
                Console.WriteLine("Lineage: Deterministic sampling enabled (every 3rd item)");
                break;

            case DemoScenario.RandomSampling:
                _ = services.AddNPipelineLineage(sp => new CustomLineageSink("lineage-random.json"));
                Console.WriteLine("Lineage: Random sampling enabled (~33% of items)");
                break;

            case DemoScenario.ComplexJoin:
                _ = services.AddNPipelineLineage();
                Console.WriteLine("Lineage: Basic tracking enabled for complex join pipeline");
                break;

            case DemoScenario.BranchingWithLineage:
                _ = services.AddNPipelineLineage();
                Console.WriteLine("Lineage: Basic tracking enabled for branching pipeline");
                break;

            case DemoScenario.ErrorHandlingWithLineage:
                _ = services.AddNPipelineLineage();
                Console.WriteLine("Lineage: Basic tracking enabled for error handling pipeline");
                break;

            case DemoScenario.CustomLineageSink:
                _ = services.AddNPipelineLineage(sp => new CustomLineageSink("lineage-custom.json"));
                Console.WriteLine("Lineage: Custom sink enabled with full tracking");
                break;

            default:
                _ = services.AddNPipelineLineage();
                break;
        }
    }

    /// <summary>
    ///     Runs the specified pipeline scenario.
    /// </summary>
    private static async Task RunScenarioAsync(IServiceProvider services, DemoScenario scenario)
    {
        switch (scenario)
        {
            case DemoScenario.BasicLineageTracking:
                Console.WriteLine("=== Basic Lineage Tracking Scenario ===");
                Console.WriteLine("Demonstrates fundamental lineage tracking through a simple pipeline.");
                Console.WriteLine();
                await services.RunPipelineAsync<BasicLineageTrackingPipeline>();
                break;

            case DemoScenario.DeterministicSampling:
                Console.WriteLine("=== Deterministic Sampling Scenario ===");
                Console.WriteLine("Demonstrates deterministic sampling (every Nth item) to reduce overhead.");
                Console.WriteLine();
                await services.RunPipelineAsync<DeterministicSamplingPipeline>();
                break;

            case DemoScenario.RandomSampling:
                Console.WriteLine("=== Random Sampling Scenario ===");
                Console.WriteLine("Demonstrates random sampling (~N% of items) to reduce overhead.");
                Console.WriteLine();
                await services.RunPipelineAsync<RandomSamplingPipeline>();
                break;

            case DemoScenario.ComplexJoin:
                Console.WriteLine("=== Complex Join Scenario ===");
                Console.WriteLine("Demonstrates lineage tracking across multi-source joins.");
                Console.WriteLine();
                await services.RunPipelineAsync<ComplexJoinPipeline>();
                break;

            case DemoScenario.BranchingWithLineage:
                Console.WriteLine("=== Branching with Lineage Scenario ===");
                Console.WriteLine("Demonstrates lineage tracking through branching and recombining paths.");
                Console.WriteLine();
                await services.RunPipelineAsync<BranchingWithLineagePipeline>();
                break;

            case DemoScenario.ErrorHandlingWithLineage:
                Console.WriteLine("=== Error Handling with Lineage Scenario ===");
                Console.WriteLine("Demonstrates lineage tracking for error outcomes and retry scenarios.");
                Console.WriteLine();
                await services.RunPipelineAsync<ErrorHandlingWithLineagePipeline>();
                break;

            case DemoScenario.CustomLineageSink:
                Console.WriteLine("=== Custom Lineage Sink Scenario ===");
                Console.WriteLine("Demonstrates custom IPipelineLineageSink implementation for exporting lineage data.");
                Console.WriteLine();
                await services.RunPipelineAsync<CustomLineageSinkPipeline>();
                break;

            default:
                Console.WriteLine("=== Basic Lineage Tracking Scenario (Default) ===");
                Console.WriteLine();
                await services.RunPipelineAsync<BasicLineageTrackingPipeline>();
                break;
        }
    }
}

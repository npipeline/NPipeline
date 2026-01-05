using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Nodes;

namespace Sample_SelfJoinNode;

/// <summary>
///     Entry point for the Self Join Node sample demonstrating self-join functionality with NPipeline.
///     This sample shows how to use the AddSelfJoin extension method to join a data stream with itself,
///     demonstrating year-over-year sales comparison as a practical use case.
/// </summary>
public sealed class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: Self Join Node ===");
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
            Console.WriteLine(SelfJoinPipeline.GetDescription());
            Console.WriteLine();

            // Demonstrate different join types
            await DemonstrateJoinTypes(host);

            Console.WriteLine();
            Console.WriteLine("All pipeline executions completed successfully!");
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
    ///     Demonstrates different join types by running the pipeline with each configuration.
    /// </summary>
    /// <param name="host">The configured host with DI container.</param>
    private static async Task DemonstrateJoinTypes(IHost host)
    {
        var joinTypes = new[]
        {
            JoinType.Inner,
            JoinType.LeftOuter,
            JoinType.RightOuter,
            JoinType.FullOuter,
        };

        foreach (var joinType in joinTypes)
        {
            Console.WriteLine("========================================");
            Console.WriteLine($"Demonstrating {joinType} Join");
            Console.WriteLine("========================================");
            Console.WriteLine();

            // Create pipeline with specific join type
            var pipeline = new SelfJoinPipeline(joinType);
            Console.WriteLine($"Join Type: {joinType}");
            Console.WriteLine(pipeline.GetJoinTypeDescription());
            Console.WriteLine($"Comparison Year: {pipeline.GetComparisonYear()}");
            Console.WriteLine();

            Console.WriteLine("Starting pipeline execution...");
            Console.WriteLine();

            // Execute the pipeline using DI container with join type parameter
            var parameters = new Dictionary<string, object>
            {
                ["JoinType"] = joinType,
                ["ComparisonYear"] = 2024,
            };

            await host.Services.RunPipelineAsync<SelfJoinPipeline>(parameters, CancellationToken.None);

            Console.WriteLine();
            Console.WriteLine("Press any key to continue to next join type...");

            try
            {
                Console.ReadKey();
            }
            catch (InvalidOperationException)
            {
                // Console input not available (e.g., when running in CI/CD or redirected output)
                // Continue automatically after a short delay
                await Task.Delay(1000);
            }

            Console.WriteLine();
        }
    }
}

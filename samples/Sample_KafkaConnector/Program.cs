using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NPipeline.Connectors.Kafka.Metrics;
using NPipeline.Connectors.Kafka.Partitioning;
using NPipeline.Connectors.Kafka.Retry;
using NPipeline.Extensions.DependencyInjection;

namespace Sample_KafkaConnector;

/// <summary>
///     Entry point for Kafka Connector sample demonstrating message processing with Apache Kafka.
/// </summary>
public sealed class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: Kafka Connector ===");
        Console.WriteLine();

        try
        {
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((_, services) =>
                {
                    services.AddNPipeline(Assembly.GetExecutingAssembly());

                    services.AddSingleton(KafkaConnectorPipeline.CreateConfiguration());
                    services.AddSingleton<IKafkaMetrics, ConsoleKafkaMetrics>();
                    services.AddSingleton<IRetryStrategy>(KafkaConnectorPipeline.CreateRetryStrategy());

                    services.AddSingleton<IPartitionKeyProvider<SampleMessage>>(
                        PartitionKeyProvider.FromProperty<SampleMessage, string>(message => message.CustomerId));
                })
                .Build();

            Console.WriteLine("Registered NPipeline services and scanned assemblies for nodes.");
            Console.WriteLine();

            Console.WriteLine("Pipeline Description:");
            Console.WriteLine(KafkaConnectorPipeline.GetDescription());
            Console.WriteLine();

            Console.WriteLine("Starting pipeline execution...");
            Console.WriteLine("Press Ctrl+C to stop.");
            Console.WriteLine();

            await host.Services.RunPipelineAsync<KafkaConnectorPipeline>();

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

using System.Reflection;
using Microsoft.Extensions.Hosting;
using NPipeline.Connectors.RabbitMQ.Configuration;
using NPipeline.Connectors.RabbitMQ.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;

namespace Sample_RabbitMqConnector;

/// <summary>
///     Entry point for the RabbitMQ Connector sample demonstrating message processing with RabbitMQ.
/// </summary>
/// <remarks>
///     Prerequisites:
///     docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management-alpine
///     Then run:
///     dotnet run --project samples/Sample_RabbitMqConnector
///     Publish test messages using the RabbitMQ Management UI at http://localhost:15672 (guest/guest)
///     or via the rabbitmqadmin CLI.
/// </remarks>
public sealed class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: RabbitMQ Connector ===");
        Console.WriteLine();

        try
        {
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((_, services) =>
                {
                    // Register NPipeline core + scan for pipeline definitions
                    services.AddNPipeline(Assembly.GetExecutingAssembly());

                    // Register RabbitMQ connection
                    services.AddRabbitMq(o =>
                    {
                        o.HostName = "localhost";
                        o.Port = 5672;
                        o.UserName = "guest";
                        o.Password = "guest";
                        o.ClientProvidedName = "npipeline-sample";
                    });

                    // Source — consume from "orders" queue
                    services.AddRabbitMqSource<OrderEvent>(new RabbitMqSourceOptions
                    {
                        QueueName = "orders",
                        PrefetchCount = 50,
                        Topology = new RabbitMqTopologyOptions
                        {
                            AutoDeclare = true,
                            Durable = true,
                            QueueType = QueueType.Quorum,
                            Bindings =
                            [
                                new BindingOptions("orders-exchange", "order.created"),
                                new BindingOptions("orders-exchange", "order.updated"),
                            ],
                            ExchangeType = "topic",
                        },
                    });

                    // Sink — publish enriched orders to "enriched-orders-exchange"
                    services.AddRabbitMqSink<EnrichedOrder>(new RabbitMqSinkOptions
                    {
                        ExchangeName = "enriched-orders-exchange",
                        RoutingKey = "order.enriched",
                        Persistent = true,
                        Topology = new RabbitMqTopologyOptions
                        {
                            AutoDeclare = true,
                            Durable = true,
                            ExchangeType = "topic",
                        },
                    });
                })
                .Build();

            Console.WriteLine("Registered NPipeline services and scanned assemblies for nodes.");
            Console.WriteLine();
            Console.WriteLine("Pipeline: RabbitMQ Source (orders queue) -> Order Enricher -> RabbitMQ Sink (enriched-orders)");
            Console.WriteLine();
            Console.WriteLine("Starting pipeline execution...");
            Console.WriteLine("Press Ctrl+C to stop.");
            Console.WriteLine();

            await host.Services.RunPipelineAsync<RabbitMqConnectorPipeline>();

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

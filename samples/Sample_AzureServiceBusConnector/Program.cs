using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Azure.ServiceBus.Nodes;
using NPipeline.Extensions.DependencyInjection;
using Sample_AzureServiceBusConnector;

Console.WriteLine("=== NPipeline Sample: Azure Service Bus Connector for Order Processing ===");
Console.WriteLine();

try
{
    var host = Host.CreateDefaultBuilder(args)
        .ConfigureServices((_, services) =>
        {
            // Register NPipeline and scan for pipeline definitions
            services.AddNPipeline(Assembly.GetExecutingAssembly());

            // Register source and sink configurations so nodes can be constructed via DI
            services.AddSingleton(ServiceBusConnectorPipeline.CreateSourceConfiguration());

            // Register source and sink nodes explicitly, referencing configurations
            services.AddTransient(_ =>
                new ServiceBusQueueSourceNode<Order>(
                    ServiceBusConnectorPipeline.CreateSourceConfiguration()));

            services.AddTransient(_ =>
                new ServiceBusQueueSinkNode<IAcknowledgableMessage<ProcessedOrder>>(
                    ServiceBusConnectorPipeline.CreateSinkConfiguration()));
        })
        .Build();

    Console.WriteLine("Pipeline Description:");
    Console.WriteLine(ServiceBusConnectorPipeline.GetDescription());
    Console.WriteLine();

    Console.WriteLine(
        "NOTE: This sample requires an Azure Service Bus namespace." +
        " Set SERVICEBUS_CONNECTION_STRING to your connection string, " +
        "then create 'input-orders' and 'processed-orders' queues.");

    Console.WriteLine();
    Console.WriteLine("Starting pipeline execution... Press Ctrl+C to stop.");

    await host.Services.RunPipelineAsync<ServiceBusConnectorPipeline>();

    Console.WriteLine("Pipeline execution completed.");
}
catch (Exception ex)
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.WriteLine($"Error: {ex.Message}");
    Console.ResetColor();
    Console.WriteLine(ex.ToString());
    Environment.ExitCode = 1;
}

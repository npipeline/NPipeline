using Azure.Messaging.ServiceBus;
using NPipeline.Configuration;
using NPipeline.Execution;
using NPipeline.Pipeline;
using Sample_AzureServiceBusConnector;

Console.WriteLine("=== NPipeline Sample: Azure Service Bus Connector for Order Processing ===");
Console.WriteLine();

var connectionString = Environment.GetEnvironmentVariable("SERVICEBUS_CONNECTION_STRING");

if (string.IsNullOrWhiteSpace(connectionString))
{
    Console.WriteLine("Set SERVICEBUS_CONNECTION_STRING to your namespace's connection string, and create the " +
                      $"'{ServiceBusConnectorPipeline.InputQueue}' and '{ServiceBusConnectorPipeline.OutputQueue}' queues.");

    Environment.ExitCode = 1;
    return;
}

Console.WriteLine(ServiceBusConnectorPipeline.GetDescription());
Console.WriteLine();
Console.WriteLine("Press Ctrl+C to stop.");
Console.WriteLine();

using var cts = new CancellationTokenSource();

Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

// One client, shared by the source and the sink.
await using var client = new ServiceBusClient(connectionString);

try
{
    await using var context = new PipelineContext(PipelineContextConfiguration.WithCancellation(cts.Token));
    await PipelineRunner.Create().RunAsync(new ServiceBusConnectorPipeline(client), context, cts.Token);
}
catch (Exception) when (cts.IsCancellationRequested)
{
    // Ctrl+C: messages that were not completed are abandoned and delivered again.
}
catch (Exception ex)
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.WriteLine($"Error: {ex.Message}");
    Console.ResetColor();
    Console.WriteLine(ex);
    Environment.ExitCode = 1;
    return;
}

Console.WriteLine();
Console.WriteLine("Pipeline stopped.");

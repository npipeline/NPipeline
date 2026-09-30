using NPipeline.Configuration;
using NPipeline.Connectors.RabbitMQ;
using NPipeline.Connectors.RabbitMQ.Configuration;
using NPipeline.Connectors.RabbitMQ.Connection;
using NPipeline.Execution;
using NPipeline.Pipeline;
using RabbitMQ.Client;

namespace Sample_RabbitMqConnector;

/// <summary>
///     Runs the RabbitMQ connector sample until Ctrl+C. Start RabbitMQ first with <c>docker compose up -d</c>.
/// </summary>
public static class Program
{
    public static async Task Main()
    {
        Console.WriteLine("=== NPipeline Sample: RabbitMQ Connector ===");
        Console.WriteLine();
        Console.WriteLine("Pipeline: orders queue -> OrderEnricher -> enriched-orders-exchange (order.enriched)");
        Console.WriteLine("Management UI: http://localhost:15672 (guest/guest). Press Ctrl+C to stop.");
        Console.WriteLine();

        using var cts = new CancellationTokenSource();

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        // One connection, shared by the source, the sink and the dead-letter sink.
        await using var connection = RabbitMqConnector.Connect(new RabbitMqConnectionOptions
        {
            HostName = "localhost",
            UserName = "guest",
            Password = "guest",
            ClientProvidedName = "npipeline-sample",
        });

        try
        {
            await DeclareOutputQueuesAsync(connection, cts.Token);

            await using var context = new PipelineContext(PipelineContextConfiguration.WithCancellation(cts.Token));
            await PipelineRunner.Create().RunAsync(new RabbitMqConnectorPipeline(connection), context, cts.Token);
        }
        catch (Exception) when (cts.IsCancellationRequested)
        {
            // Ctrl+C: orders that were not acknowledged go back on the queue.
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error executing pipeline: {ex}");
            Environment.ExitCode = 1;
            return;
        }

        Console.WriteLine();
        Console.WriteLine("Pipeline stopped.");
    }

    /// <summary>
    ///     Declares queues for the enriched orders and the dead letters, so the sample's output can be inspected. The
    ///     source declares its own queue through its topology options.
    /// </summary>
    private static async Task DeclareOutputQueuesAsync(IRabbitMqConnectionManager connection, CancellationToken cancellationToken)
    {
        await using var channel = await connection.CreateChannelAsync(cancellationToken);

        await channel.ExchangeDeclareAsync(RabbitMqConnectorPipeline.EnrichedExchange, ExchangeType.Topic, durable: true, cancellationToken: cancellationToken);
        await channel.QueueDeclareAsync(RabbitMqConnectorPipeline.EnrichedQueue, durable: true, exclusive: false, autoDelete: false, cancellationToken: cancellationToken);

        await channel.QueueBindAsync(RabbitMqConnectorPipeline.EnrichedQueue, RabbitMqConnectorPipeline.EnrichedExchange, RabbitMqConnectorPipeline.EnrichedRoutingKey,
            cancellationToken: cancellationToken);

        await channel.QueueDeclareAsync(RabbitMqConnectorPipeline.DeadLetterQueue, durable: true, exclusive: false, autoDelete: false, cancellationToken: cancellationToken);
    }
}

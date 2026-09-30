using Confluent.Kafka;
using NPipeline.Configuration;
using NPipeline.Execution;
using NPipeline.Pipeline;

namespace Sample_KafkaConnector;

/// <summary>
///     Runs the Kafka connector sample until Ctrl+C. Pass <c>--exactly-once</c> to write through a transactional sink.
/// </summary>
public static class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: Kafka Connector ===");
        Console.WriteLine();

        var exactlyOnce = args.Contains("--exactly-once", StringComparer.OrdinalIgnoreCase);

        Console.WriteLine(KafkaConnectorPipeline.GetDescription(exactlyOnce));
        Console.WriteLine();
        Console.WriteLine("Press Ctrl+C to stop.");
        Console.WriteLine();

        using var cts = new CancellationTokenSource();

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        using var deadLetterProducer = new ProducerBuilder<byte[]?, byte[]>(
            new ProducerConfig { BootstrapServers = KafkaConnectorPipeline.BootstrapServers }).Build();

        try
        {
            await using var context = new PipelineContext(PipelineContextConfiguration.WithCancellation(cts.Token));
            await PipelineRunner.Create().RunAsync(new KafkaConnectorPipeline(deadLetterProducer, exactlyOnce), context, cts.Token);
        }
        catch (Exception) when (cts.IsCancellationRequested)
        {
            // Ctrl+C: acknowledged offsets are committed as the source closes; the rest are read again next run.
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
}

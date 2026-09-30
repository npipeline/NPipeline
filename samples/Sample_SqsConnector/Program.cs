using NPipeline.Configuration;
using NPipeline.Execution;
using NPipeline.Pipeline;

namespace Sample_SqsConnector;

/// <summary>Runs the SQS connector sample until Ctrl+C.</summary>
public static class Program
{
    public static async Task Main()
    {
        Console.WriteLine("=== NPipeline Sample: SQS Connector for Order Processing ===");
        Console.WriteLine();

        var settings = SqsSampleSettings.FromEnvironment();

        Console.WriteLine(SqsConnectorPipeline.GetDescription(settings));
        Console.WriteLine();
        Console.WriteLine("Press Ctrl+C to stop.");
        Console.WriteLine();

        using var cts = new CancellationTokenSource();

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        // One client, shared by the source and the sink. Credentials come from the SDK's default chain.
        using var client = settings.CreateClient();

        try
        {
            await using var context = new PipelineContext(PipelineContextConfiguration.WithCancellation(cts.Token));
            await PipelineRunner.Create().RunAsync(new SqsConnectorPipeline(settings, client), context, cts.Token);
        }
        catch (Exception) when (cts.IsCancellationRequested)
        {
            // Ctrl+C: messages that were not acknowledged become visible again after their visibility timeout.
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Error executing pipeline: {ex.Message}");
            Console.ResetColor();
            Console.WriteLine(ex);
            Environment.ExitCode = 1;
            return;
        }

        Console.WriteLine();
        Console.WriteLine("Pipeline stopped.");
    }
}

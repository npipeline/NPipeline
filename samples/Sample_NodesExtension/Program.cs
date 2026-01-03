using NPipeline.Execution;

namespace Sample_NodesExtension;

/// <summary>
///     Entry point for the Nodes Extension sample demonstrating data cleaning and validation.
/// </summary>
public sealed class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("╔════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║  NPipeline Sample: Nodes Extension - Customer Data Processing   ║");
        Console.WriteLine("╚════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();

        try
        {
            // Display what this sample demonstrates
            Console.WriteLine(CustomerProcessingPipeline.GetDescription());
            Console.WriteLine();

            // Execute the pipeline using PipelineRunner
            Console.WriteLine("Starting pipeline execution...");
            Console.WriteLine("─".PadRight(70, '─'));
            Console.WriteLine();

            var runner = PipelineRunner.Create();
            await runner.RunAsync<CustomerProcessingPipeline>();

            Console.WriteLine();
            Console.WriteLine("─".PadRight(70, '─'));
            Console.WriteLine();

            Console.WriteLine("Pipeline execution completed successfully!");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error executing pipeline: {ex.Message}");
            Console.WriteLine();
            Console.WriteLine("Stack trace:");
            Console.WriteLine(ex);
            Environment.ExitCode = 1;
        }
    }
}

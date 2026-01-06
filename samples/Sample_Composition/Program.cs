using NPipeline.Execution;
using NPipeline.Pipeline;

namespace Sample_Composition;

/// <summary>
///     Program demonstrating NPipeline.Extensions.Composition usage.
/// </summary>
internal sealed class Program
{
    private static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline.Extensions.Composition Sample ===");
        Console.WriteLine();

        // Create pipeline runner
        var runner = PipelineRunner.Create();

        // Create pipeline context
        var context = PipelineContext.Default;

        try
        {
            Console.WriteLine("Running composition pipeline...");
            Console.WriteLine();

            // Run the composition pipeline
            await runner.RunAsync<CompositionPipeline>(context);

            Console.WriteLine();
            Console.WriteLine("Pipeline execution completed successfully!");
        }
        catch (Exception ex)
        {
            Console.WriteLine();
            Console.WriteLine($"Pipeline execution failed: {ex.Message}");
            Console.WriteLine($"Exception type: {ex.GetType().Name}");

            if (ex.InnerException is not null)
                Console.WriteLine($"Inner exception: {ex.InnerException.Message}");

            Environment.Exit(1);
        }
    }
}

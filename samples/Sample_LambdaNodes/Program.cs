using NPipeline.Execution;
using NPipeline.Pipeline;

namespace Sample_LambdaNodes;

/// <summary>
///     Demonstrates the main entry point and running various lambda-based pipelines.
/// </summary>
public static class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("╔═══════════════════════════════════════════════════════╗");
        Console.WriteLine("║        NPipeline Lambda Nodes Sample                  ║");
        Console.WriteLine("║                                                       ║");
        Console.WriteLine("║   Demonstrates simplified pipeline creation using     ║");
        Console.WriteLine("║   lambda functions instead of separate classes        ║");
        Console.WriteLine("╚═══════════════════════════════════════════════════════╝");
        Console.WriteLine();

        var runner = PipelineRunner.Create();
        var context = PipelineContext.Default;

        // Example 1: Simple synchronous pipeline
        Console.WriteLine("\n📌 Example 1: Simple Synchronous Pipeline");
        Console.WriteLine("─────────────────────────────────────────────");
        Console.WriteLine("Generate numbers → Double them → Add 100 → Print");
        Console.WriteLine();

        try
        {
            await runner.RunAsync<SimpleLambdaPipeline>(context);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Error: {ex.Message}");
        }

        // Example 2: Hybrid approach with extracted functions
        Console.WriteLine("\n\n📌 Example 2: Hybrid Approach (Extracted Functions)");
        Console.WriteLine("─────────────────────────────────────────────");
        Console.WriteLine("Process prices → Apply discount → Format → Display");
        Console.WriteLine();

        try
        {
            await runner.RunAsync<HybridApproachPipeline>(context);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Error: {ex.Message}");
        }

        // Example 4: Error handling
        Console.WriteLine("\n\n📌 Example 4: Error Handling");
        Console.WriteLine("─────────────────────────────────────────────");
        Console.WriteLine("Parse integers with fallback → Filter → Display");
        Console.WriteLine();

        try
        {
            await runner.RunAsync<ErrorHandlingPipeline>(context);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Error: {ex.Message}");
        }

        // Example 5: Complex transformations
        Console.WriteLine("\n\n📌 Example 5: Complex Object Transformations");
        Console.WriteLine("─────────────────────────────────────────────");
        Console.WriteLine("Product data → Apply discount → Display sales");
        Console.WriteLine();

        try
        {
            await runner.RunAsync<ComplexTransformationPipeline>(context);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Error: {ex.Message}");
        }

        Console.WriteLine("\n\n✅ All examples completed!");
        Console.WriteLine();
        Console.WriteLine("Key Takeaways:");
        Console.WriteLine("─────────────");
        Console.WriteLine("✓ Lambda nodes reduce boilerplate for simple operations");
        Console.WriteLine("✓ Both sync and async variants are supported");
        Console.WriteLine("✓ Extract logic into functions for better testability");
        Console.WriteLine("✓ Ideal for prototyping and quick development");
        Console.WriteLine("✓ Use class-based nodes for complex, stateful operations");
    }
}

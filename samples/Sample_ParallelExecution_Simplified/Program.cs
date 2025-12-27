using NPipeline.Pipeline;

Console.WriteLine("╔════════════════════════════════════════════════════════╗");
Console.WriteLine("║  NPipeline Simplified Parallel Execution Demo         ║");
Console.WriteLine("║  Comparing Manual and Simplified Approaches           ║");
Console.WriteLine("╚════════════════════════════════════════════════════════╝\n");

// Parse command line arguments to select variant
var variant = args.Length > 0 && args[0].Equals("builder", StringComparison.OrdinalIgnoreCase)
    ? DemoVariant.Builder
    : args.Length > 0 && args[0].Equals("preset", StringComparison.OrdinalIgnoreCase)
        ? DemoVariant.Preset
        : DemoVariant.Manual;

Console.WriteLine($"Running variant: {variant}\n");

// Create demo data
var ctx = PipelineContext.Default;
var builder = new PipelineBuilder();

Console.WriteLine("Demonstration of three ways to configure parallelism:\n");

// Show code examples for each approach
Console.WriteLine("MANUAL CONFIGURATION API (Verbose):");
Console.WriteLine("─────────────────────────────────────────────");
Console.WriteLine("builder");
Console.WriteLine("    .AddTransform<MyTransform, Input, Output>()");
Console.WriteLine("    .WithBlockingParallelism(");
Console.WriteLine("        builder,");
Console.WriteLine("        maxDegreeOfParallelism: Environment.ProcessorCount * 4,");
Console.WriteLine("        maxQueueLength: Environment.ProcessorCount * 8,");
Console.WriteLine("        outputBufferCapacity: Environment.ProcessorCount * 16)");
Console.WriteLine("    .AddSink<MySink>();");
Console.WriteLine("\n");

Console.WriteLine("PRESET API (Simple):");
Console.WriteLine("─────────────────────────────────────────────");
Console.WriteLine("builder");
Console.WriteLine("    .AddTransform<MyTransform, Input, Output>()");
Console.WriteLine("    .RunParallel(builder, ParallelWorkloadType.IoBound)");
Console.WriteLine("    .AddSink<MySink>();");
Console.WriteLine("\n");

Console.WriteLine("BUILDER API (Flexible):");
Console.WriteLine("─────────────────────────────────────────────");
Console.WriteLine("builder");
Console.WriteLine("    .AddTransform<MyTransform, Input, Output>()");
Console.WriteLine("    .RunParallel(builder, opt => opt");
Console.WriteLine("        .MaxDegreeOfParallelism(8)");
Console.WriteLine("        .DropOldestOnBackpressure())");
Console.WriteLine("    .AddSink<MySink>();");
Console.WriteLine("\n");

// Show workload type presets
Console.WriteLine("WORKLOAD TYPE PRESETS:");
Console.WriteLine("─────────────────────────────────────────────");
Console.WriteLine($"General:      DOP={Environment.ProcessorCount * 2}, Queue={Environment.ProcessorCount * 4}");
Console.WriteLine($"CpuBound:     DOP={Environment.ProcessorCount}, Queue={Environment.ProcessorCount * 2}");
Console.WriteLine($"IoBound:      DOP={Environment.ProcessorCount * 4}, Queue={Environment.ProcessorCount * 8}");
Console.WriteLine($"NetworkBound: DOP={Math.Min(Environment.ProcessorCount * 8, 100)}, Queue=200");
Console.WriteLine("\n");

Console.WriteLine("KEY BENEFITS:");
Console.WriteLine("─────────────────────────────────────────────");
Console.WriteLine("✓ Simplified API reduces boilerplate code");
Console.WriteLine("✓ Workload types encode best practices");
Console.WriteLine("✓ Builder API provides fine-grained control");
Console.WriteLine("✓ Backward compatible with old manual API");
Console.WriteLine("✓ Type-safe and compile-time validated");
Console.WriteLine("\n");

Console.WriteLine("NEXT STEPS:");
Console.WriteLine("─────────────────────────────────────────────");
Console.WriteLine("- Check Sample_ParallelExecution_Simplified/README.md for full examples");
Console.WriteLine("- See docs/extensions/parallelism.md for comprehensive documentation");
Console.WriteLine("- Run tests: dotnet test tests/NPipeline.Extensions.Parallelism.Tests.csproj");
Console.WriteLine("\n");

internal enum DemoVariant
{
    Manual,
    Preset,
    Builder,
}

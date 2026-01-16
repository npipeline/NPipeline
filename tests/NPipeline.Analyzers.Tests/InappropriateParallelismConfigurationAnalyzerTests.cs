using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers.Tests;

/// <summary>
///     Tests for InappropriateParallelismConfigurationAnalyzer.
/// </summary>
public sealed class InappropriateParallelismConfigurationAnalyzerTests
{
    [Fact]
    public void ShouldDetectExcessiveParallelismInParallelExecutionStrategy()
    {
        var code = """
                   using NPipeline.Extensions.Parallelism;

                   public class TestTransform
                   {
                       public void Configure()
                       {
                           // NP9003: Excessive parallelism
                           var strategy = new ParallelExecutionStrategy(degreeOfParallelism: 16);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InappropriateParallelismConfigurationAnalyzer.InappropriateParallelismConfigurationId);
        Assert.True(hasDiagnostic, "Analyzer should detect excessive parallelism");
    }

    [Fact]
    public void ShouldDetectHighParallelismForIoBoundWorkload()
    {
        var code = """
                   using NPipeline.Extensions.Parallelism;

                   public class IoBoundTransform
                   {
                       public void Configure()
                       {
                           // NP9003: High parallelism for I/O-bound workload
                           var strategy = new ParallelExecutionStrategy(degreeOfParallelism: 8);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InappropriateParallelismConfigurationAnalyzer.InappropriateParallelismConfigurationId);
        Assert.True(hasDiagnostic, "Analyzer should detect high parallelism for I/O-bound workload");
    }

    [Fact]
    public void ShouldDetectLowParallelismForCpuBoundWorkload()
    {
        var code = """
                   using NPipeline.Extensions.Parallelism;

                   public class CpuBoundTransform
                   {
                       public void Configure()
                       {
                           // NP9003: Low parallelism for CPU-bound workload
                           var strategy = new ParallelExecutionStrategy(degreeOfParallelism: 1);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InappropriateParallelismConfigurationAnalyzer.InappropriateParallelismConfigurationId);
        Assert.True(hasDiagnostic, "Analyzer should detect low parallelism for CPU-bound workload");
    }

    [Fact]
    public void ShouldDetectSingleThreadedCpuBoundWorkload()
    {
        var code = """
                   using NPipeline.Extensions.Parallelism;

                   public class CpuIntensiveTransform
                   {
                       public void Configure()
                       {
                           // NP9003: Single-threaded execution for CPU-bound workload
                           var strategy = new ParallelExecutionStrategy(degreeOfParallelism: 1);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InappropriateParallelismConfigurationAnalyzer.InappropriateParallelismConfigurationId);
        Assert.True(hasDiagnostic, "Analyzer should detect single-threaded CPU-bound workload");
    }

    [Fact]
    public void ShouldDetectPreserveOrderingWithHighParallelism()
    {
        var code = """
                   using NPipeline.Extensions.Parallelism;

                   public class TestTransform
                   {
                       public void Configure()
                       {
                           // NP9003: PreserveOrdering enabled with high parallelism
                           var options = new ParallelOptions(maxDegreeOfParallelism: 8, preserveOrdering: true);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InappropriateParallelismConfigurationAnalyzer.InappropriateParallelismConfigurationId);
        Assert.True(hasDiagnostic, "Analyzer should detect PreserveOrdering with high parallelism");
    }

    [Fact]
    public void ShouldDetectExcessiveParallelismInWithParallelismMethod()
    {
        var code = """
                   using NPipeline.Extensions.Parallelism;
                   using NPipeline.Pipeline;

                   public class TestPipeline : IPipelineDefinition
                   {
                       public void Define(PipelineBuilder builder, PipelineContext context)
                       {
                           // NP9003: Excessive parallelism
                           var transform = builder.AddTransform<TestTransform, int, string>("test")
                               .WithBlockingParallelism(builder, 16);
                       }
                   }

                   public class TestTransform { }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InappropriateParallelismConfigurationAnalyzer.InappropriateParallelismConfigurationId);
        Assert.True(hasDiagnostic, "Analyzer should detect excessive parallelism in WithParallelism method");
    }

    [Fact]
    public void ShouldIgnoreAppropriateParallelism()
    {
        var code = """
                   using NPipeline.Extensions.Parallelism;

                   public class CpuBoundTransform
                   {
                       public void Configure()
                       {
                           // Appropriate parallelism for CPU-bound workload
                           var strategy = new ParallelExecutionStrategy(degreeOfParallelism: 4);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InappropriateParallelismConfigurationAnalyzer.InappropriateParallelismConfigurationId);
        Assert.False(hasDiagnostic, "Analyzer should not flag appropriate parallelism");
    }

    [Fact]
    public void ShouldIgnoreAppropriateIoBoundParallelism()
    {
        var code = """
                   using NPipeline.Extensions.Parallelism;

                   public class IoBoundTransform
                   {
                       public void Configure()
                       {
                           // Appropriate parallelism for I/O-bound workload
                           var strategy = new ParallelExecutionStrategy(degreeOfParallelism: 4);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InappropriateParallelismConfigurationAnalyzer.InappropriateParallelismConfigurationId);
        Assert.False(hasDiagnostic, "Analyzer should not flag appropriate I/O-bound parallelism");
    }

    [Fact]
    public void ShouldIgnorePreserveOrderingWithLowParallelism()
    {
        var code = """
                   using NPipeline.Extensions.Parallelism;

                   public class TestTransform
                   {
                       public void Configure()
                       {
                           // PreserveOrdering with low parallelism is acceptable
                           var options = new ParallelOptions(maxDegreeOfParallelism: 2, preserveOrdering: true);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InappropriateParallelismConfigurationAnalyzer.InappropriateParallelismConfigurationId);
        Assert.False(hasDiagnostic, "Analyzer should not flag PreserveOrdering with low parallelism");
    }

    private static IEnumerable<Diagnostic> GetDiagnostics(string code)
    {
        var syntaxTree = CSharpSyntaxTree.ParseText(code);

        // Get path to NPipeline assembly
        var nPipelineAssemblyPath = typeof(object).Assembly.Location;

        var references = new[]
        {
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Enumerable).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Task).Assembly.Location),
            MetadataReference.CreateFromFile(nPipelineAssemblyPath),
        };

        var compilation = CSharpCompilation.Create("TestAssembly")
            .AddReferences(references)
            .AddSyntaxTrees(syntaxTree);

        var analyzer = new InappropriateParallelismConfigurationAnalyzer();
        var compilationWithAnalyzers = compilation.WithAnalyzers([analyzer]);
        var diagnostics = compilationWithAnalyzers.GetAnalyzerDiagnosticsAsync().Result;

        return diagnostics;
    }
}

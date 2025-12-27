using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using NPipeline.Nodes;

namespace NPipeline.Analyzers.Tests;

/// <summary>
///     Tests for the NodeParameterlessConstructorAnalyzer.
/// </summary>
public sealed class NodeParameterlessConstructorAnalyzerTests
{
    [Fact]
    public void ShouldDetectNodeWithoutParameterlessConstructor()
    {
        var code = """
                   using System.Threading.Tasks;
                   using NPipeline.Pipeline;
                   using NPipeline.Nodes;

                   public class TestNode : TransformNode<string, int>
                   {
                       private readonly string _dependency;

                       public TestNode(string dependency)
                       {
                           _dependency = dependency;
                       }

                       public override Task<int> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           return Task.FromResult(item.Length);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        Assert.True(diagnostics.Any(d => d.Id == NodeParameterlessConstructorAnalyzer.MissingParameterlessConstructorId),
            "Analyzer should detect node without parameterless constructor");
    }

    [Fact]
    public void ShouldNotReportForNodeWithParameterlessConstructor()
    {
        var code = """
                   using System.Threading.Tasks;
                   using NPipeline.Pipeline;
                   using NPipeline.Nodes;

                   public class TestNode : TransformNode<string, int>
                   {
                       public TestNode()
                       {
                       }

                       public override Task<int> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           return Task.FromResult(item.Length);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        Assert.False(diagnostics.Any(d => d.Id == NodeParameterlessConstructorAnalyzer.MissingParameterlessConstructorId),
            "Analyzer should not report for node with explicit parameterless constructor");
    }

    [Fact]
    public void ShouldNotReportForImplicitConstructor()
    {
        var code = """
                   using System.Threading.Tasks;
                   using NPipeline.Pipeline;
                   using NPipeline.Nodes;

                   public class TestNode : TransformNode<string, int>
                   {
                       public override Task<int> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           return Task.FromResult(item.Length);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        Assert.False(diagnostics.Any(),
            "Analyzer should not report for nodes with implicit parameterless constructor");
    }

    [Fact]
    public void ShouldDetectSourceNodeWithoutParameterlessConstructor()
    {
        var code = """
                   using System.Threading.Tasks;
                   using System.Collections.Generic;
                   using NPipeline.Pipeline;
                   using NPipeline.Nodes;
                   using NPipeline.DataFlow;

                   public class TestSourceNode : SourceNode<int>
                   {
                       private readonly int _count;

                       public TestSourceNode(int count)
                       {
                           _count = count;
                       }

                       public override IDataPipe<int> Initialize(PipelineContext context, CancellationToken cancellationToken)
                       {
                           async IAsyncEnumerable<int> Generate()
                           {
                               for (int i = 0; i < _count; i++)
                                   yield return i;
                           }
                           return new StreamingDataPipe<int>(Generate(), "test");
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        Assert.True(diagnostics.Any(d => d.Id == NodeParameterlessConstructorAnalyzer.MissingParameterlessConstructorId),
            "Analyzer should detect source node without parameterless constructor");
    }

    [Fact]
    public void ShouldDetectSinkNodeWithoutParameterlessConstructor()
    {
        var code = """
                   using System.Threading.Tasks;
                   using System.Collections.Generic;
                   using NPipeline.Pipeline;
                   using NPipeline.Nodes;
                   using NPipeline.DataFlow;

                   public class TestSinkNode : SinkNode<int>
                   {
                       private readonly ILogger _logger;

                       public TestSinkNode(ILogger logger)
                       {
                           _logger = logger;
                       }

                       public override async Task ExecuteAsync(IDataPipe<int> input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           await foreach (var item in input.WithCancellation(cancellationToken))
                           {
                               _logger.Log(item.ToString());
                           }
                       }
                   }

                   public interface ILogger { void Log(string message); }
                   """;

        var diagnostics = GetDiagnostics(code);

        Assert.True(diagnostics.Any(d => d.Id == NodeParameterlessConstructorAnalyzer.MissingParameterlessConstructorId),
            "Analyzer should detect sink node without parameterless constructor");
    }

    [Fact]
    public void ShouldNotReportForAbstractNode()
    {
        var code = """
                   using System.Threading.Tasks;
                   using NPipeline.Pipeline;
                   using NPipeline.Nodes;

                   public abstract class BaseNode : TransformNode<string, int>
                   {
                       protected BaseNode(string dependency)
                       {
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        Assert.False(diagnostics.Any(d => d.Id == NodeParameterlessConstructorAnalyzer.MissingParameterlessConstructorId),
            "Analyzer should not report for abstract nodes");
    }

    [Fact]
    public void ShouldNotReportForNonNodeClass()
    {
        var code = """
                   public class RegularClass
                   {
                       private readonly string _dependency;

                       public RegularClass(string dependency)
                       {
                           _dependency = dependency;
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        Assert.False(diagnostics.Any(d => d.Id == NodeParameterlessConstructorAnalyzer.MissingParameterlessConstructorId),
            "Analyzer should not report for non-node classes");
    }

    [Fact]
    public void ShouldNotReportWhenBothConstructorsExist()
    {
        var code = """
                   using System.Threading.Tasks;
                   using NPipeline.Pipeline;
                   using NPipeline.Nodes;

                   public class TestNode : TransformNode<string, int>
                   {
                       private readonly string? _dependency;

                       public TestNode()
                       {
                       }

                       public TestNode(string dependency)
                       {
                           _dependency = dependency;
                       }

                       public override Task<int> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           return Task.FromResult(item.Length);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        Assert.False(diagnostics.Any(d => d.Id == NodeParameterlessConstructorAnalyzer.MissingParameterlessConstructorId),
            "Analyzer should not report when both parameterless and parameterized constructors exist");
    }

    private static ImmutableArray<Diagnostic> GetDiagnostics(string sourceCode)
    {
        var syntaxTree = CSharpSyntaxTree.ParseText(sourceCode);

        // Get NPipeline assembly references
        var npipelineAssembly = typeof(TransformNode<,>).Assembly;

        var references = new[]
        {
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Task).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(IAsyncEnumerable<>).Assembly.Location),
            MetadataReference.CreateFromFile(npipelineAssembly.Location),
        };

        // Add System.Runtime reference for proper compilation
        var runtimeAssembly = AppDomain.CurrentDomain.GetAssemblies()
            .FirstOrDefault(a => a.GetName().Name == "System.Runtime");

        if (runtimeAssembly != null)
            references = [.. references, MetadataReference.CreateFromFile(runtimeAssembly.Location)];

        var compilation = CSharpCompilation.Create(
            "TestAssembly",
            [syntaxTree],
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        var analyzer = new NodeParameterlessConstructorAnalyzer();

        var compilationWithAnalyzers = compilation.WithAnalyzers(
            ImmutableArray.Create<DiagnosticAnalyzer>(analyzer));

        var diagnostics = compilationWithAnalyzers.GetAllDiagnosticsAsync().Result;

        return diagnostics
            .Where(d => d.Id is NodeParameterlessConstructorAnalyzer.MissingParameterlessConstructorId
                or NodeParameterlessConstructorAnalyzer.PerformanceSuggestionId)
            .ToImmutableArray();
    }
}

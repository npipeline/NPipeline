using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Analyzers.Tests;

/// <summary>
///     Tests for SourceNodeStreamingAnalyzer.
/// </summary>
public sealed class SourceNodeStreamingAnalyzerTests
{
    [Fact]
    public void ShouldDetectListAllocationInSourceNode()
    {
        var code = """
                   using System.Collections.Generic;
                   using NPipeline.DataFlow;
                   using NPipeline.Nodes;
                   using NPipeline.Pipeline;

                   public class TestSourceNode : SourceNode<string>
                   {
                       public override IDataPipe<string> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
                       {
                           var items = new List<string>(); // Should trigger diagnostic
                           items.Add("test");
                           return new InMemoryDataPipe<string>(items);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == SourceNodeStreamingAnalyzer.SourceNodeStreamingId);
        Assert.True(hasDiagnostic, "Analyzer should detect List<T> allocation in SourceNode");
    }

    [Fact]
    public void ShouldDetectArrayAllocationInSourceNode()
    {
        var code = """
                   using NPipeline.DataFlow;
                   using NPipeline.Nodes;
                   using NPipeline.Pipeline;

                   public class TestSourceNode : SourceNode<int>
                   {
                       public override IDataPipe<int> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
                       {
                           var items = new int[10]; // Should trigger diagnostic
                           for (int i = 0; i < 10; i++)
                           {
                               items[i] = i;
                           }
                           return new InMemoryDataPipe<int>(items);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == SourceNodeStreamingAnalyzer.SourceNodeStreamingId);
        Assert.True(hasDiagnostic, "Analyzer should detect array allocation in SourceNode");
    }

    [Fact]
    public void ShouldDetectToAsyncEnumerableOnCollections()
    {
        var code = """
                   using System.Collections.Generic;
                   using System.Linq;
                   using NPipeline.DataFlow;
                   using NPipeline.Nodes;
                   using NPipeline.Pipeline;

                   public class TestSourceNode : SourceNode<string>
                   {
                       public override IDataPipe<string> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
                       {
                           var items = new List<string> { "test1", "test2" };
                           return new StreamingDataPipe<string>(items.ToAsyncEnumerable()); // Should trigger diagnostic
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == SourceNodeStreamingAnalyzer.SourceNodeStreamingId);
        Assert.True(hasDiagnostic, "Analyzer should detect .ToAsyncEnumerable() on materialized collections");
    }

    [Fact]
    public void ShouldDetectSynchronousFileIOInSourceNode()
    {
        var code = """
                   using System.IO;
                   using NPipeline.DataFlow;
                   using NPipeline.Nodes;
                   using NPipeline.Pipeline;

                   public class TestSourceNode : SourceNode<string>
                   {
                       public override IDataPipe<string> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
                       {
                           var lines = File.ReadAllLines("test.txt"); // Should trigger diagnostic
                           return new InMemoryDataPipe<string>(lines);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == SourceNodeStreamingAnalyzer.SourceNodeStreamingId);
        Assert.True(hasDiagnostic, "Analyzer should detect synchronous file I/O in SourceNode");
    }

    [Fact]
    public void ShouldDetectToListCallInSourceNode()
    {
        var code = """
                   using System.Collections.Generic;
                   using System.Linq;
                   using NPipeline.DataFlow;
                   using NPipeline.Nodes;
                   using NPipeline.Pipeline;

                   public class TestSourceNode : SourceNode<int>
                   {
                       public override IDataPipe<int> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
                       {
                           var items = Enumerable.Range(1, 100).ToList(); // Should trigger diagnostic
                           return new InMemoryDataPipe<int>(items);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == SourceNodeStreamingAnalyzer.SourceNodeStreamingId);
        Assert.True(hasDiagnostic, "Analyzer should detect .ToList() call in SourceNode");
    }

    [Fact]
    public void ShouldDetectToArrayCallInSourceNode()
    {
        var code = """
                   using System.Linq;
                   using NPipeline.DataFlow;
                   using NPipeline.Nodes;
                   using NPipeline.Pipeline;

                   public class TestSourceNode : SourceNode<int>
                   {
                       public override IDataPipe<int> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
                       {
                           var items = Enumerable.Range(1, 100).ToArray(); // Should trigger diagnostic
                           return new InMemoryDataPipe<int>(items);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == SourceNodeStreamingAnalyzer.SourceNodeStreamingId);
        Assert.True(hasDiagnostic, "Analyzer should detect .ToArray() call in SourceNode");
    }

    [Fact]
    public void ShouldNotAnalyzeNonSourceNodeClasses()
    {
        var code = """
                   using System.Collections.Generic;
                   using NPipeline.DataFlow;

                   public class TestClass
                   {
                       public IDataPipe<string> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
                       {
                           var items = new List<string>(); // Should NOT trigger diagnostic (not a SourceNode)
                           return new InMemoryDataPipe<string>(items);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == SourceNodeStreamingAnalyzer.SourceNodeStreamingId);
        Assert.False(hasDiagnostic, "Analyzer should not analyze non-SourceNode classes");
    }

    [Fact]
    public void ShouldNotAnalyzeNonExecuteAsyncMethods()
    {
        var code = """
                   using System.Collections.Generic;
                   using NPipeline.DataFlow;
                   using NPipeline.Nodes;
                   using NPipeline.Pipeline;

                   public class TestSourceNode : SourceNode<string>
                   {
                       public IDataPipe<string> SomeOtherMethod(PipelineContext context, CancellationToken cancellationToken)
                       {
                           var items = new List<string>(); // Should NOT trigger diagnostic (not ExecuteAsync)
                           return new InMemoryDataPipe<string>(items);
                       }

                       public override IDataPipe<string> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
                       {
                           // Proper streaming implementation
                           return new StreamingDataPipe<string>(GetItemsAsync(cancellationToken));
                       }

                       private async IAsyncEnumerable<string> GetItemsAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
                       {
                           yield return "test1";
                           yield return "test2";
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == SourceNodeStreamingAnalyzer.SourceNodeStreamingId);
        Assert.False(hasDiagnostic, "Analyzer should not analyze non-ExecuteAsync methods");
    }

    [Fact]
    public void ShouldNotTriggerForStreamingImplementation()
    {
        var code = """
                   using NPipeline.DataFlow;
                   using NPipeline.Nodes;
                   using NPipeline.Pipeline;

                   public class TestSourceNode : SourceNode<string>
                   {
                       public override IDataPipe<string> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
                       {
                           // Proper streaming implementation - should NOT trigger diagnostic
                           return new StreamingDataPipe<string>(GetItemsAsync(cancellationToken));
                       }

                       private async IAsyncEnumerable<string> GetItemsAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
                       {
                           yield return "test1";
                           yield return "test2";
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == SourceNodeStreamingAnalyzer.SourceNodeStreamingId);
        Assert.False(hasDiagnostic, "Analyzer should not trigger for proper streaming implementations");
    }

    [Fact]
    public void ShouldNotTriggerForAsyncFileIO()
    {
        var code = """
                   using System.IO;
                   using System.Threading.Tasks;
                   using NPipeline.DataFlow;
                   using NPipeline.Nodes;
                   using NPipeline.Pipeline;

                   public class TestSourceNode : SourceNode<string>
                   {
                       public override async Task<IDataPipe<string>> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
                       {
                           var lines = await File.ReadAllLinesAsync("test.txt", cancellationToken); // Should NOT trigger diagnostic (async)
                           return new InMemoryDataPipe<string>(lines);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == SourceNodeStreamingAnalyzer.SourceNodeStreamingId);
        Assert.False(hasDiagnostic, "Analyzer should not trigger for async file I/O operations");
    }

    private static IEnumerable<Diagnostic> GetDiagnostics(string code)
    {
        var syntaxTree = CSharpSyntaxTree.ParseText(code);

        var references = new[]
        {
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Enumerable).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Task).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(File).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(IDataPipe<>).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(SourceNode<>).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(PipelineContext).Assembly.Location),
        };

        var compilation = CSharpCompilation.Create("TestAssembly")
            .AddReferences(references)
            .AddSyntaxTrees(syntaxTree);

        var analyzer = new SourceNodeStreamingAnalyzer();
        var compilation2 = compilation.WithAnalyzers(new[] { analyzer }.ToImmutableArray<DiagnosticAnalyzer>());
        var diagnostics = compilation2.GetAnalyzerDiagnosticsAsync().Result;

        return diagnostics;
    }
}

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using NPipeline.Nodes;

namespace NPipeline.Analyzers.Tests;

/// <summary>
///     Tests for AnonymousObjectAllocationAnalyzer.
/// </summary>
public sealed class AnonymousObjectAllocationAnalyzerTests
{
    [Fact]
    public void ShouldDetectAnonymousObjectInExecuteAsyncMethod()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public async Task<string> ExecuteAsync(string input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           // NP9203: Anonymous object in hot path
                           var result = new { Id = input, Processed = true, Timestamp = DateTime.UtcNow };
                           return result.Id;
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == AnonymousObjectAllocationAnalyzer.AnonymousObjectAllocationId);
        Assert.True(hasDiagnostic, "Analyzer should detect anonymous object in ExecuteAsync method");
    }

    [Fact]
    public void ShouldDetectAnonymousObjectInProcessAsyncMethod()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestNode : INode
                   {
                       public async Task ProcessAsync(IEnumerable<string> items)
                       {
                           foreach (var item in items)
                           {
                               // NP9203: Anonymous object in hot path
                               var result = new { Item = item, Processed = true };
                               await SaveAsync(result);
                           }
                       }
                       
                       private async Task SaveAsync(object obj) { /* ... */ }
                       public ValueTask DisposeAsync() => default;
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == AnonymousObjectAllocationAnalyzer.AnonymousObjectAllocationId);
        Assert.True(hasDiagnostic, "Analyzer should detect anonymous object in ProcessAsync method");
    }

    [Fact]
    public void ShouldDetectAnonymousObjectInRunAsyncMethod()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestSourceNode : ISourceNode<int>
                   {
                       public async Task<IDataPipe<int>> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
                       {
                           var numbers = Enumerable.Range(1, 100);
                           // NP9203: Anonymous object in hot path
                           var enriched = numbers.Select(x => new { Value = x, IsEven = x % 2 == 0 }).ToList();
                           return new InMemoryDataPipe<int>(enriched.Select(e => e.Value));
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == AnonymousObjectAllocationAnalyzer.AnonymousObjectAllocationId);
        Assert.True(hasDiagnostic, "Analyzer should detect anonymous object in RunAsync method");
    }

    [Fact]
    public void ShouldDetectAnonymousObjectInAsyncMethod()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestClass
                   {
                       public async Task HandleDataAsync(IEnumerable<string> data)
                       {
                           // NP9203: Anonymous object in async method (hot path)
                           var result = data.Select(x => new { Original = x, Length = x.Length }).ToList();
                           await ProcessAsync(result);
                       }
                       
                       private async Task ProcessAsync(object obj) { /* ... */ }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == AnonymousObjectAllocationAnalyzer.AnonymousObjectAllocationId);
        Assert.True(hasDiagnostic, "Analyzer should detect anonymous object in async method");
    }

    [Fact]
    public void ShouldDetectAnonymousObjectInNPipelineNodeClass()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<int, string>
                   {
                       public string ProcessItem(int item)
                       {
                           // NP9203: Anonymous object in NPipeline node class
                           var result = new { Input = item, Output = item.ToString(), Processed = true };
                           return result.Output;
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == AnonymousObjectAllocationAnalyzer.AnonymousObjectAllocationId);
        Assert.True(hasDiagnostic, "Analyzer should detect anonymous object in NPipeline node class");
    }

    [Fact]
    public void ShouldDetectAnonymousObjectInSinkNode()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestSinkNode : ISinkNode<string>
                   {
                       public async Task HandleAsync(string input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           // NP9203: Anonymous object in sink node
                           var enriched = new { Data = input, Metadata = new { Source = "Test", Version = 1 } };
                           await SaveAsync(enriched);
                       }
                       
                       private async Task SaveAsync(object obj) { /* ... */ }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == AnonymousObjectAllocationAnalyzer.AnonymousObjectAllocationId);
        Assert.True(hasDiagnostic, "Analyzer should detect anonymous object in sink node");
    }

    [Fact]
    public void ShouldDetectAnonymousObjectInAggregateNode()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestAggregateNode : IAggregateNode<int>
                   {
                       public async Task<IDataPipe<int>> ExecuteAsync(
                           IDataPipe<int> input, 
                           PipelineContext context, 
                           CancellationToken cancellationToken)
                       {
                           var items = new List<int>();
                           // NP9203: Anonymous object in aggregate node
                           var aggregated = items.Select(x => new { Value = x, Category = x > 0 ? "Positive" : "Negative" }).ToList();
                           return new InMemoryDataPipe<int>(aggregated.Select(a => a.Value));
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == AnonymousObjectAllocationAnalyzer.AnonymousObjectAllocationId);
        Assert.True(hasDiagnostic, "Analyzer should detect anonymous object in aggregate node");
    }

    [Fact]
    public void ShouldDetectAnonymousObjectInLoop()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public async Task<string> ExecuteAsync(string input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           var items = input.Split(' ');
                           for (int i = 0; i < items.Length; i++)
                           {
                               // NP9203: Anonymous object in loop
                               var processed = new { Index = i, Value = items[i], Length = items[i].Length };
                               await ProcessItemAsync(processed);
                           }
                           return input;
                       }
                       
                       private async Task ProcessItemAsync(object obj) { /* ... */ }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == AnonymousObjectAllocationAnalyzer.AnonymousObjectAllocationId);
        Assert.True(hasDiagnostic, "Analyzer should detect anonymous object in loop");
    }

    [Fact]
    public void ShouldDetectAnonymousObjectInLinqExpression()
    {
        var code = """
                   using NPipeline.Nodes;
                   using System.Linq;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public async Task<string> ExecuteAsync(string input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           var items = input.Split(' ');
                           // NP9203: Anonymous object in LINQ expression
                           var processed = items.Select(x => new { Original = x, Upper = x.ToUpper() }).ToList();
                           return string.Join(",", processed.Select(p => p.Upper));
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == AnonymousObjectAllocationAnalyzer.AnonymousObjectAllocationId);
        Assert.True(hasDiagnostic, "Analyzer should detect anonymous object in LINQ expression");
    }

    [Fact]
    public void ShouldDetectAnonymousObjectInValueTaskReturningMethod()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public ValueTask<string> ExecuteAsync(string input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           // NP9203: Anonymous object in ValueTask-returning method
                           var result = new { Input = input, Length = input.Length };
                           return new ValueTask<string>(result.Input);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == AnonymousObjectAllocationAnalyzer.AnonymousObjectAllocationId);
        Assert.True(hasDiagnostic, "Analyzer should detect anonymous object in ValueTask-returning method");
    }

    [Fact]
    public void ShouldIgnoreAnonymousObjectInNonHotPathMethod()
    {
        var code = """
                   public class TestClass
                   {
                       public void ProcessData(IEnumerable<string> data)
                       {
                           // This should not trigger NP9203 - not a hot path method
                           var result = data.Select(x => new { Value = x, Length = x.Length }).ToList();
                           Console.WriteLine(result.Count);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == AnonymousObjectAllocationAnalyzer.AnonymousObjectAllocationId);
        Assert.False(hasDiagnostic, "Analyzer should not detect anonymous object in non-hot path method");
    }

    [Fact]
    public void ShouldIgnoreAnonymousObjectInNonAsyncMethod()
    {
        var code = """
                   public class TestClass
                   {
                       public string ProcessData(IEnumerable<string> data)
                       {
                           // This should not trigger NP9203 - not async
                           var result = data.Select(x => new { Value = x, Upper = x.ToUpper() }).ToList();
                           return string.Join(",", result.Select(r => r.Value));
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == AnonymousObjectAllocationAnalyzer.AnonymousObjectAllocationId);
        Assert.False(hasDiagnostic, "Analyzer should not detect anonymous object in non-async method");
    }

    [Fact]
    public void ShouldIgnoreAnonymousObjectInNonNodeClass()
    {
        var code = """
                   public class RegularClass
                   {
                       public async Task<string> ProcessDataAsync(IEnumerable<string> data)
                       {
                           // This should not trigger NP9203 - not in NPipeline node
                           var result = data.Select(x => new { Value = x, Processed = true }).ToList();
                           return string.Join(",", result.Select(r => r.Value));
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == AnonymousObjectAllocationAnalyzer.AnonymousObjectAllocationId);
        Assert.False(hasDiagnostic, "Analyzer should not detect anonymous object in non-NPipeline node class");
    }

    [Fact]
    public void ShouldDetectMultipleAnonymousObjectAllocations()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public async Task<string> ExecuteAsync(string input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           var items = input.Split(' ');
                           // NP9203: Multiple anonymous object allocations in hot path
                           var first = items.Select(x => new { Original = x, Length = x.Length }).ToList();
                           var second = first.Select(x => new { Data = x, IsValid = x.Length > 0 }).ToList();
                           return string.Join(",", second.Select(s => s.Data.Original));
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var anonymousObjectDiagnostics = diagnostics.Where(d => d.Id == AnonymousObjectAllocationAnalyzer.AnonymousObjectAllocationId).ToList();
        Assert.True(anonymousObjectDiagnostics.Count >= 2, "Analyzer should detect multiple anonymous object allocations");
    }

    [Fact]
    public void ShouldDetectAnonymousObjectInForeachLoop()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public async Task<string> ExecuteAsync(string input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           var items = input.Split(' ');
                           foreach (var item in items)
                           {
                               // NP9203: Anonymous object in foreach loop
                               var processed = new { Item = item, Processed = true, Timestamp = DateTime.UtcNow };
                               await SaveAsync(processed);
                           }
                           return input;
                       }
                       
                       private async Task SaveAsync(object obj) { /* ... */ }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == AnonymousObjectAllocationAnalyzer.AnonymousObjectAllocationId);
        Assert.True(hasDiagnostic, "Analyzer should detect anonymous object in foreach loop");
    }

    [Fact]
    public void ShouldDetectAnonymousObjectInWhileLoop()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public async Task<string> ExecuteAsync(string input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           var items = input.Split(' ');
                           int i = 0;
                           while (i < items.Length)
                           {
                               // NP9203: Anonymous object in while loop
                               var processed = new { Index = i, Value = items[i] };
                               await ProcessAsync(processed);
                               i++;
                           }
                           return input;
                       }
                       
                       private async Task ProcessAsync(object obj) { /* ... */ }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == AnonymousObjectAllocationAnalyzer.AnonymousObjectAllocationId);
        Assert.True(hasDiagnostic, "Analyzer should detect anonymous object in while loop");
    }

    private static IEnumerable<Diagnostic> GetDiagnostics(string code)
    {
        var syntaxTree = CSharpSyntaxTree.ParseText(code);

        // Get path to NPipeline assembly
        var nPipelineAssemblyPath = typeof(INode).Assembly.Location;

        var references = new[]
        {
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Enumerable).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Task).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(DateTime).Assembly.Location),
            MetadataReference.CreateFromFile(nPipelineAssemblyPath),
        };

        var compilation = CSharpCompilation.Create("TestAssembly")
            .AddReferences(references)
            .AddSyntaxTrees(syntaxTree);

        var analyzer = new AnonymousObjectAllocationAnalyzer();
        var compilationWithAnalyzers = compilation.WithAnalyzers([analyzer]);
        var diagnostics = compilationWithAnalyzers.GetAnalyzerDiagnosticsAsync().Result;

        return diagnostics;
    }
}

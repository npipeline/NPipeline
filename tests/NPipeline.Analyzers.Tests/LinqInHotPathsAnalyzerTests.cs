using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using NPipeline.Nodes;

namespace NPipeline.Analyzers.Tests;

/// <summary>
///     Tests for LinqInHotPathsAnalyzer.
/// </summary>
public sealed class LinqInHotPathsAnalyzerTests
{
    [Fact]
    public void ShouldDetectLinqInExecuteAsyncMethod()
    {
        var code = """
                   using System.Linq;
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public async Task<string> ExecuteAsync(string input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           // NP9205: LINQ in hot path
                           var items = input.Split(' ');
                           var result = items.Where(x => x.Length > 5).ToList();
                           return string.Join(",", result);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == LinqInHotPathsAnalyzer.DiagnosticId);
        Assert.True(hasDiagnostic, "Analyzer should detect LINQ in ExecuteAsync method");
    }

    [Fact]
    public void ShouldDetectLinqInProcessAsyncMethod()
    {
        var code = """
                   using System.Linq;
                   using NPipeline.Nodes;

                   public class TestNode : INode
                   {
                       public async Task ProcessAsync(IEnumerable<string> items)
                       {
                           // NP9205: LINQ in hot path
                           var filtered = items.Where(x => x != null).Select(x => x.ToUpper()).ToArray();
                           // Process filtered items...
                       }
                       
                       public ValueTask DisposeAsync() => default;
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == LinqInHotPathsAnalyzer.DiagnosticId);
        Assert.True(hasDiagnostic, "Analyzer should detect LINQ in ProcessAsync method");
    }

    [Fact]
    public void ShouldDetectLinqInRunAsyncMethod()
    {
        var code = """
                   using System.Linq;
                   using NPipeline.Nodes;

                   public class TestSourceNode : ISourceNode<int>
                   {
                       public async Task<IDataPipe<int>> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
                       {
                           var numbers = Enumerable.Range(1, 100);
                           // NP9205: LINQ in hot path
                           var filtered = numbers.Where(x => x % 2 == 0).ToList();
                           return new ListDataPipe<int>(filtered);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == LinqInHotPathsAnalyzer.DiagnosticId);
        Assert.True(hasDiagnostic, "Analyzer should detect LINQ in RunAsync method");
    }

    [Fact]
    public void ShouldDetectLinqInAsyncMethod()
    {
        var code = """
                   using System.Linq;
                   using NPipeline.Nodes;

                   public class TestClass
                   {
                       public async Task HandleDataAsync(IEnumerable<string> data)
                       {
                           // NP9205: LINQ in async method (hot path)
                           var result = data.GroupBy(x => x.Length).ToList();
                           // Process grouped data...
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == LinqInHotPathsAnalyzer.DiagnosticId);
        Assert.True(hasDiagnostic, "Analyzer should detect LINQ in async method");
    }

    [Fact]
    public void ShouldDetectLinqInNPipelineNodeClass()
    {
        var code = """
                   using System.Linq;
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<int, string>
                   {
                       public string ProcessItem(int item)
                       {
                           // NP9205: LINQ in NPipeline node class
                           var result = Enumerable.Range(item, 10).Where(x => x > item).ToList();
                           return string.Join(",", result);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == LinqInHotPathsAnalyzer.DiagnosticId);
        Assert.True(hasDiagnostic, "Analyzer should detect LINQ in NPipeline node class");
    }

    [Fact]
    public void ShouldDetectLinqInSinkNode()
    {
        var code = """
                   using System.Linq;
                   using NPipeline.Nodes;

                   public class TestSinkNode : ISinkNode<string>
                   {
                       public async Task HandleAsync(string input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           // NP9205: LINQ in sink node
                           var words = input.Split(' ').Where(x => x.Length > 3).ToArray();
                           await ProcessWordsAsync(words);
                       }
                       
                       private async Task ProcessWordsAsync(string[] words) { /* ... */ }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == LinqInHotPathsAnalyzer.DiagnosticId);
        Assert.True(hasDiagnostic, "Analyzer should detect LINQ in sink node");
    }

    [Fact]
    public void ShouldDetectLinqInAggregateNode()
    {
        var code = """
                   using System.Linq;
                   using NPipeline.Nodes;

                   public class TestAggregateNode : IAggregateNode<int>
                   {
                       public async Task<IDataPipe<int>> ExecuteAsync(
                           IDataPipe<int> input, 
                           PipelineContext context, 
                           CancellationToken cancellationToken)
                       {
                           var items = new List<int>();
                           // NP9205: LINQ in aggregate node
                           var filtered = items.Where(x => x > 0).OrderBy(x => x).ToList();
                           return new ListDataPipe<int>(filtered);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == LinqInHotPathsAnalyzer.DiagnosticId);
        Assert.True(hasDiagnostic, "Analyzer should detect LINQ in aggregate node");
    }

    [Fact]
    public void ShouldIgnoreLinqInNonHotPathMethod()
    {
        var code = """
                   using System.Linq;

                   public class TestClass
                   {
                       public void ProcessData(IEnumerable<string> data)
                       {
                           // This should not trigger NP9205 - not a hot path method
                           var result = data.Where(x => x != null).ToList();
                           Console.WriteLine(result.Count);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == LinqInHotPathsAnalyzer.DiagnosticId);
        Assert.False(hasDiagnostic, "Analyzer should not detect LINQ in non-hot path method");
    }

    [Fact]
    public void ShouldIgnoreLinqInNonAsyncMethod()
    {
        var code = """
                   using System.Linq;

                   public class TestClass
                   {
                       public string ProcessData(IEnumerable<string> data)
                       {
                           // This should not trigger NP9205 - not async
                           var result = data.Where(x => x.Length > 0).ToList();
                           return string.Join(",", result);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == LinqInHotPathsAnalyzer.DiagnosticId);
        Assert.False(hasDiagnostic, "Analyzer should not detect LINQ in non-async method");
    }

    [Fact]
    public void ShouldIgnoreLinqInNonNodeClass()
    {
        var code = """
                   using System.Linq;

                   public class RegularClass
                   {
                       public async Task<string> ProcessDataAsync(IEnumerable<string> data)
                       {
                           // This should not trigger NP9205 - not in NPipeline node
                           var result = data.Where(x => x != null).ToList();
                           return string.Join(",", result);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == LinqInHotPathsAnalyzer.DiagnosticId);
        Assert.False(hasDiagnostic, "Analyzer should not detect LINQ in non-NPipeline node class");
    }

    [Fact]
    public void ShouldDetectMultipleLinqOperations()
    {
        var code = """
                   using System.Linq;
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public async Task<string> ExecuteAsync(string input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           // NP9205: Multiple LINQ operations in hot path
                           var words = input.Split(' ').Where(x => x.Length > 2).ToList();
                           var filtered = words.Select(x => x.ToUpper()).Where(x => x.StartsWith("A")).ToArray();
                           return string.Join(",", filtered);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var linqDiagnostics = diagnostics.Where(d => d.Id == LinqInHotPathsAnalyzer.DiagnosticId).ToList();
        Assert.True(linqDiagnostics.Count >= 2, "Analyzer should detect multiple LINQ operations");
    }

    [Fact]
    public void ShouldDetectChainedLinqOperations()
    {
        var code = """
                   using System.Linq;
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public async Task<string> ExecuteAsync(string input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           // NP9205: Chained LINQ operations in hot path
                           var result = input.Split(' ')
                               .Where(x => x.Length > 0)
                               .Select(x => x.Trim())
                               .Where(x => x.Length > 2)
                               .OrderBy(x => x)
                               .ToList();
                           return string.Join(",", result);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == LinqInHotPathsAnalyzer.DiagnosticId);
        Assert.True(hasDiagnostic, "Analyzer should detect chained LINQ operations");
    }

    [Fact]
    public void ShouldDetectLinqInValueTaskReturningMethod()
    {
        var code = """
                   using System.Linq;
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public ValueTask<string> ExecuteAsync(string input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           // NP9205: LINQ in ValueTask-returning method
                           var result = input.Split(' ').Where(x => x.Length > 0).ToList();
                           return new ValueTask<string>(string.Join(",", result));
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == LinqInHotPathsAnalyzer.DiagnosticId);
        Assert.True(hasDiagnostic, "Analyzer should detect LINQ in ValueTask-returning method");
    }

    [Fact]
    public void ShouldDetectCommonLinqMethods()
    {
        var linqMethods = new[]
        {
            "Where", "Select", "SelectMany", "GroupBy", "OrderBy", "OrderByDescending",
            "ThenBy", "ThenByDescending", "ToList", "ToArray", "ToDictionary", "ToHashSet",
            "First", "FirstOrDefault", "Single", "SingleOrDefault", "Last", "LastOrDefault",
            "Count", "LongCount", "Sum", "Average", "Min", "Max", "Aggregate", "Distinct",
            "Union", "Intersect", "Except", "Concat", "Reverse", "Skip", "Take", "SkipWhile",
            "TakeWhile", "Join", "GroupJoin", "Zip", "SequenceEqual", "All", "Any", "Contains",
        };

        foreach (var method in linqMethods)
        {
            var code = $$"""
                         using System.Linq;
                         using NPipeline.Nodes;

                         public class TestTransformNode : ITransformNode<string, string>
                         {
                             public async Task<string> ExecuteAsync(string input, PipelineContext context, CancellationToken cancellationToken)
                             {
                                 // NP9205: {{method}} in hot path
                                 var items = input.Split(' ');
                                 var result = items.{{method}}(x => x.Length > 0);
                                 return string.Join(",", result);
                             }
                         }
                         """;

            var diagnostics = GetDiagnostics(code);
            var hasDiagnostic = diagnostics.Any(d => d.Id == LinqInHotPathsAnalyzer.DiagnosticId);
            Assert.True(hasDiagnostic, $"Analyzer should detect {method} LINQ method");
        }
    }

    [Fact]
    public void ShouldIgnoreNonSystemLinqMethods()
    {
        var code = """
                   using System.Linq;
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public async Task<string> ExecuteAsync(string input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           // This should not trigger NP9205 - custom Where method, not System.Linq
                           var items = input.Split(' ');
                           var result = items.CustomWhere(x => x.Length > 0);
                           return string.Join(",", result);
                       }
                   }

                   public static class Extensions
                   {
                       public static IEnumerable<string> CustomWhere(this IEnumerable<string> source, Func<string, bool> predicate)
                       {
                           return source.Where(predicate);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == LinqInHotPathsAnalyzer.DiagnosticId);
        Assert.False(hasDiagnostic, "Analyzer should not detect non-System.Linq methods");
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
            MetadataReference.CreateFromFile(nPipelineAssemblyPath),
        };

        var compilation = CSharpCompilation.Create("TestAssembly")
            .AddReferences(references)
            .AddSyntaxTrees(syntaxTree);

        var analyzer = new LinqInHotPathsAnalyzer();
        var compilationWithAnalyzers = compilation.WithAnalyzers([analyzer]);
        var diagnostics = compilationWithAnalyzers.GetAnalyzerDiagnosticsAsync().Result;

        return diagnostics;
    }
}

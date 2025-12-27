using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using NPipeline.Nodes;

namespace NPipeline.Analyzers.Tests;

/// <summary>
///     Tests for InefficientStringOperationsAnalyzer.
/// </summary>
public sealed class InefficientStringOperationsAnalyzerTests
{
    [Fact]
    public void ShouldDetectStringConcatenationInLoop()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public string BuildReport(string[] items)
                       {
                           var report = "";
                           foreach (var item in items)
                           {
                               // NP9202: String concatenation creates new objects each iteration
                               report += "Item: " + item + "\n";
                           }
                           return report;
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientStringOperationsAnalyzer.InefficientStringOperationsId);
        Assert.True(hasDiagnostic, "Analyzer should detect string concatenation in loop");
    }

    [Fact]
    public void ShouldDetectStringConcatenationInHotPath()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestNode : INode
                   {
                       public async Task ProcessAsync(Input input)
                       {
                           // NP9202: String concatenation in hot path
                           var message = "Processing " + input.Type + " with ID " + input.Id;
                           await LogAsync(message);
                       }
                       
                       private async Task LogAsync(string message) { /* ... */ }
                       public ValueTask DisposeAsync() => default;
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientStringOperationsAnalyzer.InefficientStringOperationsId);
        Assert.True(hasDiagnostic, "Analyzer should detect string concatenation in hot path");
    }

    [Fact]
    public void ShouldDetectMultipleStringConcatenations()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public string FormatData(Data data)
                       {
                           // NP9202: Multiple string operations create allocations
                           return "Data: " + data.Name + ", Value: " + data.Value.ToString() + ", Time: " + data.Timestamp;
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientStringOperationsAnalyzer.InefficientStringOperationsId);
        Assert.True(hasDiagnostic, "Analyzer should detect multiple string concatenations");
    }

    [Fact]
    public void ShouldDetectStringConcatenationInAsyncMethod()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestClass
                   {
                       public async Task HandleDataAsync(Data data)
                       {
                           // NP9202: String concatenation in async method (hot path)
                           var message = "Processing " + data.Type + " with value " + data.Value;
                           await ProcessAsync(message);
                       }
                       
                       private async Task ProcessAsync(string message) { /* ... */ }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientStringOperationsAnalyzer.InefficientStringOperationsId);
        Assert.True(hasDiagnostic, "Analyzer should detect string concatenation in async method");
    }

    [Fact]
    public void ShouldDetectStringConcatenationInNPipelineNodeClass()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<int, string>
                   {
                       public string ProcessItem(int item)
                       {
                           // NP9202: String concatenation in NPipeline node class
                           return "Item: " + item + ", Processed: " + (item * 2);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientStringOperationsAnalyzer.InefficientStringOperationsId);
        Assert.True(hasDiagnostic, "Analyzer should detect string concatenation in NPipeline node class");
    }

    [Fact]
    public void ShouldDetectStringConcatenationInWhileLoop()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public string ProcessItems(string[] items)
                       {
                           var result = "";
                           var i = 0;
                           while (i < items.Length)
                           {
                               // NP9202: String concatenation in while loop
                               result += items[i] + ",";
                               i++;
                           }
                           return result;
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientStringOperationsAnalyzer.InefficientStringOperationsId);
        Assert.True(hasDiagnostic, "Analyzer should detect string concatenation in while loop");
    }

    [Fact]
    public void ShouldDetectStringConcatenationInForLoop()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public string ProcessItems(string[] items)
                       {
                           var result = "";
                           for (int i = 0; i < items.Length; i++)
                           {
                               // NP9202: String concatenation in for loop
                               result += "Item " + i + ": " + items[i] + "\n";
                           }
                           return result;
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientStringOperationsAnalyzer.InefficientStringOperationsId);
        Assert.True(hasDiagnostic, "Analyzer should detect string concatenation in for loop");
    }

    [Fact]
    public void ShouldDetectStringConcatenationInDoWhileLoop()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public string ProcessItems(string[] items)
                       {
                           var result = "";
                           var i = 0;
                           do
                           {
                               // NP9202: String concatenation in do-while loop
                               result += items[i] + ",";
                               i++;
                           } while (i < items.Length);
                           return result;
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientStringOperationsAnalyzer.InefficientStringOperationsId);
        Assert.True(hasDiagnostic, "Analyzer should detect string concatenation in do-while loop");
    }

    [Fact]
    public void ShouldDetectStringConcatenationInSinkNode()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestSinkNode : ISinkNode<string>
                   {
                       public async Task HandleAsync(string input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           // NP9202: String concatenation in sink node
                           var logMessage = "Processing input: " + input + " at " + DateTime.Now;
                           await LogAsync(logMessage);
                       }
                       
                       private async Task LogAsync(string message) { /* ... */ }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientStringOperationsAnalyzer.InefficientStringOperationsId);
        Assert.True(hasDiagnostic, "Analyzer should detect string concatenation in sink node");
    }

    [Fact]
    public void ShouldDetectStringConcatenationInAggregateNode()
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
                           // NP9202: String concatenation in aggregate node
                           var message = "Processing " + input.Count + " items";
                           await LogAsync(message);
                           
                           return input;
                       }
                       
                       private async Task LogAsync(string message) { /* ... */ }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientStringOperationsAnalyzer.InefficientStringOperationsId);
        Assert.True(hasDiagnostic, "Analyzer should detect string concatenation in aggregate node");
    }

    [Fact]
    public void ShouldIgnoreStringConcatenationInNonHotPathMethod()
    {
        var code = """
                   public class TestClass
                   {
                       public void ProcessData(string[] items)
                       {
                           // This should not trigger NP9202 - not a hot path method
                           var result = "";
                           foreach (var item in items)
                           {
                               result += item + ",";
                           }
                           Console.WriteLine(result);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientStringOperationsAnalyzer.InefficientStringOperationsId);
        Assert.False(hasDiagnostic, "Analyzer should not detect string concatenation in non-hot path method");
    }

    [Fact]
    public void ShouldIgnoreStringConcatenationInNonAsyncMethod()
    {
        var code = """
                   public class TestClass
                   {
                       public string ProcessData(string[] items)
                       {
                           // This should not trigger NP9202 - not async
                           var result = "";
                           foreach (var item in items)
                           {
                               result += item + ",";
                           }
                           return result;
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientStringOperationsAnalyzer.InefficientStringOperationsId);
        Assert.False(hasDiagnostic, "Analyzer should not detect string concatenation in non-async method");
    }

    [Fact]
    public void ShouldIgnoreStringConcatenationInNonNodeClass()
    {
        var code = """
                   public class RegularClass
                   {
                       public async Task ProcessDataAsync(string[] items)
                       {
                           // This should not trigger NP9202 - not in NPipeline node
                           var result = "";
                           foreach (var item in items)
                           {
                               result += item + ",";
                           }
                           await Console.Out.WriteLineAsync(result);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientStringOperationsAnalyzer.InefficientStringOperationsId);
        Assert.False(hasDiagnostic, "Analyzer should not detect string concatenation in non-NPipeline node class");
    }

    [Fact]
    public void ShouldDetectStringConcatenationInValueTaskReturningMethod()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public ValueTask<string> ExecuteAsync(string input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           // NP9202: String concatenation in ValueTask-returning method
                           var result = "Processed: " + input + " at " + DateTime.Now;
                           return new ValueTask<string>(result);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientStringOperationsAnalyzer.InefficientStringOperationsId);
        Assert.True(hasDiagnostic, "Analyzer should detect string concatenation in ValueTask-returning method");
    }

    [Fact]
    public void ShouldDetectMultipleStringConcatenationsInSameExpression()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public string BuildMessage(string type, string id, string value)
                       {
                           // NP9202: Multiple string concatenations in single expression
                           return "Type: " + type + ", ID: " + id + ", Value: " + value + ", Time: " + DateTime.Now;
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientStringOperationsAnalyzer.InefficientStringOperationsId);
        Assert.True(hasDiagnostic, "Analyzer should detect multiple string concatenations in same expression");
    }

    [Fact]
    public void ShouldDetectStringConcatenationInRunAsyncMethod()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestSourceNode : ISourceNode<int>
                   {
                       public async Task<IDataPipe<int>> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
                       {
                           // NP9202: String concatenation in RunAsync method
                           var message = "Starting source node at " + DateTime.Now;
                           await LogAsync(message);
                           
                           return new InMemoryDataPipe<int>(new List<int> { 1, 2, 3 });
                       }
                       
                       private async Task LogAsync(string message) { /* ... */ }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientStringOperationsAnalyzer.InefficientStringOperationsId);
        Assert.True(hasDiagnostic, "Analyzer should detect string concatenation in RunAsync method");
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

        var analyzer = new InefficientStringOperationsAnalyzer();
        var compilationWithAnalyzers = compilation.WithAnalyzers([analyzer]);
        var diagnostics = compilationWithAnalyzers.GetAnalyzerDiagnosticsAsync().Result;

        return diagnostics;
    }
}

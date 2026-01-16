using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using NPipeline.Nodes;

namespace NPipeline.Analyzers.Tests;

/// <summary>
///     Tests for InefficientExceptionHandlingAnalyzer.
/// </summary>
public sealed class InefficientExceptionHandlingAnalyzerTests
{
    [Fact]
    public void ShouldDetectCatchAllException()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           try
                           {
                               await ProcessAsync(input);
                           }
                           // NP9104: Catch-all exception handler
                           catch (Exception ex)
                           {
                               _logger.LogError(ex, "Processing failed");
                               throw;
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientExceptionHandlingAnalyzer.InefficientExceptionHandlingId);
        Assert.True(hasDiagnostic, "Analyzer should detect catch-all exception handler");
    }

    [Fact]
    public void ShouldCatchAllExceptionWithoutType()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           try
                           {
                               await ProcessAsync(input);
                           }
                           // NP9104: Catch-all exception handler without type
                           catch
                           {
                               _logger.LogError("Processing failed");
                               throw;
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientExceptionHandlingAnalyzer.InefficientExceptionHandlingId);
        Assert.True(hasDiagnostic, "Analyzer should detect catch-all exception handler without type");
    }

    [Fact]
    public void ShouldDetectEmptyCatchBlock()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           try
                           {
                               await ProcessAsync(input);
                           }
                           // NP9104: Empty catch block
                           catch
                           {
                               // Exception silently ignored
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientExceptionHandlingAnalyzer.InefficientExceptionHandlingId);
        Assert.True(hasDiagnostic, "Analyzer should detect empty catch block");
    }

    [Fact]
    public void ShouldDetectExceptionSwallowing()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public async ValueTask<string> ExecuteAsync(string input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           try
                           {
                               return await TransformAsync(input);
                           }
                           // NP9104: Exception swallowing pattern
                           catch (Exception)
                           {
                               // Log but don't re-throw - silent failure
                               _logger.Warning("Transform failed");
                               return default(string);
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientExceptionHandlingAnalyzer.InefficientExceptionHandlingId);
        Assert.True(hasDiagnostic, "Analyzer should detect exception swallowing pattern");
    }

    [Fact]
    public void ShouldDetectInefficientExceptionFiltering()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           try
                           {
                               await SaveAsync(input);
                           }
                           // NP9104: Inefficient exception filtering
                           catch (Exception ex) when (ex.Message.Contains("timeout"))
                           {
                               _logger.LogWarning($"Timeout occurred: {ex.Message}");
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientExceptionHandlingAnalyzer.InefficientExceptionHandlingId);
        Assert.True(hasDiagnostic, "Analyzer should detect inefficient exception filtering");
    }

    [Fact]
    public void ShouldDetectImproperRethrow()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           try
                           {
                               await ProcessAsync(input);
                           }
                           // NP9104: Improper re-throw pattern
                           catch (Exception ex)
                           {
                               _logger.LogError(ex, "Processing failed");
                               throw ex; // Re-throwing loses stack trace
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientExceptionHandlingAnalyzer.InefficientExceptionHandlingId);
        Assert.True(hasDiagnostic, "Analyzer should detect improper re-throw pattern");
    }

    [Fact]
    public void ShouldDetectExceptionHandlingInProcessAsync()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public async Task ProcessAsync(string input)
                       {
                           try
                           {
                               await ProcessDataAsync(input);
                           }
                           // NP9104: Exception handling in ProcessAsync method
                           catch (Exception ex)
                           {
                               _logger.LogError(ex, "Processing failed");
                               throw;
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientExceptionHandlingAnalyzer.InefficientExceptionHandlingId);
        Assert.True(hasDiagnostic, "Analyzer should detect exception handling in ProcessAsync method");
    }

    [Fact]
    public void ShouldDetectExceptionHandlingInSinkNode()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestSinkNode : ISinkNode<string>
                   {
                       public async Task HandleAsync(string input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           try
                           {
                               await SaveAsync(input);
                           }
                           // NP9104: Exception handling in sink node
                           catch (Exception ex)
                           {
                               _logger.LogError(ex, "Save failed");
                               throw;
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientExceptionHandlingAnalyzer.InefficientExceptionHandlingId);
        Assert.True(hasDiagnostic, "Analyzer should detect exception handling in sink node");
    }

    [Fact]
    public void ShouldDetectExceptionHandlingInSourceNode()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestSourceNode : ISourceNode<int>
                   {
                       public async Task<IDataPipe<int>> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
                       {
                           try
                           {
                               return await GetDataAsync();
                           }
                           // NP9104: Exception handling in source node
                           catch (Exception ex)
                           {
                               _logger.LogError(ex, "Data retrieval failed");
                               throw;
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientExceptionHandlingAnalyzer.InefficientExceptionHandlingId);
        Assert.True(hasDiagnostic, "Analyzer should detect exception handling in source node");
    }

    [Fact]
    public void ShouldDetectExceptionHandlingInAggregateNode()
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
                           try
                           {
                               return await AggregateAsync(input);
                           }
                           // NP9104: Exception handling in aggregate node
                           catch (Exception ex)
                           {
                               _logger.LogError(ex, "Aggregation failed");
                               throw;
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientExceptionHandlingAnalyzer.InefficientExceptionHandlingId);
        Assert.True(hasDiagnostic, "Analyzer should detect exception handling in aggregate node");
    }

    [Fact]
    public void ShouldIgnoreSpecificExceptionHandling()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           try
                           {
                               await ProcessAsync(input);
                           }
                           // This should not trigger NP9104 - specific exception type
                           catch (IOException ex)
                           {
                               _logger.LogError(ex, "IO processing failed");
                               throw;
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientExceptionHandlingAnalyzer.InefficientExceptionHandlingId);
        Assert.False(hasDiagnostic, "Analyzer should not flag specific exception handling");
    }

    [Fact]
    public void ShouldIgnoreExceptionHandlingInNonHotPathMethod()
    {
        var code = """
                   public class TestClass
                   {
                       public void ProcessData(string data)
                       {
                           try
                           {
                               ProcessItem(data);
                           }
                           // This should not trigger NP9104 - not a hot path method
                           catch (Exception ex)
                           {
                               Console.WriteLine($"Error: {ex.Message}");
                           }
                       }
                       
                       private void ProcessItem(string item) { /* ... */ }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientExceptionHandlingAnalyzer.InefficientExceptionHandlingId);
        Assert.False(hasDiagnostic, "Analyzer should not flag exception handling in non-hot path method");
    }

    [Fact]
    public void ShouldIgnoreExceptionHandlingInNonAsyncMethod()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public string ProcessItem(string item)
                       {
                           try
                           {
                               return TransformItem(item);
                           }
                           // This should not trigger NP9104 - not async
                           catch (Exception ex)
                           {
                               Console.WriteLine($"Error: {ex.Message}");
                               return item;
                           }
                       }
                       
                       private string TransformItem(string item) => item.ToUpper();
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientExceptionHandlingAnalyzer.InefficientExceptionHandlingId);
        Assert.False(hasDiagnostic, "Analyzer should not flag exception handling in non-async method");
    }

    [Fact]
    public void ShouldIgnoreExceptionHandlingInNonNodeClass()
    {
        var code = """
                   public class RegularClass
                   {
                       public async Task ProcessDataAsync(string data)
                       {
                           try
                           {
                               await ProcessItemAsync(data);
                           }
                           // This should not trigger NP9104 - not in NPipeline node
                           catch (Exception ex)
                           {
                               Console.WriteLine($"Error: {ex.Message}");
                           }
                       }
                       
                       private async Task ProcessItemAsync(string item) { /* ... */ }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientExceptionHandlingAnalyzer.InefficientExceptionHandlingId);
        Assert.False(hasDiagnostic, "Analyzer should not flag exception handling in non-NPipeline node class");
    }

    [Fact]
    public void ShouldDetectExceptionHandlingInValueTaskReturningMethod()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public ValueTask<string> ExecuteAsync(string input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           try
                           {
                               return await TransformAsync(input);
                           }
                           // NP9104: Exception handling in ValueTask-returning method
                           catch (Exception ex)
                           {
                               _logger.LogError(ex, "Processing failed");
                               throw;
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientExceptionHandlingAnalyzer.InefficientExceptionHandlingId);
        Assert.True(hasDiagnostic, "Analyzer should detect exception handling in ValueTask-returning method");
    }

    [Fact]
    public void ShouldDetectExceptionHandlingInRunAsyncMethod()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestSourceNode : ISourceNode<int>
                   {
                       public async Task<IDataPipe<int>> RunAsync(PipelineContext context, CancellationToken cancellationToken)
                       {
                           try
                           {
                               return await GetDataAsync();
                           }
                           // NP9104: Exception handling in RunAsync method
                           catch (Exception ex)
                           {
                               _logger.LogError(ex, "Data retrieval failed");
                               throw;
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientExceptionHandlingAnalyzer.InefficientExceptionHandlingId);
        Assert.True(hasDiagnostic, "Analyzer should detect exception handling in RunAsync method");
    }

    [Fact]
    public void ShouldDetectExceptionHandlingInHandleAsyncMethod()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public async Task HandleAsync(string input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           try
                           {
                               return await ProcessAsync(input);
                           }
                           // NP9104: Exception handling in HandleAsync method
                           catch (Exception ex)
                           {
                               _logger.LogError(ex, "Processing failed");
                               throw;
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientExceptionHandlingAnalyzer.InefficientExceptionHandlingId);
        Assert.True(hasDiagnostic, "Analyzer should detect exception handling in HandleAsync method");
    }

    [Fact]
    public void ShouldDetectExceptionHandlingInExecuteMethod()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public async Task Initialize(string input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           try
                           {
                               return await ProcessAsync(input);
                           }
                           // NP9104: Exception handling in Initialize method
                           catch (Exception ex)
                           {
                               _logger.LogError(ex, "Processing failed");
                               throw;
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientExceptionHandlingAnalyzer.InefficientExceptionHandlingId);
        Assert.True(hasDiagnostic, "Analyzer should detect exception handling in Initialize method");
    }

    [Fact]
    public void ShouldDetectExceptionHandlingInProcessMethod()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public async Task Process(string input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           try
                           {
                               return await ProcessAsync(input);
                           }
                           // NP9104: Exception handling in Process method
                           catch (Exception ex)
                           {
                               _logger.LogError(ex, "Processing failed");
                               throw;
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientExceptionHandlingAnalyzer.InefficientExceptionHandlingId);
        Assert.True(hasDiagnostic, "Analyzer should detect exception handling in Process method");
    }

    [Fact]
    public void ShouldDetectExceptionHandlingWithLoggingOnly()
    {
        var code = """
                   using NPipeline.Nodes;

                   public class TestTransformNode : ITransformNode<string, string>
                   {
                       public async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           try
                           {
                               await ProcessAsync(input);
                           }
                           // NP9104: Exception swallowing with logging only
                           catch (Exception ex)
                           {
                               _logger.LogError(ex, "Processing failed");
                               // No re-throw or proper handling
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == InefficientExceptionHandlingAnalyzer.InefficientExceptionHandlingId);
        Assert.True(hasDiagnostic, "Analyzer should detect exception swallowing with logging only");
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

        var analyzer = new InefficientExceptionHandlingAnalyzer();
        var compilationWithAnalyzers = compilation.WithAnalyzers([analyzer]);
        var diagnostics = compilationWithAnalyzers.GetAnalyzerDiagnosticsAsync().Result;

        return diagnostics;
    }
}

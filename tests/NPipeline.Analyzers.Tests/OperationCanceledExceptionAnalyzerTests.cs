using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using Xunit;

namespace NPipeline.Analyzers.Tests;

/// <summary>
///     Tests for OperationCanceledExceptionAnalyzer.
/// </summary>
public sealed class OperationCanceledExceptionAnalyzerTests
{
    [Fact]
    public void ShouldDetectCatchingExceptionWithoutReThrow()
    {
        var code = """
                   using System;
                   using System.Threading;
                   using System.Threading.Tasks;

                   public class TestClass
                   {
                       public async Task TestMethod()
                       {
                           try
                           {
                               await SomeOperationAsync();
                           }
                           catch (Exception ex) // Should trigger NP9103
                           {
                               Console.WriteLine(ex.Message);
                               // Missing re-throw for OperationCanceledException
                           }
                       }

                       private Task SomeOperationAsync() => Task.CompletedTask;
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == OperationCanceledExceptionAnalyzer.SwallowingOperationCanceledExceptionId);
        Assert.True(hasDiagnostic, "Analyzer should detect catch (Exception) without re-throw");
    }

    [Fact]
    public void ShouldDetectCatchingOperationCanceledExceptionWithoutReThrow()
    {
        var code = """
                   using System;
                   using System.Threading;
                   using System.Threading.Tasks;

                   public class TestClass
                   {
                       public async Task TestMethod()
                       {
                           try
                           {
                               await SomeOperationAsync();
                           }
                           catch (OperationCanceledException) // Should trigger NP9103
                           {
                               Console.WriteLine("Operation was cancelled");
                               // Missing re-throw
                           }
                       }

                       private Task SomeOperationAsync() => Task.CompletedTask;
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == OperationCanceledExceptionAnalyzer.SwallowingOperationCanceledExceptionId);
        Assert.True(hasDiagnostic, "Analyzer should detect catch (OperationCanceledException) without re-throw");
    }

    [Fact]
    public void ShouldNotDiagnoseWhenCatchingExceptionWithReThrow()
    {
        var code = """
                   using System;
                   using System.Threading;
                   using System.Threading.Tasks;

                   public class TestClass
                   {
                       public async Task TestMethod()
                       {
                           try
                           {
                               await SomeOperationAsync();
                           }
                           catch (Exception ex)
                           {
                               if (ex is OperationCanceledException)
                                   throw; // Proper re-throw
                               
                               Console.WriteLine(ex.Message);
                           }
                       }

                       private Task SomeOperationAsync() => Task.CompletedTask;
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == OperationCanceledExceptionAnalyzer.SwallowingOperationCanceledExceptionId);
        Assert.False(hasDiagnostic, "Analyzer should not diagnose when Exception is properly re-thrown");
    }

    [Fact]
    public void ShouldNotDiagnoseWhenCatchingOperationCanceledExceptionWithReThrow()
    {
        var code = """
                   using System;
                   using System.Threading;
                   using System.Threading.Tasks;

                   public class TestClass
                   {
                       public async Task TestMethod()
                       {
                           try
                           {
                               await SomeOperationAsync();
                           }
                           catch (OperationCanceledException)
                           {
                               Console.WriteLine("Operation was cancelled");
                               throw; // Proper re-throw
                           }
                       }

                       private Task SomeOperationAsync() => Task.CompletedTask;
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == OperationCanceledExceptionAnalyzer.SwallowingOperationCanceledExceptionId);
        Assert.False(hasDiagnostic, "Analyzer should not diagnose when OperationCanceledException is properly re-thrown");
    }

    [Fact]
    public void ShouldDetectCatchingAggregateExceptionWithoutReThrow()
    {
        var code = """
                   using System;
                   using System.Threading;
                   using System.Threading.Tasks;

                   public class TestClass
                   {
                       public async Task TestMethod()
                       {
                           try
                           {
                               await SomeOperationAsync();
                           }
                           catch (AggregateException aex) // Should trigger NP9103
                           {
                               Console.WriteLine(aex.Message);
                               // Missing check for inner OperationCanceledException
                           }
                       }

                       private Task SomeOperationAsync() => Task.CompletedTask;
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == OperationCanceledExceptionAnalyzer.SwallowingOperationCanceledExceptionId);
        Assert.True(hasDiagnostic, "Analyzer should detect catch (AggregateException) without checking for inner OperationCanceledException");
    }

    [Fact]
    public void ShouldNotDiagnoseWhenCatchingAggregateExceptionWithProperHandling()
    {
        var code = """
                   using System;
                   using System.Threading;
                   using System.Threading.Tasks;

                   public class TestClass
                   {
                       public async Task TestMethod()
                       {
                           try
                           {
                               await SomeOperationAsync();
                           }
                           catch (AggregateException aex)
                           {
                               // Check for inner OperationCanceledException
                               if (aex.InnerExceptions.Any(ex => ex is OperationCanceledException))
                                   throw;
                               
                               Console.WriteLine(aex.Message);
                           }
                       }

                       private Task SomeOperationAsync() => Task.CompletedTask;
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == OperationCanceledExceptionAnalyzer.SwallowingOperationCanceledExceptionId);
        Assert.False(hasDiagnostic, "Analyzer should not diagnose when AggregateException properly handles inner OperationCanceledException");
    }

    private static IEnumerable<Diagnostic> GetDiagnostics(string code)
    {
        var syntaxTree = CSharpSyntaxTree.ParseText(code);

        var references = new[]
        {
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Enumerable).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Task).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(OperationCanceledException).Assembly.Location),
        };

        var compilation = CSharpCompilation.Create("TestAssembly")
            .AddReferences(references)
            .AddSyntaxTrees(syntaxTree);

        var analyzer = new OperationCanceledExceptionAnalyzer();
        var compilation2 = compilation.WithAnalyzers(new[] { analyzer }.ToImmutableArray<DiagnosticAnalyzer>());
        var diagnostics = compilation2.GetAnalyzerDiagnosticsAsync().Result;

        return diagnostics;
    }
}

using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using Xunit;

namespace NPipeline.Analyzers.Tests;

/// <summary>
///     Tests for CancellationTokenRespectAnalyzer.
/// </summary>
public sealed class CancellationTokenRespectAnalyzerTests
{
    [Fact]
    public void ShouldDetectAsyncCallWithoutCancellationToken()
    {
        var code = """
                   using System;
                   using System.Threading;
                   using System.Threading.Tasks;

                   namespace TestNamespace
                   {
                       public class TestClass
                       {
                           public async Task ProcessAsync(CancellationToken cancellationToken)
                           {
                               await Task.Delay(1000); // Missing cancellationToken
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        // The analyzer should detect that Task.Delay is called without cancellationToken
        var hasDiagnostic = diagnostics.Any(d => d.Id == CancellationTokenRespectAnalyzer.CancellationTokenNotRespectedId);
        Assert.True(hasDiagnostic, "Analyzer should detect async call without cancellation token");
    }

    [Fact]
    public void ShouldDetectLoopWithoutCancellationCheck()
    {
        var code = """
                   using System;
                   using System.Threading;
                   using System.Threading.Tasks;

                   namespace TestNamespace
                   {
                       public class TestClass
                       {
                           public async Task ProcessAsync(CancellationToken cancellationToken)
                           {
                               for (int i = 0; i < 1000; i++)
                               {
                                   await Task.Delay(100, cancellationToken);
                                   // Missing cancellationToken.ThrowIfCancellationRequested();
                               }
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        // The analyzer should detect that the loop doesn't check for cancellation
        var hasDiagnostic = diagnostics.Any(d => d.Id == CancellationTokenRespectAnalyzer.CancellationTokenNotRespectedId);
        Assert.True(hasDiagnostic, "Analyzer should detect loop without cancellation check");
    }

    [Fact]
    public void ShouldDetectAsyncIteratorWithoutEnumeratorCancellation()
    {
        var code = """
                   using System;
                   using System.Collections.Generic;
                   using System.Runtime.CompilerServices;
                   using System.Threading;
                   using System.Threading.Tasks;

                   namespace TestNamespace
                   {
                       public class TestClass
                       {
                           public async IAsyncEnumerable<int> ProcessAsync(CancellationToken cancellationToken)
                           {
                               for (int i = 0; i < 1000; i++)
                               {
                                   await Task.Delay(100, cancellationToken);
                                   yield return i;
                               }
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        // The analyzer should detect that the async iterator doesn't have [EnumeratorCancellation] attribute
        var hasDiagnostic = diagnostics.Any(d => d.Id == CancellationTokenRespectAnalyzer.CancellationTokenNotRespectedId);
        Assert.True(hasDiagnostic, "Analyzer should detect async iterator without [EnumeratorCancellation] attribute");
    }

    [Fact]
    public void ShouldNotDetectWhenCancellationTokenIsRespected()
    {
        var code = """
                   using System;
                   using System.Collections.Generic;
                   using System.Runtime.CompilerServices;
                   using System.Threading;
                   using System.Threading.Tasks;

                   namespace TestNamespace
                   {
                       public class TestClass
                       {
                           public async IAsyncEnumerable<int> ProcessAsync([EnumeratorCancellation] CancellationToken cancellationToken)
                           {
                               for (int i = 0; i < 1000; i++)
                               {
                                   cancellationToken.ThrowIfCancellationRequested();
                                   await Task.Delay(100, cancellationToken);
                                   yield return i;
                               }
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        // The analyzer should not report any diagnostic
        var hasDiagnostic = diagnostics.Any(d => d.Id == CancellationTokenRespectAnalyzer.CancellationTokenNotRespectedId);
        Assert.False(hasDiagnostic, "Analyzer should not report when cancellation token is respected");
    }

    [Fact]
    public void ShouldNotDetectLoopWithCancellationCheck()
    {
        var code = """
                   using System;
                   using System.Threading;
                   using System.Threading.Tasks;

                   namespace TestNamespace
                   {
                       public class TestClass
                       {
                           public async Task ProcessWithCancellationCheckAsync(CancellationToken cancellationToken)
                           {
                               for (int i = 0; i < 1000; i++)
                               {
                                   cancellationToken.ThrowIfCancellationRequested();
                                   await Task.Delay(100, cancellationToken);
                               }
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        // Debug: Print all diagnostics
        Console.WriteLine($"Diagnostics count: {diagnostics.Count()}");

        foreach (var diagnostic in diagnostics)
        {
            Console.WriteLine($"Diagnostic: {diagnostic.Id} - {diagnostic.GetMessage()}");
        }

        // The analyzer should not report any diagnostic
        var hasDiagnostic = diagnostics.Any(d => d.Id == CancellationTokenRespectAnalyzer.CancellationTokenNotRespectedId);
        Assert.False(hasDiagnostic, "Analyzer should not report when loop has cancellation check");
    }

    [Fact]
    public void ShouldNotDetectAsyncIteratorWithEnumeratorCancellation()
    {
        var code = """
                   using System;
                   using System.Collections.Generic;
                   using System.Runtime.CompilerServices;
                   using System.Threading;
                   using System.Threading.Tasks;

                   namespace TestNamespace
                   {
                       public class TestClass
                       {
                           public async IAsyncEnumerable<int> ProcessAsync([EnumeratorCancellation] CancellationToken cancellationToken)
                           {
                               for (int i = 0; i < 1000; i++)
                               {
                                   await Task.Delay(100, cancellationToken);
                                   yield return i;
                               }
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        // Debug: Print all diagnostics
        Console.WriteLine($"Diagnostics count: {diagnostics.Count()}");

        foreach (var diagnostic in diagnostics)
        {
            Console.WriteLine($"Diagnostic: {diagnostic.Id} - {diagnostic.GetMessage()}");
        }

        // The analyzer should not report any diagnostic
        var hasDiagnostic = diagnostics.Any(d => d.Id == CancellationTokenRespectAnalyzer.CancellationTokenNotRespectedId);
        Assert.False(hasDiagnostic, "Analyzer should not report when async iterator has [EnumeratorCancellation] attribute");
    }

    private static IEnumerable<Diagnostic> GetDiagnostics(string code)
    {
        var syntaxTree = CSharpSyntaxTree.ParseText(code);

        var references = new[]
        {
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Enumerable).Assembly.Location),
        };

        var compilation = CSharpCompilation.Create("TestAssembly")
            .AddReferences(references)
            .AddSyntaxTrees(syntaxTree);

        var analyzer = new CancellationTokenRespectAnalyzer();
        var compilation2 = compilation.WithAnalyzers(new[] { analyzer }.ToImmutableArray<DiagnosticAnalyzer>());
        var diagnostics = compilation2.GetAnalyzerDiagnosticsAsync().Result;

        return diagnostics;
    }
}

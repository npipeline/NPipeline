using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers.Tests;

/// <summary>
///     Tests for the BlockingAsyncOperationAnalyzer.
/// </summary>
public sealed class BlockingAsyncOperationAnalyzerTests
{
    [Fact]
    public void ShouldDetectResultCallInAsyncMethod()
    {
        var code = """
                   using System.Threading.Tasks;

                   public class TestClass
                   {
                       public async Task<string> TestMethod()
                       {
                           var task = Task.FromResult("test");
                           return task.Result; // Should trigger diagnostic
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BlockingAsyncOperationAnalyzer.BlockingAsyncOperationId);
        Assert.True(hasDiagnostic, "Analyzer should detect Task.Result call in async method");
    }

    [Fact]
    public void ShouldDetectWaitCallInAsyncMethod()
    {
        var code = """
                   using System.Threading.Tasks;

                   public class TestClass
                   {
                       public async Task TestMethod()
                       {
                           var task = Task.Delay(100);
                           task.Wait(); // Should trigger diagnostic
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BlockingAsyncOperationAnalyzer.BlockingAsyncOperationId);
        Assert.True(hasDiagnostic, "Analyzer should detect Task.Wait() call in async method");
    }

    [Fact]
    public void ShouldDetectGetAwaiterGetResultPattern()
    {
        var code = """
                   using System.Threading.Tasks;

                   public class TestClass
                   {
                       public async Task<string> TestMethod()
                       {
                           var task = Task.FromResult("test");
                           return task.GetAwaiter().GetResult(); // Should trigger diagnostic
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BlockingAsyncOperationAnalyzer.BlockingAsyncOperationId);
        Assert.True(hasDiagnostic, "Analyzer should detect GetAwaiter().GetResult() pattern in async method");
    }

    [Fact]
    public void ShouldDetectThreadSleepInAsyncMethod()
    {
        var code = """
                   using System.Threading;
                   using System.Threading.Tasks;

                   public class TestClass
                   {
                       public async Task TestMethod()
                       {
                           Thread.Sleep(1000); // Should trigger diagnostic
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BlockingAsyncOperationAnalyzer.BlockingAsyncOperationId);
        Assert.True(hasDiagnostic, "Analyzer should detect Thread.Sleep() call in async method");
    }

    [Fact]
    public void ShouldDetectSynchronousFileIOOperations()
    {
        var code = """
                   using System.IO;
                   using System.Threading.Tasks;

                   public class TestClass
                   {
                       public async Task<string> TestMethod()
                       {
                           return File.ReadAllText("test.txt"); // Should trigger diagnostic
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BlockingAsyncOperationAnalyzer.BlockingAsyncOperationId);
        Assert.True(hasDiagnostic, "Analyzer should detect File.ReadAllText() call in async method");
    }

    [Fact]
    public void ShouldDetectMultipleFileIOOperations()
    {
        var code = """
                   using System.IO;
                   using System.Threading.Tasks;

                   public class TestClass
                   {
                       public async Task TestMethod()
                       {
                           File.WriteAllText("test.txt", "content"); // Should trigger diagnostic
                           File.ReadAllBytes("test.bin"); // Should trigger diagnostic
                           File.AppendAllText("log.txt", "message"); // Should trigger diagnostic
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var blockingDiagnostics = diagnostics.Where(d => d.Id == BlockingAsyncOperationAnalyzer.BlockingAsyncOperationId).ToList();
        Assert.True(blockingDiagnostics.Count >= 3, "Analyzer should detect multiple File I/O blocking calls");
    }

    [Fact]
    public void ShouldDetectSynchronousNetworkIOOperations()
    {
        var code = """
                   using System.Net.Http;
                   using System.Threading.Tasks;

                   public class TestClass
                   {
                       public async Task TestMethod()
                       {
                           var client = new HttpClient();
                           var response = client.GetAsync("https://example.com"); // Should trigger diagnostic (not awaited)
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BlockingAsyncOperationAnalyzer.BlockingAsyncOperationId);
        Assert.True(hasDiagnostic, "Analyzer should detect non-awaited HttpClient.GetAsync() call in async method");
    }

    [Fact]
    public void ShouldDetectWebClientBlockingOperations()
    {
        var code = """
                   using System.Net;
                   using System.Threading.Tasks;

                   public class TestClass
                   {
                       public async Task<string> TestMethod()
                       {
                           var client = new WebClient();
                           return client.DownloadString("https://example.com"); // Should trigger diagnostic
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BlockingAsyncOperationAnalyzer.BlockingAsyncOperationId);
        Assert.True(hasDiagnostic, "Analyzer should detect WebClient.DownloadString() call in async method");
    }

    [Fact]
    public void ShouldDetectStreamReaderBlockingOperations()
    {
        var code = """
                   using System.IO;
                   using System.Threading.Tasks;

                   public class TestClass
                   {
                       public async Task<string> TestMethod()
                       {
                           using var reader = new StreamReader("test.txt");
                           return reader.ReadToEnd(); // Should trigger diagnostic (not awaited)
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BlockingAsyncOperationAnalyzer.BlockingAsyncOperationId);
        Assert.True(hasDiagnostic, "Analyzer should detect StreamReader.ReadToEnd() call in async method");
    }

    [Fact]
    public void ShouldNotAnalyzeNonAsyncMethods()
    {
        var code = """
                   using System.Threading.Tasks;

                   public class TestClass
                   {
                       public string TestMethod()
                       {
                           var task = Task.FromResult("test");
                           return task.Result; // Should NOT trigger diagnostic (method is not async)
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BlockingAsyncOperationAnalyzer.BlockingAsyncOperationId);
        Assert.False(hasDiagnostic, "Analyzer should not analyze non-async methods");
    }

    [Fact]
    public void ShouldNotTriggerForAsyncMethodsWithoutBlockingPatterns()
    {
        var code = """
                   using System.Threading.Tasks;

                   public class TestClass
                   {
                       public async Task<string> TestMethod()
                       {
                           var task = Task.FromResult("test");
                           return await task; // Should NOT trigger diagnostic (proper await)
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BlockingAsyncOperationAnalyzer.BlockingAsyncOperationId);
        Assert.False(hasDiagnostic, "Analyzer should not trigger for proper async patterns");
    }

    [Fact]
    public void ShouldNotTriggerForAwaitedNetworkOperations()
    {
        var code = """
                   using System.Net.Http;
                   using System.Threading.Tasks;

                   public class TestClass
                   {
                       public async Task<string> TestMethod()
                       {
                           var client = new HttpClient();
                           var response = await client.GetAsync("https://example.com"); // Should NOT trigger diagnostic (awaited)
                           return await response.Content.ReadAsStringAsync();
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BlockingAsyncOperationAnalyzer.BlockingAsyncOperationId);
        Assert.False(hasDiagnostic, "Analyzer should not trigger for awaited network operations");
    }

    [Fact]
    public void ShouldDetectBlockingInTaskReturningMethods()
    {
        var code = """
                   using System.Threading.Tasks;

                   public class TestClass
                   {
                       public Task<string> TestMethod() // No async keyword but returns Task
                       {
                           var task = Task.FromResult("test");
                           return Task.FromResult(task.Result); // Should trigger diagnostic
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BlockingAsyncOperationAnalyzer.BlockingAsyncOperationId);
        Assert.True(hasDiagnostic, "Analyzer should detect blocking in Task-returning methods");
    }

    [Fact]
    public void ShouldDetectBlockingInValueTaskReturningMethods()
    {
        var code = """
                   using System.Threading.Tasks;

                   public class TestClass
                   {
                       public ValueTask<string> TestMethod() // ValueTask return type
                       {
                           var task = Task.FromResult("test");
                           return new ValueTask<string>(task.Result); // Should trigger diagnostic
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BlockingAsyncOperationAnalyzer.BlockingAsyncOperationId);
        Assert.True(hasDiagnostic, "Analyzer should detect blocking in ValueTask-returning methods");
    }

    [Fact]
    public void ShouldDetectBlockingInComplexExpressions()
    {
        var code = """
                   using System.Threading.Tasks;

                   public class TestClass
                   {
                       public async Task<string> TestMethod()
                       {
                           var task1 = Task.FromResult("hello");
                           var task2 = Task.FromResult("world");
                           return (task1.Result + " " + task2.Result).ToUpper(); // Should trigger diagnostic
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var blockingDiagnostics = diagnostics.Where(d => d.Id == BlockingAsyncOperationAnalyzer.BlockingAsyncOperationId).ToList();
        Assert.True(blockingDiagnostics.Count >= 2, "Analyzer should detect multiple blocking calls in complex expressions");
    }

    [Fact]
    public void ShouldDetectBlockingInConditionalExpressions()
    {
        var code = """
                   using System.Threading.Tasks;

                   public class TestClass
                   {
                       public async Task<string> TestMethod(bool useFirst)
                       {
                           var task1 = Task.FromResult("first");
                           var task2 = Task.FromResult("second");
                           return useFirst ? task1.Result : task2.Result; // Should trigger diagnostic
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var blockingDiagnostics = diagnostics.Where(d => d.Id == BlockingAsyncOperationAnalyzer.BlockingAsyncOperationId).ToList();
        Assert.True(blockingDiagnostics.Count >= 2, "Analyzer should detect blocking calls in conditional expressions");
    }

    [Fact]
    public void ShouldDetectBlockingInSwitchExpressions()
    {
        var code = """
                   using System.Threading.Tasks;

                   public class TestClass
                   {
                       public async Task<string> TestMethod(int option)
                       {
                           var task1 = Task.FromResult("first");
                           var task2 = Task.FromResult("second");
                           return option switch
                           {
                               1 => task1.Result, // Should trigger diagnostic
                               2 => task2.Result, // Should trigger diagnostic
                               _ => "default"
                           };
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var blockingDiagnostics = diagnostics.Where(d => d.Id == BlockingAsyncOperationAnalyzer.BlockingAsyncOperationId).ToList();
        Assert.True(blockingDiagnostics.Count >= 2, "Analyzer should detect blocking calls in switch expressions");
    }

    [Fact]
    public void ShouldDetectBlockingInLambdaExpressions()
    {
        var code = """
                   using System;
                   using System.Threading.Tasks;

                   public class TestClass
                   {
                       public async Task<string> TestMethod()
                       {
                           Func<string> lambda = () =>
                           {
                               var task = Task.FromResult("test");
                               return task.Result; // Should trigger diagnostic
                           };
                           return await Task.FromResult(lambda());
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BlockingAsyncOperationAnalyzer.BlockingAsyncOperationId);
        Assert.True(hasDiagnostic, "Analyzer should detect blocking calls in lambda expressions");
    }

    [Fact]
    public void ShouldDetectBlockingInLocalFunctions()
    {
        var code = """
                   using System.Threading.Tasks;

                   public class TestClass
                   {
                       public async Task<string> TestMethod()
                       {
                           string LocalFunction()
                           {
                               var task = Task.FromResult("test");
                               return task.Result; // Should trigger diagnostic
                           }
                           return await Task.FromResult(LocalFunction());
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BlockingAsyncOperationAnalyzer.BlockingAsyncOperationId);
        Assert.True(hasDiagnostic, "Analyzer should detect blocking calls in local functions");
    }

    private static IEnumerable<Diagnostic> GetDiagnostics(string code)
    {
        var syntaxTree = CSharpSyntaxTree.ParseText(code);

        var references = new[]
        {
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Enumerable).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Task).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(HttpClient).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(File).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Thread).Assembly.Location),
        };

        var compilation = CSharpCompilation.Create("TestAssembly")
            .AddReferences(references)
            .AddSyntaxTrees(syntaxTree);

        var analyzer = new BlockingAsyncOperationAnalyzer();
        var compilation2 = compilation.WithAnalyzers(new[] { analyzer }.ToImmutableArray<DiagnosticAnalyzer>());
        var diagnostics = compilation2.GetAnalyzerDiagnosticsAsync().Result;

        return diagnostics;
    }
}

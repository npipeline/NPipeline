using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using NPipeline.Nodes;

namespace NPipeline.Analyzers.Tests;

/// <summary>
///     Tests for BatchingConfigurationMismatchAnalyzer.
/// </summary>
public sealed class BatchingConfigurationMismatchAnalyzerTests
{
    [Fact]
    public void ShouldDetectLargeBatchSizeWithShortTimeout()
    {
        var code = """
                   using NPipeline.Configuration;

                   namespace TestNamespace
                   {
                       public class TestClass
                       {
                           public void TestMethod()
                           {
                               var options = new BatchingOptions
                               {
                                   BatchSize = 1000,
                                   Timeout = System.TimeSpan.FromMilliseconds(100)
                               };
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BatchingConfigurationMismatchAnalyzer.DiagnosticId);
        Assert.True(hasDiagnostic, "Analyzer should detect large batch size with short timeout");
    }

    [Fact]
    public void ShouldDetectSmallBatchSizeWithLongTimeout()
    {
        var code = """
                   using NPipeline.Configuration;

                   namespace TestNamespace
                   {
                       public class TestClass
                       {
                           public void TestMethod()
                           {
                               var options = new BatchingOptions
                               {
                                   BatchSize = 5,
                                   Timeout = System.TimeSpan.FromSeconds(30)
                               };
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BatchingConfigurationMismatchAnalyzer.DiagnosticId);
        Assert.True(hasDiagnostic, "Analyzer should detect small batch size with long timeout");
    }

    [Fact]
    public void ShouldDetectMediumBatchSizeWithShortTimeout()
    {
        var code = """
                   using NPipeline.Configuration;

                   namespace TestNamespace
                   {
                       public class TestClass
                       {
                           public void TestMethod()
                           {
                               var options = new BatchingOptions
                               {
                                   BatchSize = 50,
                                   Timeout = System.TimeSpan.FromMilliseconds(200)
                               };
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BatchingConfigurationMismatchAnalyzer.DiagnosticId);
        Assert.True(hasDiagnostic, "Analyzer should detect medium batch size with short timeout");
    }

    [Fact]
    public void ShouldDetectMediumBatchSizeWithLongTimeout()
    {
        var code = """
                   using NPipeline.Configuration;

                   namespace TestNamespace
                   {
                       public class TestClass
                       {
                           public void TestMethod()
                           {
                               var options = new BatchingOptions
                               {
                                   BatchSize = 50,
                                   Timeout = System.TimeSpan.FromSeconds(10)
                               };
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BatchingConfigurationMismatchAnalyzer.DiagnosticId);
        Assert.True(hasDiagnostic, "Analyzer should detect medium batch size with long timeout");
    }

    [Fact]
    public void ShouldNotDetectBalancedConfiguration()
    {
        var code = """
                   using NPipeline.Configuration;

                   namespace TestNamespace
                   {
                       public class TestClass
                       {
                           public void TestMethod()
                           {
                               var options = new BatchingOptions
                               {
                                   BatchSize = 50,
                                   Timeout = System.TimeSpan.FromSeconds(2)
                               };
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BatchingConfigurationMismatchAnalyzer.DiagnosticId);
        Assert.False(hasDiagnostic, "Analyzer should not detect balanced configuration");
    }

    [Fact]
    public void ShouldDetectBatchingStrategyLargeBatchSizeWithShortTimeout()
    {
        var code = """
                   using NPipeline.Configuration;

                   namespace TestNamespace
                   {
                       public class TestClass
                       {
                           public void TestMethod()
                           {
                               var strategy = new BatchingStrategy(
                                   batchSize: 1000,
                                   maxWaitTime: System.TimeSpan.FromMilliseconds(100)
                               );
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BatchingConfigurationMismatchAnalyzer.DiagnosticId);
        Assert.True(hasDiagnostic, "Analyzer should detect BatchingStrategy large batch size with short timeout");
    }

    [Fact]
    public void ShouldDetectBatchingStrategySmallBatchSizeWithLongTimeout()
    {
        var code = """
                   using NPipeline.Configuration;

                   namespace TestNamespace
                   {
                       public class TestClass
                       {
                           public void TestMethod()
                           {
                               var strategy = new BatchingStrategy(
                                   batchSize: 5,
                                   maxWaitTime: System.TimeSpan.FromSeconds(30)
                               );
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BatchingConfigurationMismatchAnalyzer.DiagnosticId);
        Assert.True(hasDiagnostic, "Analyzer should detect BatchingStrategy small batch size with long timeout");
    }

    [Fact]
    public void ShouldDetectBatchingStrategyMediumBatchSizeWithShortTimeout()
    {
        var code = """
                   using NPipeline.Configuration;

                   namespace TestNamespace
                   {
                       public class TestClass
                       {
                           public void TestMethod()
                           {
                               var strategy = new BatchingStrategy(
                                   batchSize: 50,
                                   maxWaitTime: System.TimeSpan.FromMilliseconds(200)
                               );
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BatchingConfigurationMismatchAnalyzer.DiagnosticId);
        Assert.True(hasDiagnostic, "Analyzer should detect BatchingStrategy medium batch size with short timeout");
    }

    [Fact]
    public void ShouldNotDetectBalancedBatchingStrategyConfiguration()
    {
        var code = """
                   using NPipeline.Configuration;

                   namespace TestNamespace
                   {
                       public class TestClass
                       {
                           public void TestMethod()
                           {
                               var strategy = new BatchingStrategy(
                                   batchSize: 50,
                                   maxWaitTime: System.TimeSpan.FromSeconds(2)
                               );
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BatchingConfigurationMismatchAnalyzer.DiagnosticId);
        Assert.False(hasDiagnostic, "Analyzer should not detect balanced BatchingStrategy configuration");
    }

    [Fact]
    public void ShouldDetectWithBatchingLargeBatchSizeWithShortTimeout()
    {
        var code = """
                   using NPipeline;
                   using NPipeline.Configuration;

                   namespace TestNamespace
                   {
                       public class TestClass
                       {
                           public void TestMethod()
                           {
                               var builder = new PipelineBuilder();
                               builder.AddBatchingTransform<object, object, object>("test")
                                   .WithBatching(new BatchingOptions
                                   {
                                       BatchSize = 1000,
                                       Timeout = System.TimeSpan.FromMilliseconds(100)
                                   });
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BatchingConfigurationMismatchAnalyzer.DiagnosticId);
        Assert.True(hasDiagnostic, "Analyzer should detect WithBatching large batch size with short timeout");
    }

    [Fact]
    public void ShouldDetectWithBatchingSmallBatchSizeWithLongTimeout()
    {
        var code = """
                   using NPipeline;
                   using NPipeline.Configuration;

                   namespace TestNamespace
                   {
                       public class TestClass
                       {
                           public void TestMethod()
                           {
                               var builder = new PipelineBuilder();
                               builder.AddBatchingTransform<object, object, object>("test")
                                   .WithBatching(new BatchingOptions
                                   {
                                       BatchSize = 5,
                                       Timeout = System.TimeSpan.FromSeconds(30)
                                   });
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BatchingConfigurationMismatchAnalyzer.DiagnosticId);
        Assert.True(hasDiagnostic, "Analyzer should detect WithBatching small batch size with long timeout");
    }

    [Fact]
    public void ShouldNotDetectWithBatchingBalancedConfiguration()
    {
        var code = """
                   using NPipeline;
                   using NPipeline.Configuration;

                   namespace TestNamespace
                   {
                       public class TestClass
                       {
                           public void TestMethod()
                           {
                               var builder = new PipelineBuilder();
                               builder.AddBatchingTransform<object, object, object>("test")
                                   .WithBatching(new BatchingOptions
                                   {
                                       BatchSize = 50,
                                       Timeout = System.TimeSpan.FromSeconds(2)
                                   });
                           }
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == BatchingConfigurationMismatchAnalyzer.DiagnosticId);
        Assert.False(hasDiagnostic, "Analyzer should not detect WithBatching balanced configuration");
    }

    private static IEnumerable<Diagnostic> GetDiagnostics(string code)
    {
        var syntaxTree = CSharpSyntaxTree.ParseText(code);

        // Get path to NPipeline assembly
        var nPipelineAssemblyPath = typeof(INode).Assembly.Location;

        var references = new[]
        {
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(TimeSpan).Assembly.Location),
            MetadataReference.CreateFromFile(nPipelineAssemblyPath),
        };

        var compilation = CSharpCompilation.Create("TestAssembly")
            .AddReferences(references)
            .AddSyntaxTrees(syntaxTree);

        var analyzer = new BatchingConfigurationMismatchAnalyzer();
        var compilationWithAnalyzers = compilation.WithAnalyzers([analyzer]);
        var diagnostics = compilationWithAnalyzers.GetAnalyzerDiagnosticsAsync().Result;

        return diagnostics;
    }
}

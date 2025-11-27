using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using NPipeline.Configuration;

namespace NPipeline.Analyzers.Tests;

/// <summary>
///     Tests for UnboundedMaterializationConfigurationAnalyzer.
/// </summary>
public sealed class UnboundedMaterializationConfigurationAnalyzerTests
{
    [Fact]
    public void ShouldDetectNullMaxMaterializedItems()
    {
        var code = """
                   using NPipeline.Configuration;

                   public class TestClass
                   {
                       public void TestMethod()
                       {
                           var options = new PipelineRetryOptions(
                               MaxItemRetries: 3,
                               MaxNodeRestartAttempts: 2,
                               MaxSequentialNodeAttempts: 5,
                               MaxMaterializedItems: null); // NP9501: Explicitly null
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == UnboundedMaterializationConfigurationAnalyzer.DiagnosticId);
        Assert.True(hasDiagnostic, "Analyzer should detect MaxMaterializedItems explicitly set to null");
    }

    [Fact]
    public void ShouldDetectMissingMaxMaterializedItems()
    {
        var code = """
                   using NPipeline.Configuration;

                   public class TestClass
                   {
                       public void TestMethod()
                       {
                           var options = new PipelineRetryOptions(
                               MaxItemRetries: 3,
                               MaxNodeRestartAttempts: 2,
                               MaxSequentialNodeAttempts: 5); // NP9501: Missing MaxMaterializedItems
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == UnboundedMaterializationConfigurationAnalyzer.DiagnosticId);
        Assert.True(hasDiagnostic, "Analyzer should detect missing MaxMaterializedItems parameter");
    }

    [Fact]
    public void ShouldDetectMissingMaxMaterializedItemsWithPositionalArguments()
    {
        var code = """
                   using NPipeline.Configuration;

                   public class TestClass
                   {
                       public void TestMethod()
                       {
                           var options = new PipelineRetryOptions(3, 2, 5); // NP9501: Missing 4th parameter
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == UnboundedMaterializationConfigurationAnalyzer.DiagnosticId);
        Assert.True(hasDiagnostic, "Analyzer should detect missing MaxMaterializedItems in positional arguments");
    }

    [Fact]
    public void ShouldIgnoreValidMaxMaterializedItems()
    {
        var code = """
                   using NPipeline.Configuration;

                   public class TestClass
                   {
                       public void TestMethod()
                       {
                           var options = new PipelineRetryOptions(
                               MaxItemRetries: 3,
                               MaxNodeRestartAttempts: 2,
                               MaxSequentialNodeAttempts: 5,
                               MaxMaterializedItems: 1000); // Valid: non-null value
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == UnboundedMaterializationConfigurationAnalyzer.DiagnosticId);
        Assert.False(hasDiagnostic, "Analyzer should not report when MaxMaterializedItems has valid value");
    }

    [Fact]
    public void ShouldIgnoreValidMaxMaterializedItemsWithPositionalArguments()
    {
        var code = """
                   using NPipeline.Configuration;

                   public class TestClass
                   {
                       public void TestMethod()
                       {
                           var options = new PipelineRetryOptions(3, 2, 5, 1000); // Valid: 4th parameter provided
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == UnboundedMaterializationConfigurationAnalyzer.DiagnosticId);
        Assert.False(hasDiagnostic, "Analyzer should not report when MaxMaterializedItems is provided positionally");
    }

    [Fact]
    public void ShouldIgnoreOtherObjectCreation()
    {
        var code = """
                   using System;

                   public class TestClass
                   {
                       public void TestMethod()
                       {
                           var options = new OtherOptions(
                               MaxItems: null); // Should not be analyzed
                       }
                   }

                   public class OtherOptions
                   {
                       public OtherOptions(int? MaxItems) { }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == UnboundedMaterializationConfigurationAnalyzer.DiagnosticId);
        Assert.False(hasDiagnostic, "Analyzer should not analyze non-PipelineRetryOptions objects");
    }

    [Fact]
    public void ShouldDetectMaxMaterializedItemsAsFourthPositionalArgument()
    {
        var code = """
                   using NPipeline.Configuration;

                   public class TestClass
                   {
                       public void TestMethod()
                       {
                           var options = new PipelineRetryOptions(3, 2, 5, null); // NP9501: 4th positional arg is null
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == UnboundedMaterializationConfigurationAnalyzer.DiagnosticId);
        Assert.True(hasDiagnostic, "Analyzer should detect null in 4th positional argument");
    }

    [Fact]
    public void ShouldIgnoreNamedArgumentsAfterMaxMaterializedItems()
    {
        var code = """
                   using NPipeline.Configuration;
                   using NPipeline.Configuration.RetryDelay;

                   public class TestClass
                   {
                       public void TestMethod()
                       {
                           var delayConfig = new FixedDelayConfiguration(TimeSpan.FromSeconds(1));
                           var options = new PipelineRetryOptions(
                               MaxItemRetries: 3,
                               MaxNodeRestartAttempts: 2,
                               MaxSequentialNodeAttempts: 5,
                               MaxMaterializedItems: 1000,
                               DelayStrategyConfiguration: delayConfig); // Additional named arg should be ignored
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == UnboundedMaterializationConfigurationAnalyzer.DiagnosticId);
        Assert.False(hasDiagnostic, "Analyzer should ignore additional named arguments after MaxMaterializedItems");
    }

    [Fact]
    public void ShouldHandleMixedNamedAndPositionalArguments()
    {
        var code = """
                   using NPipeline.Configuration;

                   public class TestClass
                   {
                       public void TestMethod()
                       {
                           // Mixed: first two positional, then named MaxMaterializedItems
                           var options1 = new PipelineRetryOptions(3, 2, MaxMaterializedItems: null); // NP9501
                           
                           // Mixed: positional with named MaxMaterializedItems
                           var options2 = new PipelineRetryOptions(3, 2, 5, MaxMaterializedItems: null); // NP9501
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == UnboundedMaterializationConfigurationAnalyzer.DiagnosticId);
        Assert.True(hasDiagnostic, "Analyzer should detect null MaxMaterializedItems in mixed argument styles");
    }

    [Fact]
    public void ShouldHandleDefaultOptions()
    {
        var code = """
                   using NPipeline.Configuration;

                   public class TestClass
                   {
                       public void TestMethod()
                       {
                           var options = PipelineRetryOptions.Default; // Should not be analyzed (no constructor call)
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == UnboundedMaterializationConfigurationAnalyzer.DiagnosticId);
        Assert.False(hasDiagnostic, "Analyzer should not analyze static property access");
    }

    private static IEnumerable<Diagnostic> GetDiagnostics(string code)
    {
        var syntaxTree = CSharpSyntaxTree.ParseText(code);

        // Get path to NPipeline assembly
        var nPipelineAssemblyPath = typeof(PipelineRetryOptions).Assembly.Location;

        var references = new[]
        {
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Enumerable).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Task).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(TimeSpan).Assembly.Location),
            MetadataReference.CreateFromFile(nPipelineAssemblyPath),
        };

        var compilation = CSharpCompilation.Create("TestAssembly")
            .AddReferences(references)
            .AddSyntaxTrees(syntaxTree);

        var analyzer = new UnboundedMaterializationConfigurationAnalyzer();
        var compilationWithAnalyzers = compilation.WithAnalyzers([analyzer]);
        var diagnostics = compilationWithAnalyzers.GetAnalyzerDiagnosticsAsync().Result;

        return diagnostics;
    }
}

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using NPipeline.Reliability;

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
                               MaxMaterializedItems: null); // NP9002: Explicitly null
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == UnboundedMaterializationConfigurationAnalyzer.UnboundedMaterializationConfigurationId);
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
                               MaxItemRetries: 3); // NP9002: Missing MaxMaterializedItems
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == UnboundedMaterializationConfigurationAnalyzer.UnboundedMaterializationConfigurationId);
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
                           var options = new PipelineRetryOptions(3); // NP9002: Missing 2nd parameter (MaxMaterializedItems)
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == UnboundedMaterializationConfigurationAnalyzer.UnboundedMaterializationConfigurationId);
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
                               MaxMaterializedItems: 1000); // Valid: non-null value
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == UnboundedMaterializationConfigurationAnalyzer.UnboundedMaterializationConfigurationId);
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
                           var options = new PipelineRetryOptions(3, 1000); // Valid: 2nd parameter (MaxMaterializedItems) provided
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == UnboundedMaterializationConfigurationAnalyzer.UnboundedMaterializationConfigurationId);
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

        var hasDiagnostic = diagnostics.Any(d => d.Id == UnboundedMaterializationConfigurationAnalyzer.UnboundedMaterializationConfigurationId);
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
                           var options = new PipelineRetryOptions(3, null); // NP9002: 2nd positional arg is null
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == UnboundedMaterializationConfigurationAnalyzer.UnboundedMaterializationConfigurationId);
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
                               MaxMaterializedItems: 1000,
                               DelayStrategyConfiguration: delayConfig); // Additional named arg should be ignored
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == UnboundedMaterializationConfigurationAnalyzer.UnboundedMaterializationConfigurationId);
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
                           // Positional first arg, then named MaxMaterializedItems
                           var options1 = new PipelineRetryOptions(3, MaxMaterializedItems: null); // NP9002
                           
                           // All named with MaxMaterializedItems null
                           var options2 = new PipelineRetryOptions(MaxItemRetries: 3, MaxMaterializedItems: null); // NP9002
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == UnboundedMaterializationConfigurationAnalyzer.UnboundedMaterializationConfigurationId);
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

        var hasDiagnostic = diagnostics.Any(d => d.Id == UnboundedMaterializationConfigurationAnalyzer.UnboundedMaterializationConfigurationId);
        Assert.False(hasDiagnostic, "Analyzer should not analyze static property access");
    }

    /// <summary>
    ///     Stand-in for <c>NPipeline.Configuration.PipelineRetryOptions</c>, which the resilience redesign deleted from
    ///     NPipeline. The analyzer resolves the type semantically, so the test compilation declares it with the shape it
    ///     had until the analyzer is rewritten.
    /// </summary>
    private const string DeletedPipelineRetryOptionsStub = """
                                                           using System;

                                                           namespace NPipeline.Configuration.RetryDelay
                                                           {
                                                               public abstract record RetryDelayStrategyConfiguration;

                                                               public sealed record FixedDelayConfiguration(TimeSpan Delay) : RetryDelayStrategyConfiguration;
                                                           }

                                                           namespace NPipeline.Configuration
                                                           {
                                                               using NPipeline.Configuration.RetryDelay;

                                                               public sealed record PipelineRetryOptions(
                                                                   int MaxItemRetries = 0,
                                                                   int? MaxMaterializedItems = null,
                                                                   RetryDelayStrategyConfiguration? DelayStrategyConfiguration = null,
                                                                   int MaxNodeRestartAttempts = 3,
                                                                   int MaxSequentialNodeAttempts = 5)
                                                               {
                                                                   public static PipelineRetryOptions Default { get; } = new();
                                                               }
                                                           }
                                                           """;

    private static IEnumerable<Diagnostic> GetDiagnostics(string code)
    {
        var syntaxTree = CSharpSyntaxTree.ParseText(code);
        var stubSyntaxTree = CSharpSyntaxTree.ParseText(DeletedPipelineRetryOptionsStub);

        // Get path to NPipeline assembly
        var nPipelineAssemblyPath = typeof(PipelineResilienceOptions).Assembly.Location;

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
            .AddSyntaxTrees(syntaxTree, stubSyntaxTree);

        var analyzer = new UnboundedMaterializationConfigurationAnalyzer();
        var compilationWithAnalyzers = compilation.WithAnalyzers([analyzer]);
        var diagnostics = compilationWithAnalyzers.GetAnalyzerDiagnosticsAsync().Result;

        return diagnostics;
    }
}

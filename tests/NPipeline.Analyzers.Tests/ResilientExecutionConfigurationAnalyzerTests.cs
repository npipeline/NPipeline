using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using NPipeline.Pipeline;
using Xunit;

namespace NPipeline.Analyzers.Tests;

/// <summary>
///     Tests for the ResilientExecutionConfigurationAnalyzer.
/// </summary>
public sealed class ResilientExecutionConfigurationAnalyzerTests
{
    [Fact]
    public void ShouldDetectRestartNodeInErrorHandler()
    {
        var code = """
                   using NPipeline.ErrorHandling;
                   using NPipeline.Pipeline;
                   using System.Threading;
                   using System.Threading.Tasks;

                   public class MyErrorHandler : IPipelineErrorHandler
                   {
                       public async Task<PipelineErrorDecision> HandleNodeFailureAsync(
                           string nodeId,
                           Exception error,
                           PipelineContext context,
                           CancellationToken cancellationToken)
                       {
                           return PipelineErrorDecision.RestartNode;
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        // The analyzer should detect that RestartNode is being returned
        var hasDiagnostic = diagnostics.Any(d => d.Id == ResilientExecutionConfigurationAnalyzer.IncompleteResilientConfigurationId);
        Assert.True(hasDiagnostic, "Analyzer should detect RestartNode return in error handler");
    }

    [Fact]
    public void ShouldIgnoreFailPipelineDecision()
    {
        var code = """
                   using NPipeline.ErrorHandling;
                   using NPipeline.Pipeline;
                   using System.Threading;
                   using System.Threading.Tasks;

                   public class MyErrorHandler : IPipelineErrorHandler
                   {
                       public async Task<PipelineErrorDecision> HandleNodeFailureAsync(
                           string nodeId,
                           Exception error,
                           PipelineContext context,
                           CancellationToken cancellationToken)
                       {
                           return PipelineErrorDecision.FailPipeline;
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        // The analyzer should NOT report a diagnostic for FailPipeline
        var hasDiagnostic = diagnostics.Any(d => d.Id == ResilientExecutionConfigurationAnalyzer.IncompleteResilientConfigurationId);
        Assert.False(hasDiagnostic, "Analyzer should not report for FailPipeline decision");
    }

    [Fact]
    public void ShouldIgnoreContinueWithoutNodeDecision()
    {
        var code = """
                   using NPipeline.ErrorHandling;
                   using NPipeline.Pipeline;
                   using System.Threading;
                   using System.Threading.Tasks;

                   public class MyErrorHandler : IPipelineErrorHandler
                   {
                       public async Task<PipelineErrorDecision> HandleNodeFailureAsync(
                           string nodeId,
                           Exception error,
                           PipelineContext context,
                           CancellationToken cancellationToken)
                       {
                           return PipelineErrorDecision.ContinueWithoutNode;
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        // The analyzer should NOT report a diagnostic for ContinueWithoutNode
        var hasDiagnostic = diagnostics.Any(d => d.Id == ResilientExecutionConfigurationAnalyzer.IncompleteResilientConfigurationId);
        Assert.False(hasDiagnostic, "Analyzer should not report for ContinueWithoutNode decision");
    }

    [Fact]
    public void ShouldDetectRestartNodeInSwitchExpression()
    {
        var code = """
                   using NPipeline.ErrorHandling;
                   using NPipeline.Pipeline;
                   using System.Threading;
                   using System.Threading.Tasks;

                   public class MyErrorHandler : IPipelineErrorHandler
                   {
                       public async Task<PipelineErrorDecision> HandleNodeFailureAsync(
                           string nodeId,
                           Exception error,
                           PipelineContext context,
                           CancellationToken cancellationToken)
                       {
                           return error switch
                           {
                               TimeoutException => PipelineErrorDecision.RestartNode,
                               _ => PipelineErrorDecision.FailPipeline
                           };
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == ResilientExecutionConfigurationAnalyzer.IncompleteResilientConfigurationId);
        Assert.True(hasDiagnostic, "Analyzer should detect RestartNode in switch expression");
    }

    [Fact]
    public void ShouldDetectRestartNodeInConditionalExpression()
    {
        var code = """
                   using NPipeline.ErrorHandling;
                   using NPipeline.Pipeline;
                   using System.Threading;
                   using System.Threading.Tasks;

                   public class MyErrorHandler : IPipelineErrorHandler
                   {
                       public async Task<PipelineErrorDecision> HandleNodeFailureAsync(
                           string nodeId,
                           Exception error,
                           PipelineContext context,
                           CancellationToken cancellationToken)
                       {
                           return error is TimeoutException ? PipelineErrorDecision.RestartNode : PipelineErrorDecision.FailPipeline;
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == ResilientExecutionConfigurationAnalyzer.IncompleteResilientConfigurationId);
        Assert.True(hasDiagnostic, "Analyzer should detect RestartNode in conditional expression");
    }

    private static IEnumerable<Diagnostic> GetDiagnostics(string code)
    {
        var syntaxTree = CSharpSyntaxTree.ParseText(code);

        // Get the path to the NPipeline assembly
        var nPipelineAssemblyPath = typeof(PipelineContext).Assembly.Location;

        var references = new[]
        {
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Enumerable).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Task).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(CancellationToken).Assembly.Location),
            MetadataReference.CreateFromFile(nPipelineAssemblyPath),
        };

        var compilation = CSharpCompilation.Create("TestAssembly")
            .AddReferences(references)
            .AddSyntaxTrees(syntaxTree);

        var analyzer = new ResilientExecutionConfigurationAnalyzer();
        var compilation2 = compilation.WithAnalyzers(new[] { analyzer }.ToImmutableArray<DiagnosticAnalyzer>());
        var diagnostics = compilation2.GetAnalyzerDiagnosticsAsync().Result;

        return diagnostics;
    }
}

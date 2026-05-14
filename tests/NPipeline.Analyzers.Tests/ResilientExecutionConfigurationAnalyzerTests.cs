using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using NPipeline.Pipeline;

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
                   using NPipeline.Graph;
                   using NPipeline.Nodes;
                   using NPipeline.Pipeline;
                   using NPipeline.Resilience;
                   using System.Threading;
                   using System.Threading.Tasks;

                   public class MyPolicy : IResiliencePolicy
                   {
                       public Task<ResilienceDecision> DecideNodeFailureAsync(
                           NodeDefinition nodeDefinition, INode node, Exception exception,
                           PipelineContext context, CancellationToken cancellationToken)
                           => Task.FromResult(ResilienceDecision.Fail);

                       public async Task<ResilienceDecision> DecidePipelineFailureAsync(
                           string nodeId, Exception error, PipelineContext context,
                           CancellationToken cancellationToken)
                       {
                           return ResilienceDecision.RestartNode;
                       }

                       public Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
                           ITransformNode<TIn, TOut> node, TIn failedItem, Exception exception,
                           PipelineContext context, string nodeId, int retryAttempt,
                           CancellationToken cancellationToken)
                           => Task.FromResult(ResilienceDecision.Fail);

                       public System.ValueTask<System.TimeSpan> GetRetryDelayAsync(
                           PipelineContext context, int attemptNumber, CancellationToken cancellationToken)
                           => new(System.TimeSpan.Zero);

                       public IResilienceCircuitBreaker GetCircuitBreaker(PipelineContext context, string nodeId)
                           => null;
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        // The analyzer should detect that RestartNode is being returned
        var hasDiagnostic = diagnostics.Any(d => d.Id == ResilientExecutionConfigurationAnalyzer.IncompleteResilientConfigurationId);
        Assert.True(hasDiagnostic, "Analyzer should detect RestartNode return in error handler");
    }

    [Fact]
    public void ShouldIgnoreFailDecision()
    {
        var code = """
                   using NPipeline.Graph;
                   using NPipeline.Nodes;
                   using NPipeline.Pipeline;
                   using NPipeline.Resilience;
                   using System.Threading;
                   using System.Threading.Tasks;

                   public class MyPolicy : IResiliencePolicy
                   {
                       public Task<ResilienceDecision> DecideNodeFailureAsync(
                           NodeDefinition nodeDefinition, INode node, Exception exception,
                           PipelineContext context, CancellationToken cancellationToken)
                           => Task.FromResult(ResilienceDecision.Fail);

                       public async Task<ResilienceDecision> DecidePipelineFailureAsync(
                           string nodeId, Exception error, PipelineContext context,
                           CancellationToken cancellationToken)
                       {
                           return ResilienceDecision.Fail;
                       }

                       public Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
                           ITransformNode<TIn, TOut> node, TIn failedItem, Exception exception,
                           PipelineContext context, string nodeId, int retryAttempt,
                           CancellationToken cancellationToken)
                           => Task.FromResult(ResilienceDecision.Fail);

                       public System.ValueTask<System.TimeSpan> GetRetryDelayAsync(
                           PipelineContext context, int attemptNumber, CancellationToken cancellationToken)
                           => new(System.TimeSpan.Zero);

                       public IResilienceCircuitBreaker GetCircuitBreaker(PipelineContext context, string nodeId)
                           => null;
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        // The analyzer should NOT report a diagnostic for Fail
        var hasDiagnostic = diagnostics.Any(d => d.Id == ResilientExecutionConfigurationAnalyzer.IncompleteResilientConfigurationId);
        Assert.False(hasDiagnostic, "Analyzer should not report for Fail decision");
    }

    [Fact]
    public void ShouldIgnoreContinueWithoutNodeDecision()
    {
        var code = """
                   using NPipeline.Graph;
                   using NPipeline.Nodes;
                   using NPipeline.Pipeline;
                   using NPipeline.Resilience;
                   using System.Threading;
                   using System.Threading.Tasks;

                   public class MyPolicy : IResiliencePolicy
                   {
                       public Task<ResilienceDecision> DecideNodeFailureAsync(
                           NodeDefinition nodeDefinition, INode node, Exception exception,
                           PipelineContext context, CancellationToken cancellationToken)
                           => Task.FromResult(ResilienceDecision.Fail);

                       public async Task<ResilienceDecision> DecidePipelineFailureAsync(
                           string nodeId, Exception error, PipelineContext context,
                           CancellationToken cancellationToken)
                       {
                           return ResilienceDecision.ContinueWithoutNode;
                       }

                       public Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
                           ITransformNode<TIn, TOut> node, TIn failedItem, Exception exception,
                           PipelineContext context, string nodeId, int retryAttempt,
                           CancellationToken cancellationToken)
                           => Task.FromResult(ResilienceDecision.Fail);

                       public System.ValueTask<System.TimeSpan> GetRetryDelayAsync(
                           PipelineContext context, int attemptNumber, CancellationToken cancellationToken)
                           => new(System.TimeSpan.Zero);

                       public IResilienceCircuitBreaker GetCircuitBreaker(PipelineContext context, string nodeId)
                           => null;
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
                   using NPipeline.Graph;
                   using NPipeline.Nodes;
                   using NPipeline.Pipeline;
                   using NPipeline.Resilience;
                   using System.Threading;
                   using System.Threading.Tasks;

                   public class MyPolicy : IResiliencePolicy
                   {
                       public Task<ResilienceDecision> DecideNodeFailureAsync(
                           NodeDefinition nodeDefinition, INode node, Exception exception,
                           PipelineContext context, CancellationToken cancellationToken)
                           => Task.FromResult(ResilienceDecision.Fail);

                       public async Task<ResilienceDecision> DecidePipelineFailureAsync(
                           string nodeId, Exception error, PipelineContext context,
                           CancellationToken cancellationToken)
                       {
                           return error switch
                           {
                               TimeoutException => ResilienceDecision.RestartNode,
                               _ => ResilienceDecision.Fail
                           };
                       }

                       public Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
                           ITransformNode<TIn, TOut> node, TIn failedItem, Exception exception,
                           PipelineContext context, string nodeId, int retryAttempt,
                           CancellationToken cancellationToken)
                           => Task.FromResult(ResilienceDecision.Fail);

                       public System.ValueTask<System.TimeSpan> GetRetryDelayAsync(
                           PipelineContext context, int attemptNumber, CancellationToken cancellationToken)
                           => new(System.TimeSpan.Zero);

                       public IResilienceCircuitBreaker GetCircuitBreaker(PipelineContext context, string nodeId)
                           => null;
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
                   using NPipeline.Graph;
                   using NPipeline.Nodes;
                   using NPipeline.Pipeline;
                   using NPipeline.Resilience;
                   using System.Threading;
                   using System.Threading.Tasks;

                   public class MyPolicy : IResiliencePolicy
                   {
                       public Task<ResilienceDecision> DecideNodeFailureAsync(
                           NodeDefinition nodeDefinition, INode node, Exception exception,
                           PipelineContext context, CancellationToken cancellationToken)
                           => Task.FromResult(ResilienceDecision.Fail);

                       public async Task<ResilienceDecision> DecidePipelineFailureAsync(
                           string nodeId, Exception error, PipelineContext context,
                           CancellationToken cancellationToken)
                       {
                           return error is TimeoutException ? ResilienceDecision.RestartNode : ResilienceDecision.Fail;
                       }

                       public Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
                           ITransformNode<TIn, TOut> node, TIn failedItem, Exception exception,
                           PipelineContext context, string nodeId, int retryAttempt,
                           CancellationToken cancellationToken)
                           => Task.FromResult(ResilienceDecision.Fail);

                       public System.ValueTask<System.TimeSpan> GetRetryDelayAsync(
                           PipelineContext context, int attemptNumber, CancellationToken cancellationToken)
                           => new(System.TimeSpan.Zero);

                       public IResilienceCircuitBreaker GetCircuitBreaker(PipelineContext context, string nodeId)
                           => null;
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

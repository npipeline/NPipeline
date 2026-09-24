using Microsoft.CodeAnalysis;

namespace NPipeline.Analyzers.Tests;

public sealed class ResilientExecutionConfigurationAnalyzerTests
{
    private const string RestartPolicy = """
                                         public sealed class RestartOnce : ResiliencePolicyBase
                                         {
                                             public override ValueTask<ResilienceDecision> DecideRestartAsync(StreamFailure failure, CancellationToken cancellationToken)
                                                 => new(failure.Attempt == 1 ? ResilienceDecision.RestartNode : ResilienceDecision.Fail);
                                         }

                                         """;

    private static async Task<IReadOnlyList<Diagnostic>> AnalyzeAsync(string source)
    {
        var diagnostics = await ResilienceAnalyzerTestHelper.GetDiagnosticsAsync<ResilientExecutionConfigurationAnalyzer>(source);
        return diagnostics.Where(d => d.Id == ResilientExecutionConfigurationAnalyzer.IncompleteResilientConfigurationId).ToList();
    }

    [Fact]
    public async Task Reports_RestartNode_FromItemDecision()
    {
        var diagnostics = await AnalyzeAsync("""
                                             public sealed class Policy : ResiliencePolicyBase
                                             {
                                                 public override ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken)
                                                     => ValueTask.FromResult(ResilienceDecision.RestartNode);
                                             }

                                             public static class Setup
                                             {
                                                 public static NodeRestartOptions Restart => new() { MaxRestarts = 3 };
                                             }
                                             """);

        var diagnostic = Assert.Single(diagnostics);
        Assert.Contains("DecideItemFailureAsync", diagnostic.GetMessage());
    }

    [Fact]
    public async Task Reports_RestartNode_FromNodeDecision_InSwitchArm()
    {
        var diagnostics = await AnalyzeAsync("""
                                             public sealed class Policy : ResiliencePolicyBase
                                             {
                                                 public override async ValueTask<ResilienceDecision> DecideNodeFailureAsync(NodeFailure failure, CancellationToken cancellationToken)
                                                 {
                                                     await Task.Yield();
                                                     return failure.Exception switch
                                                     {
                                                         TimeoutException => ResilienceDecision.RestartNode,
                                                         _ => ResilienceDecision.Fail,
                                                     };
                                                 }
                                             }

                                             public static class Setup
                                             {
                                                 public static NodeRestartOptions Restart => new() { MaxRestarts = 3 };
                                             }
                                             """);

        var diagnostic = Assert.Single(diagnostics);
        Assert.Contains("DecideNodeFailureAsync", diagnostic.GetMessage());
    }

    [Fact]
    public async Task Reports_RestartDecision_WhenNothingSetsMaxRestarts()
    {
        var diagnostics = await AnalyzeAsync(RestartPolicy);

        var diagnostic = Assert.Single(diagnostics);
        Assert.Contains("MaxRestarts", diagnostic.GetMessage());
    }

    [Theory]
    [InlineData("builder.WithResilience(o => o with { NodeRestart = new NodeRestartOptions { MaxRestarts = 3 } });")]
    [InlineData("builder.WithResilience(o => o with { NodeRestart = o.NodeRestart with { MaxRestarts = 2 } });")]
    public async Task DoesNotReport_RestartDecision_WhenMaxRestartsIsSet(string configure)
    {
        var diagnostics = await AnalyzeAsync(RestartPolicy + $$"""
                                                               public static class Setup
                                                               {
                                                                   public static void Configure(PipelineBuilder builder)
                                                                   {
                                                                       {{configure}}
                                                                   }
                                                               }
                                                               """);

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task DoesNotReport_PolicyWithoutRestartNode()
    {
        var diagnostics = await AnalyzeAsync("""
                                             public sealed class Policy : ResiliencePolicyBase
                                             {
                                                 public override ValueTask<ResilienceDecision> DecideRestartAsync(StreamFailure failure, CancellationToken cancellationToken)
                                                     => new(ResilienceDecision.ContinueWithoutNode);

                                                 public override ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken)
                                                     => new(ResilienceDecision.Skip);
                                             }
                                             """);

        Assert.Empty(diagnostics);
    }
}

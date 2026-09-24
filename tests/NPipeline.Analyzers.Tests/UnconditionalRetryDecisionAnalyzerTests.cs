using Microsoft.CodeAnalysis;

namespace NPipeline.Analyzers.Tests;

public sealed class UnconditionalRetryDecisionAnalyzerTests
{
    private static async Task<IReadOnlyList<Diagnostic>> AnalyzeAsync(string source)
    {
        var diagnostics = await ResilienceAnalyzerTestHelper.GetDiagnosticsAsync<UnconditionalRetryDecisionAnalyzer>(source);
        return diagnostics.Where(d => d.Id == UnconditionalRetryDecisionAnalyzer.UnconditionalRetryDecisionId).ToList();
    }

    [Fact]
    public async Task Reports_BasePolicy_ThatAlwaysRetriesItems()
    {
        var diagnostics = await AnalyzeAsync("""
                                             public sealed class AlwaysRetry : ResiliencePolicyBase
                                             {
                                                 public override ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken)
                                                     => ValueTask.FromResult(ResilienceDecision.Retry);
                                             }
                                             """);

        var diagnostic = Assert.Single(diagnostics);
        Assert.Contains("DecideItemFailureAsync", diagnostic.GetMessage());
    }

    [Fact]
    public async Task Reports_Retry_GuardedOnlyByExceptionType()
    {
        // Retries a permanent TimeoutException forever: the exception type is not the retry budget.
        var diagnostics = await AnalyzeAsync("""
                                             public sealed class RetryTimeouts : ResiliencePolicyBase
                                             {
                                                 public override ValueTask<ResilienceDecision> DecideNodeFailureAsync(NodeFailure failure, CancellationToken cancellationToken)
                                                 {
                                                     if (failure.Exception is TimeoutException)
                                                         return ValueTask.FromResult(ResilienceDecision.Retry);

                                                     return ValueTask.FromResult(ResilienceDecision.Fail);
                                                 }
                                             }
                                             """);

        Assert.Single(diagnostics);
    }

    [Fact]
    public async Task Reports_InterfaceImplementation()
    {
        var diagnostics = await AnalyzeAsync("""
                                             public sealed class Policy : IResiliencePolicy
                                             {
                                                 public ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken)
                                                     => new(ResilienceDecision.Retry);

                                                 public ValueTask<ResilienceDecision> DecideRestartAsync(StreamFailure failure, CancellationToken cancellationToken)
                                                     => new(ResilienceDecision.Fail);

                                                 public ValueTask<ResilienceDecision> DecideNodeFailureAsync(NodeFailure failure, CancellationToken cancellationToken)
                                                     => new(ResilienceDecision.Fail);
                                             }
                                             """);

        Assert.Single(diagnostics);
    }

    [Theory]
    [InlineData("failure.CanRetry ? ResilienceDecision.Retry : ResilienceDecision.Fail")]
    [InlineData("failure.Attempt <= 5 ? ResilienceDecision.Retry : ResilienceDecision.Fail")]
    [InlineData("failure is { IsTransient: true } ? ResilienceDecision.Retry : ResilienceDecision.Fail")]
    [InlineData("failure.Exception is TimeoutException && failure.MaxRetries > 0 ? ResilienceDecision.Retry : ResilienceDecision.Fail")]
    public async Task DoesNotReport_RetryThatConsultsTheBudget(string decision)
    {
        var diagnostics = await AnalyzeAsync($$"""
                                               public sealed class Policy : ResiliencePolicyBase
                                               {
                                                   public override ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken)
                                                       => ValueTask.FromResult({{decision}});
                                               }
                                               """);

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task DoesNotReport_PolicyThatDefersToBase()
    {
        var diagnostics = await AnalyzeAsync("""
                                             public sealed class Policy : ResiliencePolicyBase
                                             {
                                                 public override ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken)
                                                 {
                                                     if (failure.Exception is ArgumentException)
                                                         return ValueTask.FromResult(ResilienceDecision.DeadLetter);

                                                     return failure.Exception is TimeoutException
                                                         ? ValueTask.FromResult(ResilienceDecision.Retry)
                                                         : base.DecideItemFailureAsync(failure, cancellationToken);
                                                 }
                                             }
                                             """);

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task DoesNotReport_PolicyThatNeverRetries()
    {
        var diagnostics = await AnalyzeAsync("""
                                             public sealed class Policy : ResiliencePolicyBase
                                             {
                                                 public override ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken)
                                                     => ValueTask.FromResult(ResilienceDecision.Skip);
                                             }
                                             """);

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task DoesNotReport_UnrelatedMethodWithTheSameName()
    {
        var diagnostics = await AnalyzeAsync("""
                                             public sealed class NotAPolicy
                                             {
                                                 public ValueTask<ResilienceDecision> DecideNodeFailureAsync(Exception failure, CancellationToken cancellationToken)
                                                     => new(ResilienceDecision.Retry);
                                             }
                                             """);

        Assert.Empty(diagnostics);
    }
}

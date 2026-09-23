using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.Reliability;

namespace NPipeline.Tests.Configuration;

/// <summary>
///     Both options records used to carry a hand-written <c>With</c> method that folded each argument with
///     <c>?? existing</c>, so passing <see langword="null" /> was indistinguishable from not passing the argument
///     and a nullable could never be cleared. The methods are deleted; the record <c>with</c> expression they were
///     reimplementing does the job correctly, and these tests pin the behaviour they could not express.
/// </summary>
public sealed class OptionsWithExpressionTests
{
    [Fact]
    public void LineageOptions_WithExpression_CanClearANullable()
    {
        var capped = LineageOptions.Default with { MaterializationCap = 500, OnMismatch = _ => { } };

        var cleared = capped with { MaterializationCap = null, OnMismatch = null };

        _ = cleared.MaterializationCap.Should().BeNull();
        _ = cleared.OnMismatch.Should().BeNull();
    }

    [Fact]
    public void LineageOptions_WithExpression_LeavesUnnamedPropertiesAlone()
    {
        var source = LineageOptions.CompleteLineage;

        var derived = source with { SampleEvery = 1 };

        _ = derived.SampleEvery.Should().Be(1);
        _ = derived.RedactData.Should().Be(source.RedactData);
        _ = derived.CaptureHopSnapshots.Should().Be(source.CaptureHopSnapshots);
        _ = derived.OverflowPolicy.Should().Be(source.OverflowPolicy);
    }

    [Fact]
    public void PipelineResilienceOptions_WithExpression_CanClearANullable()
    {
        var configured = PipelineResilienceOptions.None with { CircuitBreaker = PipelineCircuitBreakerOptions.Default };

        var cleared = configured with { CircuitBreaker = null };

        _ = cleared.CircuitBreaker.Should().BeNull();
    }

    [Fact]
    public void PipelineResilienceOptions_WithExpression_LeavesUnnamedPropertiesAlone()
    {
        var source = PipelineResilienceOptions.ForProfile(PipelineOptimizationProfile.Default) with { OnItemFailure = ItemFailureAction.Skip };

        var derived = source with { NodeRetry = new NodeRetryOptions { MaxRetries = 7 } };

        _ = derived.NodeRetry.MaxRetries.Should().Be(7);
        _ = derived.ItemRetry.Should().BeSameAs(source.ItemRetry);
        _ = derived.NodeRestart.Should().BeSameAs(source.NodeRestart);
        _ = derived.OnItemFailure.Should().Be(source.OnItemFailure);
        _ = derived.Time.Should().BeSameAs(source.Time);
    }
}

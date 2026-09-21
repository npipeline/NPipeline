using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.Configuration.RetryDelay;

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
    public void PipelineRetryOptions_WithExpression_CanClearANullable()
    {
        var configured = PipelineRetryOptions.Default with
        {
            MaxMaterializedItems = 10_000,
            DelayStrategyConfiguration = RetryDelayConfigurationExtensions.DefaultExponentialBackoffWithJitter,
        };

        var cleared = configured with { MaxMaterializedItems = null, DelayStrategyConfiguration = null };

        _ = cleared.MaxMaterializedItems.Should().BeNull();
        _ = cleared.DelayStrategyConfiguration.Should().BeNull();
    }

    [Fact]
    public void PipelineRetryOptions_WithExpression_LeavesUnnamedPropertiesAlone()
    {
        var source = PipelineRetryOptions.ForProfile(PipelineOptimizationProfile.Default);

        var derived = source with { MaxItemRetries = 7 };

        _ = derived.MaxItemRetries.Should().Be(7);
        _ = derived.MaxMaterializedItems.Should().Be(source.MaxMaterializedItems);
        _ = derived.DelayStrategyConfiguration.Should().Be(source.DelayStrategyConfiguration);
        _ = derived.MaxNodeRestartAttempts.Should().Be(source.MaxNodeRestartAttempts);
        _ = derived.MaxSequentialNodeAttempts.Should().Be(source.MaxSequentialNodeAttempts);
    }
}

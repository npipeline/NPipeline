using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.Configuration.RetryDelay;

namespace NPipeline.Tests.Configuration;

public sealed class PipelineRetryOptionsTests
{
    [Fact]
    public void ForProfile_Default_ShouldApplySensibleRetryDefaults()
    {
        var options = PipelineRetryOptions.ForProfile(PipelineOptimizationProfile.Default);

        options.MaxItemRetries.Should().Be(3);
        options.MaxMaterializedItems.Should().Be(10_000);
        options.DelayStrategyConfiguration.Should().NotBeNull();
    }

    [Fact]
    public void ForProfile_Default_ShouldUseExponentialBackoffWithFullJitter()
    {
        var options = PipelineRetryOptions.ForProfile(PipelineOptimizationProfile.Default);

        options.DelayStrategyConfiguration.Should().Be(RetryDelayConfigurationExtensions.DefaultExponentialBackoffWithJitter);
    }

    [Fact]
    public void ForProfile_HighThroughput_ShouldReturnStrictDefaults()
    {
        var options = PipelineRetryOptions.ForProfile(PipelineOptimizationProfile.HighThroughput);

        options.MaxItemRetries.Should().Be(0);
        options.MaxMaterializedItems.Should().BeNull();
        options.DelayStrategyConfiguration.Should().BeNull();
    }

    [Fact]
    public void ForProfile_HighThroughput_ShouldMatchDefaultInstance()
    {
        var options = PipelineRetryOptions.ForProfile(PipelineOptimizationProfile.HighThroughput);

        options.Should().Be(PipelineRetryOptions.Default);
    }

    [Fact]
    public void DefaultExponentialBackoffWithJitter_ShouldMatchForProfileDefaultDelay()
    {
        var fromProfile = PipelineRetryOptions.ForProfile(PipelineOptimizationProfile.Default);

        fromProfile.DelayStrategyConfiguration.Should().Be(RetryDelayConfigurationExtensions.DefaultExponentialBackoffWithJitter);
    }
}
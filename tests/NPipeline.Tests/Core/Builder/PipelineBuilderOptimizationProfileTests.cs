using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.Configuration.RetryDelay;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Core.Builder;

public sealed class PipelineBuilderOptimizationProfileTests
{
    [Fact]
    public void WithOptimizationProfile_Default_ShouldApplySensibleRetryDefaultsWhenNotExplicitlyConfigured()
    {
        var builder = new PipelineBuilder()
            .WithoutExtendedValidation()
            .WithOptimizationProfile(PipelineOptimizationProfile.Default);

        builder.AddSource<TestSourceNode, int>("source");
        var pipeline = builder.Build();

        var retryOptions = pipeline.Graph.ErrorHandling.RetryOptions;
        retryOptions.Should().NotBeNull();

        retryOptions!.MaxItemRetries.Should().Be(3);
        retryOptions.MaxMaterializedItems.Should().Be(10_000);
        retryOptions.DelayStrategyConfiguration.Should().NotBeNull();
        retryOptions.MaxNodeRestartAttempts.Should().Be(PipelineRetryOptions.Default.MaxNodeRestartAttempts);
        retryOptions.MaxSequentialNodeAttempts.Should().Be(PipelineRetryOptions.Default.MaxSequentialNodeAttempts);
    }

    [Fact]
    public void WithOptimizationProfile_HighThroughput_ShouldKeepStrictRetryDefaultsWhenNotExplicitlyConfigured()
    {
        var builder = new PipelineBuilder()
            .WithoutExtendedValidation()
            .WithOptimizationProfile(PipelineOptimizationProfile.HighThroughput);

        builder.AddSource<TestSourceNode, int>("source");
        var pipeline = builder.Build();

        var retryOptions = pipeline.Graph.ErrorHandling.RetryOptions;
        retryOptions.Should().NotBeNull();

        retryOptions.Should().Be(PipelineRetryOptions.Default);
    }

    [Fact]
    public void WithRetry_ShouldApplyDefaultProfileRetryDefaults()
    {
        var builder = new PipelineBuilder()
            .WithoutExtendedValidation();
        builder.WithRetry();

        builder.AddSource<TestSourceNode, int>("source");
        var pipeline = builder.Build();

        var retryOptions = pipeline.Graph.ErrorHandling.RetryOptions;
        retryOptions.Should().NotBeNull();

        retryOptions!.MaxItemRetries.Should().Be(3);
        retryOptions.MaxMaterializedItems.Should().Be(10_000);
        retryOptions.DelayStrategyConfiguration.Should().Be(RetryDelayConfigurationExtensions.DefaultExponentialBackoffWithJitter);
    }

    [Fact]
    public void WithRetry_ShouldOverrideHighThroughputStrictDefaults()
    {
        var builder = new PipelineBuilder()
            .WithoutExtendedValidation()
            .WithOptimizationProfile(PipelineOptimizationProfile.HighThroughput);

        builder.WithRetry();

        builder.AddSource<TestSourceNode, int>("source");
        var pipeline = builder.Build();

        var retryOptions = pipeline.Graph.ErrorHandling.RetryOptions;
        retryOptions.Should().NotBeNull();

        retryOptions!.MaxItemRetries.Should().Be(3);
        retryOptions.MaxMaterializedItems.Should().Be(10_000);
        retryOptions.DelayStrategyConfiguration.Should().Be(RetryDelayConfigurationExtensions.DefaultExponentialBackoffWithJitter);
    }

    [Fact]
    public void WithRetryOptions_ShouldNotBeOverwrittenByProfileDefaults()
    {
        var builder = new PipelineBuilder()
            .WithoutExtendedValidation()
            .WithOptimizationProfile(PipelineOptimizationProfile.Default)
            .WithRetryOptions(opt => opt with { MaxItemRetries = 5, MaxMaterializedItems = 256 });

        builder.AddSource<TestSourceNode, int>("source");
        var pipeline = builder.Build();

        var retryOptions = pipeline.Graph.ErrorHandling.RetryOptions;
        retryOptions.Should().NotBeNull();

        retryOptions!.MaxItemRetries.Should().Be(5);
        retryOptions.MaxMaterializedItems.Should().Be(256);
        retryOptions.DelayStrategyConfiguration.Should().BeNull();
    }

    [Fact]
    public void WithRetryOptions_NullConfigure_ShouldThrow()
    {
        var builder = new PipelineBuilder();

        var act = () => builder.WithRetryOptions(null!);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void WithRetryOptions_NullResult_ShouldThrow()
    {
        var builder = new PipelineBuilder();

        var act = () => builder.WithRetryOptions(_ => null!);

        act.Should().Throw<ArgumentNullException>();
    }

    private sealed class TestSourceNode : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }
}
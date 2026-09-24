using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.Reliability;

namespace NPipeline.Tests.Reliability;

public sealed class PipelineResilienceOptionsTests
{
    [Fact]
    public void DefaultProfile_RetriesTransientItemFailuresThreeTimes()
    {
        var options = PipelineResilienceOptions.ForProfile(PipelineOptimizationProfile.Default);

        options.ItemRetry.Should().BeSameAs(ItemRetryOptions.Default);
        options.ItemRetry.MaxRetries.Should().Be(3);
        options.ItemRetry.Classifier.Should().BeSameAs(RetryClassifier.Default);
        options.ItemRetry.Backoff.Kind.Should().Be(RetryBackoffKind.Exponential);
        options.NodeRestart.MaxRestarts.Should().Be(0);
        options.NodeRetry.MaxRetries.Should().Be(0);
        options.OnItemFailure.Should().Be(ItemFailureAction.Fail);
    }

    [Fact]
    public void HighThroughputProfile_RetriesNothing()
    {
        PipelineResilienceOptions.ForProfile(PipelineOptimizationProfile.HighThroughput).Should().BeSameAs(PipelineResilienceOptions.None);
        PipelineResilienceOptions.None.ItemRetry.MaxRetries.Should().Be(0);
    }

    [Fact]
    public void With_DerivesOptionsWithoutChangingTheOriginal()
    {
        var derived = PipelineResilienceOptions.None with { ItemRetry = ItemRetryOptions.Default with { MaxRetries = 5 } };

        derived.ItemRetry.MaxRetries.Should().Be(5);
        derived.ItemRetry.Backoff.Should().Be(ItemRetryOptions.Default.Backoff);
        PipelineResilienceOptions.None.ItemRetry.MaxRetries.Should().Be(0);
    }

    [Fact]
    public void Validate_RejectsNegativeLimits()
    {
        ((Action)(() => (PipelineResilienceOptions.None with { ItemRetry = new ItemRetryOptions { MaxRetries = -1 } }).Validate()))
            .Should().Throw<ArgumentOutOfRangeException>();

        ((Action)(() => (PipelineResilienceOptions.None with { NodeRestart = new NodeRestartOptions { MaxReplayWindow = 0 } }).Validate()))
            .Should().Throw<ArgumentOutOfRangeException>();

        ((Action)(() => (PipelineResilienceOptions.None with { NodeRetry = new NodeRetryOptions { MaxRetries = -1 } }).Validate()))
            .Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Validate_RejectsAnInvalidBackoff()
    {
        var options = PipelineResilienceOptions.None with
        {
            // An exponential kind with no factor: each property is valid on its own, the combination is not.
            ItemRetry = new ItemRetryOptions { Backoff = new RetryBackoff { Kind = RetryBackoffKind.Exponential } },
        };

        ((Action)(() => options.Validate())).Should().Throw<ArgumentOutOfRangeException>();
    }
}

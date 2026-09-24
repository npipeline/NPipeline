using System.Diagnostics;
using System.Reflection;
using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;

[assembly: AssemblyMetadata("NPipelineOptimizationProfile", "Default")]

namespace NPipeline.Tests.Core.Builder;

public sealed class PipelineBuilderOptimizationProfileTests
{
    [Fact]
    public void WithOptimizationProfile_Default_ShouldRetryTransientItemFailuresWhenNotExplicitlyConfigured()
    {
        var builder = new PipelineBuilder()
            .WithoutExtendedValidation()
            .WithOptimizationProfile(PipelineOptimizationProfile.Default);

        builder.AddSource<TestSourceNode, int>("source");
        var pipeline = builder.Build();

        var resilience = pipeline.Graph.ErrorHandling.Resilience;
        resilience.Should().NotBeNull();

        resilience!.ItemRetry.Should().BeSameAs(ItemRetryOptions.Default);
        resilience.ItemRetry.Classifier.Should().BeSameAs(RetryClassifier.Default);
        resilience.NodeRestart.Should().BeSameAs(NodeRestartOptions.None);
        resilience.NodeRetry.Should().BeSameAs(NodeRetryOptions.None);
    }

    [Fact]
    public void WithOptimizationProfile_HighThroughput_ShouldRetryNothingWhenNotExplicitlyConfigured()
    {
        var builder = new PipelineBuilder()
            .WithoutExtendedValidation()
            .WithOptimizationProfile(PipelineOptimizationProfile.HighThroughput);

        builder.AddSource<TestSourceNode, int>("source");
        var pipeline = builder.Build();

        pipeline.Graph.ErrorHandling.Resilience.Should().BeSameAs(PipelineResilienceOptions.None);
    }

    [Fact]
    public void Build_ShouldWarn_WhenRuntimeAndCompileTimeProfilesDiffer()
    {
        var builder = new PipelineBuilder()
            .WithoutExtendedValidation()
            .WithOptimizationProfile(PipelineOptimizationProfile.HighThroughput);

        builder.AddSource<TestSourceNode, int>("source");

        using var listener = new RecordingTraceListener();
        Trace.Listeners.Add(listener);

        try
        {
            _ = builder.Build();
        }
        finally
        {
            Trace.Listeners.Remove(listener);
        }

        listener.Messages.Should().Contain(message =>
            message.Contains("Optimization profile mismatch detected", StringComparison.Ordinal));
    }

    [Fact]
    public void WithResilience_StartsFromTheProfileDefaults()
    {
        var builder = new PipelineBuilder()
            .WithoutExtendedValidation()
            .WithOptimizationProfile(PipelineOptimizationProfile.Default)
            .WithResilience(o => o with { ItemRetry = o.ItemRetry with { MaxRetries = 5 } });

        builder.AddSource<TestSourceNode, int>("source");
        var pipeline = builder.Build();

        var resilience = pipeline.Graph.ErrorHandling.Resilience;
        resilience!.ItemRetry.MaxRetries.Should().Be(5);
        resilience.ItemRetry.Backoff.Should().Be(ItemRetryOptions.Default.Backoff);
    }

    [Fact]
    public void WithResilience_CallsCompose()
    {
        var builder = new PipelineBuilder()
            .WithoutExtendedValidation()
            .WithResilience(o => o with { OnItemFailure = ItemFailureAction.Skip })
            .WithResilience(o => o with { NodeRetry = new NodeRetryOptions { MaxRetries = 2 } });

        builder.AddSource<TestSourceNode, int>("source");
        var resilience = builder.Build().Graph.ErrorHandling.Resilience!;

        resilience.OnItemFailure.Should().Be(ItemFailureAction.Skip);
        resilience.NodeRetry.MaxRetries.Should().Be(2);
    }

    [Fact]
    public void WithResilience_NullConfigure_ShouldThrow()
    {
        var builder = new PipelineBuilder();

        var act = () => builder.WithResilience(null!);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void WithResilience_NullResult_ShouldFailTheBuild()
    {
        var builder = new PipelineBuilder().WithoutExtendedValidation();
        builder.AddSource<TestSourceNode, int>("source");
        builder.WithResilience(_ => null!);

        var act = () => builder.Build();

        act.Should().Throw<InvalidOperationException>().WithMessage("*returned null*");
    }

    private sealed class TestSourceNode : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken) => throw new NotImplementedException();
    }

    private sealed class RecordingTraceListener : TraceListener
    {
        public List<string> Messages { get; } = [];

        public override void Write(string? message)
        {
            if (!string.IsNullOrEmpty(message))
                Messages.Add(message);
        }

        public override void WriteLine(string? message)
        {
            if (!string.IsNullOrEmpty(message))
                Messages.Add(message);
        }
    }
}

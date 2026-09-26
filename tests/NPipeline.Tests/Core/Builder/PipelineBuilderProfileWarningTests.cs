using System.Diagnostics;
using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Core.Builder;

/// <summary>
///     Tests the process-wide caching of the compile-time optimisation profile lookup: the assembly metadata is read
///     once, and a runtime/compile-time profile mismatch is warned about once rather than on every build.
/// </summary>
[Collection("ProcessWideCounters")]
public sealed class PipelineBuilderProfileWarningTests
{
    [Fact]
    public void Build_ShouldWarnOnlyOnce_WhenTheSameMismatchIsBuiltRepeatedly()
    {
        PipelineBuilder.ResetOptimizationProfileCaches();

        using var listener = new RecordingTraceListener();
        Trace.Listeners.Add(listener);

        try
        {
            for (var i = 0; i < 3; i++)
            {
                var builder = new PipelineBuilder()
                    .WithoutExtendedValidation()
                    .WithOptimizationProfile(PipelineOptimizationProfile.HighThroughput);

                builder.AddSource<TestSourceNode, int>("source");
                _ = builder.Build();
            }
        }
        finally
        {
            Trace.Listeners.Remove(listener);
        }

        listener.Messages.Count(message =>
            message.Contains("Optimization profile mismatch detected", StringComparison.Ordinal)).Should().Be(1);
    }

    [Fact]
    public void Build_ResolvesAssemblyMetadataOnce_PerAssembly()
    {
        PipelineBuilder.ResetOptimizationProfileCaches();

        void BuildAndDiscard()
        {
            var builder = new PipelineBuilder()
                .WithoutExtendedValidation()
                .WithOptimizationProfile(PipelineOptimizationProfile.Default);

            builder.AddSource<TestSourceNode, int>("source");
            _ = builder.Build();
        }

        BuildAndDiscard();
        var afterFirst = PipelineBuilder.AssemblyMetadataReadCount;

        BuildAndDiscard();
        var afterSecond = PipelineBuilder.AssemblyMetadataReadCount;

        _ = afterFirst.Should().BeGreaterThan(0);
        _ = afterSecond.Should().Be(afterFirst);
    }

    private sealed class TestSourceNode : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new NPipeline.DataFlow.DataStreams.DataStream<int>(Empty(), "empty");

        private static async IAsyncEnumerable<int> Empty()
        {
            await Task.CompletedTask;
            yield break;
        }
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

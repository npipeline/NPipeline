using AwesomeAssertions;
using NPipeline.DataFlow.Branching;
using NPipeline.Execution;
using NPipeline.Execution.Factories;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Nodes.Branch;

public sealed class BranchCapacityTests
{
    [Fact]
    public async Task Branch_ShouldRecordConfiguredCapacityEvenIfUnboundedInternally()
    {
        var ctx = PipelineContext.Default;
        var collect1 = new InMemorySinkNode<int>();
        var collect2 = new InMemorySinkNode<int>();

        ctx.Items[PipelineContextKeys.PreconfiguredNodes] = new Dictionary<string, INode>
        {
            { "s1", collect1 },
            { "s2", collect2 },
        };

        var runner = new PipelineRunner(new PipelineFactory(), new DefaultNodeFactory());
        await runner.RunAsync<BranchingPipeline>(ctx);
        var metrics = ctx.GetBranchMetrics("t");
        metrics.Should().NotBeNull();
        metrics!.SubscriberCount.Should().Be(2);
        metrics.PerSubscriberCapacity.Should().Be(32);
        metrics.SubscribersCompleted.Should().Be(2);
        metrics.Faulted.Should().Be(0);
    }

    private sealed class BranchingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var src = builder.AddInMemorySource("src", Enumerable.Range(1, 10));
            var t = builder.AddPassThroughTransform<int, int>("t");
            var s1 = builder.AddSink<InMemorySinkNode<int>, int>("s1");
            var s2 = builder.AddSink<InMemorySinkNode<int>, int>("s2");
            builder.Connect(src, t).Connect(t, s1).Connect(t, s2);
            builder.WithBranchOptions("t", new BranchOptions(32));
        }
    }
}

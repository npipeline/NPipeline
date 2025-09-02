using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.Execution.Strategies;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Resilience.Restart;

public sealed class ResilienceIdempotencyTests
{
    [Fact]
    public void ApplyingResilienceTwice_ShouldWrapOnlyOnce()
    {
        var builder = new PipelineBuilder();
        var s = builder.AddSource<Src, int>("s");
        var t = builder.AddTransform<T, int, int>("t");
        var k = builder.AddSink<Sink, int>("k");
        builder.Connect(s, t).Connect(t, k);
        builder.WithResilience(t).WithResilience(t); // second call should be idempotent
        var pipeline = builder.Build();

        var node = pipeline.Graph.Nodes.Single(n => n.Id == "t");
        node.ExecutionStrategy.Should().BeOfType<ResilientExecutionStrategy>();
    }

    private sealed class Src : SourceNode<int>
    {
        public override IDataPipe<int> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
        {
            return new ListDataPipe<int>(new[] { 1 }.ToList(), "s");
        }
    }

    private sealed class T : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item);
        }
    }

    private sealed class Sink : SinkNode<int>
    {
        public override Task ExecuteAsync(IDataPipe<int> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}

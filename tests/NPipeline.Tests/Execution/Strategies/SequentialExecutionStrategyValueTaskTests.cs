using AwesomeAssertions;
using NPipeline.Execution.Strategies;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Execution.Strategies;

public sealed class SequentialExecutionStrategyValueTaskTests
{
    [Fact]
    public async Task Should_PreferValueTaskPath_WhenTransformOverrides()
    {
        await using var input = new NPipeline.DataFlow.DataPipes.ListDataPipe<int>(new[] { 1, 2, 3 }, "input");
        var transform = new ValueTaskFriendlyTransform();
        var strategy = new SequentialExecutionStrategy();
        var context = new PipelineContextBuilder().Build();

        await using var output = await strategy.ExecuteAsync(input, transform, context, CancellationToken.None);

        var results = new List<int>();

        await foreach (var value in output.WithCancellation(CancellationToken.None))
        {
            results.Add(value);
        }

        _ = results.Should().BeEquivalentTo([2, 3, 4]);
        _ = transform.ExecuteAsyncCallCount.Should().Be(0);
        _ = transform.ExecuteValueTaskCallCount.Should().Be(3);
    }

    private sealed class ValueTaskFriendlyTransform : TransformNode<int, int>
    {
        public int ExecuteAsyncCallCount { get; private set; }

        public int ExecuteValueTaskCallCount { get; private set; }

        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            ExecuteAsyncCallCount++;
            return FromValueTask(ExecuteValueTaskAsync(item, context, cancellationToken));
        }

        protected internal override ValueTask<int> ExecuteValueTaskAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            ExecuteValueTaskCallCount++;
            return ValueTask.FromResult(item + 1);
        }
    }
}

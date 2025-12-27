using AwesomeAssertions;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Tests.Common;

namespace NPipeline.Extensions.Parallelism.Tests;

public sealed class ParallelExecutionStrategyValueTaskTests
{
    [Fact]
    public async Task Should_PreferValueTaskPath_WhenTransformOverrides()
    {
        await using InMemoryDataPipe<int> input = new([1, 2, 3], "input");
        ValueTaskFriendlyTransform transform = new();
        ParallelExecutionStrategy strategy = new(1);
        var context = new PipelineContext();
        List<int> results = [];

        using (context.ScopedNode("transform"))
        {
            await using var output = await strategy.ExecuteAsync(input, transform, context, CancellationToken.None);

            await foreach (var value in output.WithCancellation(CancellationToken.None))
            {
                results.Add(value);
            }
        }

        _ = results.Should().BeEquivalentTo([2, 3, 4]);
        _ = transform.ExecuteAsyncCallCount.Should().Be(0);
        _ = transform.ExecuteValueTaskCallCount.Should().Be(3);
    }

    [Fact]
    public async Task DropOldest_Should_PreferValueTaskPath_WhenTransformOverrides()
    {
        await using InMemoryDataPipe<int> input = new([10, 20, 30], "input");
        ValueTaskFriendlyTransform transform = new();
        DropOldestParallelStrategy strategy = new(1);
        var context = new PipelineContext();
        List<int> results = [];

        using (context.ScopedNode("transform"))
        {
            context.Items[PipelineContextKeys.NodeExecutionOptions("transform")] = new ParallelOptions
            {
                MaxDegreeOfParallelism = 1,
                MaxQueueLength = 4,
                QueuePolicy = BoundedQueuePolicy.DropOldest,
            };

            await using var output = await strategy.ExecuteAsync(input, transform, context, CancellationToken.None);

            await foreach (var value in output.WithCancellation(CancellationToken.None))
            {
                results.Add(value);
            }
        }

        int[] expected = [11, 21, 31];
        _ = results.Should().BeEquivalentTo(expected);
        _ = transform.ExecuteAsyncCallCount.Should().Be(0);
        _ = transform.ExecuteValueTaskCallCount.Should().Be(3);
    }

    [Fact]
    public async Task DropNewest_Should_PreferValueTaskPath_WhenTransformOverrides()
    {
        await using InMemoryDataPipe<int> input = new([7, 8, 9], "input");
        ValueTaskFriendlyTransform transform = new();
        DropNewestParallelStrategy strategy = new(1);
        var context = new PipelineContext();
        List<int> results = [];

        using (context.ScopedNode("transform"))
        {
            context.Items[PipelineContextKeys.NodeExecutionOptions("transform")] = new ParallelOptions
            {
                MaxDegreeOfParallelism = 1,
                MaxQueueLength = 4,
                QueuePolicy = BoundedQueuePolicy.DropNewest,
            };

            await using var output = await strategy.ExecuteAsync(input, transform, context, CancellationToken.None);

            await foreach (var value in output.WithCancellation(CancellationToken.None))
            {
                results.Add(value);
            }
        }

        int[] expected = [8, 9, 10];
        _ = results.Should().BeEquivalentTo(expected);
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

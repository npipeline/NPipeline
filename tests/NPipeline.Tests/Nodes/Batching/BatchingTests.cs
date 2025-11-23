using System.Diagnostics;
using System.Threading.Channels;
using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution.Strategies;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Xunit.Abstractions;

namespace NPipeline.Tests.Nodes.Batching;

public sealed class BatchingTests(ITestOutputHelper output)
{
    [Fact]
    public async Task BatchingNode_BatchesCorrectly_BySize()
    {
        _ = output; // Parameter is unused but required for test infrastructure

        // Arrange
        var source = Enumerable.Range(0, 100).ToAsyncEnumerable();
        var pipe = new StreamingDataPipe<int>(source);
        var batchingNode = new BatchingNode<int>(10, TimeSpan.FromSeconds(10));

        // Act
        var resultPipe = await batchingNode.ExecuteWithStrategyAsync(pipe, PipelineContext.Default, CancellationToken.None);
        var results = await resultPipe.ToListAsync();

        // Assert
        results.Should().HaveCount(10);
        results.ForEach(batch => batch.Should().HaveCount(10));
    }

    [Fact]
    public async Task BatchingNode_BatchesCorrectly_WithIncompleteFinalBatch()
    {
        _ = output; // Parameter is unused but required for test infrastructure

        // Arrange
        var source = Enumerable.Range(0, 95).ToAsyncEnumerable();
        var pipe = new StreamingDataPipe<int>(source);
        var batchingNode = new BatchingNode<int>(10, TimeSpan.FromSeconds(10));

        // Act
        var resultPipe = await batchingNode.ExecuteWithStrategyAsync(pipe, PipelineContext.Default, CancellationToken.None);
        var results = await resultPipe.ToListAsync();

        // Assert
        results.Should().HaveCount(10);
        results.Take(9).ToList().ForEach(batch => batch.Should().HaveCount(10));
        results.Last().Should().HaveCount(5);
    }

    [Fact]
    public async Task BatchingNode_BatchesCorrectly_ByTime()
    {
        _ = output; // Parameter is unused but required for test infrastructure

        // Arrange
        var channel = Channel.CreateUnbounded<int>();

        _ = Task.Run(async () =>
        {
            for (var i = 0; i < 5; i++)
            {
                await channel.Writer.WriteAsync(i);
                await Task.Delay(200);
            }

            channel.Writer.Complete();
        });

        var pipe = new StreamingDataPipe<int>(channel.Reader.ReadAllAsync());
        var batchingNode = new BatchingNode<int>(10, TimeSpan.FromMilliseconds(50));

        // Act
        var stopwatch = Stopwatch.StartNew();
        var resultPipe = await batchingNode.ExecuteWithStrategyAsync(pipe, PipelineContext.Default, CancellationToken.None);
        var results = await resultPipe.ToListAsync();
        stopwatch.Stop();

        // Assert
        results.Should().NotBeEmpty();
        results.First().Should().HaveCount(1); // The first item should be in its own batch due to the delay
        stopwatch.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(4));
    }

    [Fact]
    public async Task BatchingNode_HandlesEmptySource()
    {
        _ = output; // Parameter is unused but required for test infrastructure

        // Arrange
        var source = Array.Empty<int>().ToAsyncEnumerable();
        var pipe = new StreamingDataPipe<int>(source);
        var batchingNode = new BatchingNode<int>(10, TimeSpan.FromSeconds(10));

        // Act
        var resultPipe = await batchingNode.ExecuteWithStrategyAsync(pipe, PipelineContext.Default, CancellationToken.None);
        var results = await resultPipe.ToListAsync();

        // Assert
        results.Should().BeEmpty();
    }

    [Fact]
    public async Task UnbatchingNode_FlattensBatches()
    {
        _ = output; // Parameter is unused but required for test infrastructure

        // Arrange
        var source = new List<List<int>>
        {
            new() { 1, 2, 3 },
            new() { 4, 5 },
            new() { 6, 7, 8, 9 },
        }.ToAsyncEnumerable();

        var pipe = new StreamingDataPipe<IEnumerable<int>>(source);
        var unbatchingNode = new UnbatchingNode<int>();
        unbatchingNode.ExecutionStrategy = new UnbatchingExecutionStrategy();

        // Act
        var resultPipe = await unbatchingNode.ExecuteWithStrategyAsync(pipe, PipelineContext.Default, CancellationToken.None);
        var results = await resultPipe.ToListAsync();

        // Assert
        results.Should().HaveCount(9);
        results.Should().BeEquivalentTo(Enumerable.Range(1, 9));
    }
}

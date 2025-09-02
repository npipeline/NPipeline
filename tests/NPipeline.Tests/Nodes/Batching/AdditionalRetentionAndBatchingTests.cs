using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;
using Xunit.Abstractions;

namespace NPipeline.Tests.Nodes.Batching;

public sealed class AdditionalRetentionAndBatchingTests(ITestOutputHelper output)
{
    [Fact]
    public async Task BatchAsync_AccumulatesUntilSizeWithinLargeWindow()
    {
        _ = output; // Parameter is unused but required for test infrastructure

        // Arrange
        var source = Enumerable.Range(0, 25).ToAsyncEnumerable();
        var pipe = new StreamingDataPipe<int>(source);
        var batchingNode = new BatchingNode<int>(10, TimeSpan.FromSeconds(5));

        // Act
        var resultPipe = await batchingNode.ExecuteWithStrategyAsync(pipe, PipelineContext.Default, CancellationToken.None);
        var results = await resultPipe.ToListAsync();

        // Assert
        // Expect 3 batches: 10,10,5 with large window not forcing early flush
        results.Should().HaveCount(3);
        results[0].Should().HaveCount(10);
        results[1].Should().HaveCount(10);
        results[2].Should().HaveCount(5);
    }

    private sealed class NoOpLogger : IPipelineLogger
    {
        public void Log(LogLevel logLevel, string message, params object[] args)
        {
        }

        public void Log(LogLevel logLevel, Exception exception, string message, params object[] args)
        {
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return false;
        }
    }
}

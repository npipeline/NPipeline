using System.Diagnostics;
using System.Threading.Channels;
using AwesomeAssertions;
using Microsoft.Extensions.Logging;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Xunit.Abstractions;

namespace NPipeline.Tests.Nodes.Batching;

/// <summary>
///     Comprehensive tests for BatchingNode functionality including basic batching,
///     time-based batching, empty source handling, and advanced window scenarios.
/// </summary>
public sealed class BatchingTests(ITestOutputHelper output)
{
    #region Advanced Batching Tests

    [Fact]
    public async Task BatchAsync_AccumulatesUntilSizeWithinLargeWindow()
    {
        _ = output; // Parameter is unused but required for test infrastructure

        // Arrange
        var source = Enumerable.Range(0, 25).ToAsyncEnumerable();
        var batchingNode = new BatchingNode<int>(10, TimeSpan.FromSeconds(5));
        var context = PipelineContext.Default;

        // Act
        var results = new List<IReadOnlyCollection<int>>();

        await foreach (var batch in batchingNode.ExecuteAsync(source, context, CancellationToken.None))
        {
            results.Add(batch);
        }

        // Assert
        // Expect 3 batches: 10,10,5 with large window not forcing early flush
        results.Should().HaveCount(3);
        results[0].Should().HaveCount(10);
        results[1].Should().HaveCount(10);
        results[2].Should().HaveCount(5);
    }

    #endregion

    #region Unbatching Tests

    [Fact]
    public async Task UnbatchingNode_FlattensBatches()
    {
        _ = output; // Parameter is unused but required for test infrastructure

        // Arrange
        var source = new List<List<int>>
        {
            new() { 1, 2, 3 },
            new() { 4, 5, 6, 7, 8, 9 },
        }.ToAsyncEnumerable();

        var unbatchingNode = new UnbatchingNode<int>();
        var context = PipelineContext.Default;

        // Act
        var results = new List<int>();

        await foreach (var item in unbatchingNode.ExecuteAsync(source, context, CancellationToken.None))
        {
            results.Add(item);
        }

        // Assert
        results.Should().HaveCount(9);
        results.Should().BeEquivalentTo(Enumerable.Range(1, 9));
    }

    #endregion

    #region Helper Classes

    private sealed class NoOpLogger : ILogger
    {
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
        }

        public bool IsEnabled(LogLevel logLevel) => false;

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    }

    #endregion

    #region Basic Batching Functionality Tests

    [Fact]
    public async Task BatchingNode_BatchesCorrectly_BySize()
    {
        _ = output; // Parameter is unused but required for test infrastructure

        // Arrange
        var source = Enumerable.Range(0, 100).ToAsyncEnumerable();
        var batchingNode = new BatchingNode<int>(10, TimeSpan.FromSeconds(10));
        var context = PipelineContext.Default;

        // Act
        var results = new List<IReadOnlyCollection<int>>();

        await foreach (var batch in batchingNode.ExecuteAsync(source, context, CancellationToken.None))
        {
            results.Add(batch);
        }

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
        var batchingNode = new BatchingNode<int>(10, TimeSpan.FromSeconds(10));
        var context = PipelineContext.Default;

        // Act
        var results = new List<IReadOnlyCollection<int>>();

        await foreach (var batch in batchingNode.ExecuteAsync(source, context, CancellationToken.None))
        {
            results.Add(batch);
        }

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

        var source = channel.Reader.ReadAllAsync();
        var batchingNode = new BatchingNode<int>(10, TimeSpan.FromMilliseconds(50));
        var context = PipelineContext.Default;

        // Act
        var stopwatch = Stopwatch.StartNew();
        var results = new List<IReadOnlyCollection<int>>();

        await foreach (var batch in batchingNode.ExecuteAsync(source, context, CancellationToken.None))
        {
            results.Add(batch);
        }

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
        var batchingNode = new BatchingNode<int>(10, TimeSpan.FromSeconds(10));
        var context = PipelineContext.Default;

        // Act
        var results = new List<IReadOnlyCollection<int>>();

        await foreach (var batch in batchingNode.ExecuteAsync(source, context, CancellationToken.None))
        {
            results.Add(batch);
        }

        // Assert
        results.Should().BeEmpty();
    }

    #endregion
}

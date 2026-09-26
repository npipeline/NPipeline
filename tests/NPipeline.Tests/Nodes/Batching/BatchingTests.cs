using System.Diagnostics;
using System.Threading.Channels;
using AwesomeAssertions;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow;
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
        var context = PipelineContext.CreateDefault();

        // Act
        var results = new List<IReadOnlyCollection<int>>();

        await foreach (var batch in batchingNode.TransformAsync(source, context, CancellationToken.None))
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

    [Fact]
    public async Task BatchAsync_SmallWindow_StillBatches()
    {
        _ = output;

        // Arrange
        var source = Enumerable.Range(0, 100).ToAsyncEnumerable();

        // Act
        var batches = await source.BatchAsync(10, TimeSpan.FromMilliseconds(50)).ToListAsync();

        // Assert: a window of 100 ms or less used to produce one batch per item. A stalled producer can still split a
        // batch on a loaded machine, so this asserts batching happens rather than exact batch boundaries.
        batches.Sum(batch => batch.Count).Should().Be(100);
        batches.Count.Should().BeLessThan(50);
    }

    [Fact]
    public async Task BatchAsync_ForeignCancellation_SurfacesAsError()
    {
        _ = output;

        // Arrange
        var source = ThrowAfterItems(1, 2);

        // Act
        var act = async () => await source.BatchAsync(10, TimeSpan.FromSeconds(5)).ToListAsync();

        // Assert
        await act.Should().ThrowAsync<TaskCanceledException>().WithMessage("http timeout");
    }

    [Fact]
    public async Task BatchAsync_ConsumerLeavesEarly_StopsProducer()
    {
        _ = output;

        // Arrange
        var yielded = 0;

        async IAsyncEnumerable<int> CountingSource()
        {
            var i = 0;

            while (true)
            {
                _ = Interlocked.Increment(ref yielded);
                yield return i++;
                await Task.Delay(10); // keep the producer from racing far ahead
            }
        }

        // Act
        await foreach (var batch in CountingSource().BatchAsync(5, TimeSpan.FromSeconds(5)))
        {
            batch.Should().HaveCount(5);
            break;
        }

        await Task.Delay(200);
        var afterStop = Volatile.Read(ref yielded);
        await Task.Delay(200);

        // Assert
        Volatile.Read(ref yielded).Should().Be(afterStop);
        afterStop.Should().BeLessThanOrEqualTo(22, "the producer stops once the consumer leaves");
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
        var context = PipelineContext.CreateDefault();

        // Act
        var results = new List<int>();

        await foreach (var item in unbatchingNode.TransformAsync(source, context, CancellationToken.None))
        {
            results.Add(item);
        }

        // Assert
        results.Should().HaveCount(9);
        results.Should().BeEquivalentTo(Enumerable.Range(1, 9));
    }

    #endregion

    #region Helper Classes

    private static async IAsyncEnumerable<int> ThrowAfterItems(params int[] items)
    {
        foreach (var item in items)
            yield return item;

        await Task.Yield();
        throw new TaskCanceledException("http timeout");
    }

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
        var context = PipelineContext.CreateDefault();

        // Act
        var results = new List<IReadOnlyCollection<int>>();

        await foreach (var batch in batchingNode.TransformAsync(source, context, CancellationToken.None))
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
        var context = PipelineContext.CreateDefault();

        // Act
        var results = new List<IReadOnlyCollection<int>>();

        await foreach (var batch in batchingNode.TransformAsync(source, context, CancellationToken.None))
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
        var context = PipelineContext.CreateDefault();

        // Act
        var stopwatch = Stopwatch.StartNew();
        var results = new List<IReadOnlyCollection<int>>();

        await foreach (var batch in batchingNode.TransformAsync(source, context, CancellationToken.None))
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
        var context = PipelineContext.CreateDefault();

        // Act
        var results = new List<IReadOnlyCollection<int>>();

        await foreach (var batch in batchingNode.TransformAsync(source, context, CancellationToken.None))
        {
            results.Add(batch);
        }

        // Assert
        results.Should().BeEmpty();
    }

    #endregion
}

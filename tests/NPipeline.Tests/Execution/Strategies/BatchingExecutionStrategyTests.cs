using System.Runtime.CompilerServices;
using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution.Strategies;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Pipeline;
using NPipeline.Tests.Reliability.Behavior;

namespace NPipeline.Tests.Execution.Strategies;

public sealed class BatchingExecutionStrategyTests
{
    [Fact]
    public async Task Batcher_FlushesPartialBatchOnTimeout()
    {
        var sink = new CollectingSink<IReadOnlyCollection<int>>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var run = BehaviorPipeline.RunAsync(b =>
        {
            var s = b.AddSource<StreamingSource<int>, int>("source");
            var batch = b.AddBatcher<int>("batch", 10, TimeSpan.FromMilliseconds(200));
            var k = b.AddSink<CollectingSink<IReadOnlyCollection<int>>, IReadOnlyCollection<int>>("sink");
            _ = b.AddPreconfiguredNodeInstance(s.Id, StreamingSource<int>.Unbounded([1])).AddPreconfiguredNodeInstance(k.Id, sink);
            _ = b.Connect(s, batch).Connect(batch, k);
        }, cancellationToken: cts.Token);

        var deadline = DateTime.UtcNow.AddSeconds(3);

        while (sink.Items.Count == 0 && DateTime.UtcNow < deadline)
            await Task.Delay(50);

        var got = sink.Items.Count;
        await cts.CancelAsync();

        try
        {
            await run;
        }
        catch
        {
            // cancelled
        }

        got.Should().Be(1);
    }

    [Fact]
    public async Task Batcher_DoesNotEmitBatchOfOneAfterIdleGap()
    {
        var items = 0;

        async IAsyncEnumerable<int> IdleThenBurstAsync([EnumeratorCancellation] CancellationToken ct)
        {
            // The idle gap outlasts the window, so a window timed from the last emission would already have expired.
            await Task.Delay(1_500, ct);
            _ = Interlocked.Increment(ref items);
            yield return 1;
            await Task.Delay(50, ct);
            _ = Interlocked.Increment(ref items);
            yield return 2;
            await Task.Delay(50, ct);
            _ = Interlocked.Increment(ref items);
            yield return 3;
        }

        var strategy = new BatchingExecutionStrategy(10, TimeSpan.FromSeconds(1));
        await using var input = new DataStream<int>(IdleThenBurstAsync(CancellationToken.None), "input");
        var context = new PipelineContext();
        var node = new BatchingNode<int>(10, TimeSpan.FromSeconds(1));

        await using var output = await strategy.ExecuteAsync(input, node, context, "batch", CancellationToken.None);

        var batches = new List<IReadOnlyCollection<int>>();

        await foreach (var batch in output.WithCancellation(CancellationToken.None))
            batches.Add(batch);

        items.Should().Be(3);
        batches.Should().ContainSingle();
        batches[0].Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task Batcher_DisposingWhileInputIdle_DoesNotThrowAndDisposesInput()
    {
        var disposed = false;

        async IAsyncEnumerable<int> OneItemThenIdleAsync([EnumeratorCancellation] CancellationToken ct)
        {
            try
            {
                yield return 1;
                await Task.Delay(Timeout.Infinite, ct);
                yield return 2;
            }
            finally
            {
                disposed = true;
            }
        }

        // A partial batch flushed on time leaves the producer blocked on the idle input when the consumer leaves.
        var strategy = new BatchingExecutionStrategy(2, TimeSpan.FromMilliseconds(50));
        await using var input = new DataStream<int>(OneItemThenIdleAsync(CancellationToken.None), "input");
        var context = new PipelineContext();
        var scope = new FailureRecordingScope();
        context.NodeEnvironment.NodeExecutionScopeRegistry.RegisterNodeObservabilityScope("batch", scope);
        var node = new BatchingNode<int>(2, TimeSpan.FromMilliseconds(50));

        await using var output = await strategy.ExecuteAsync(input, node, context, "batch", CancellationToken.None);
        var enumerator = output.GetAsyncEnumerator(CancellationToken.None);

        (await enumerator.MoveNextAsync()).Should().BeTrue();
        enumerator.Current.Should().Equal(1);

        var act = async () => await enumerator.DisposeAsync();
        await act.Should().NotThrowAsync();
        disposed.Should().BeTrue();
        scope.RecordedFailure.Should().BeNull("a consumer leaving early is not a failure of the batching node");
    }

    [Fact]
    public void Constructor_NegativeTimespan_Throws()
    {
        var act = () => new BatchingExecutionStrategy(10, TimeSpan.FromMilliseconds(-1));

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public async Task BatchAsync_SourceFailsMidWindow_EmitsPartialBatchThenThrows()
    {
        async IAsyncEnumerable<int> TwoItemsThenFailAsync([EnumeratorCancellation] CancellationToken ct = default)
        {
            yield return 1;
            yield return 2;
            await Task.Delay(20, ct);
            throw new InvalidOperationException("source failed");
        }

        var batches = new List<IReadOnlyCollection<int>>();

        var act = async () =>
        {
            await foreach (var batch in TwoItemsThenFailAsync().BatchAsync(10, TimeSpan.FromSeconds(5)))
                batches.Add(batch);
        };

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("source failed");
        batches.Should().ContainSingle().Which.Should().Equal(1, 2);
    }

    private sealed class FailureRecordingScope : IAutoObservabilityScope
    {
        public Exception? RecordedFailure { get; private set; }

        public void RecordItemCount(long processed, long emitted)
        {
        }

        public void IncrementProcessed()
        {
        }

        public void IncrementEmitted()
        {
        }

        public void RecordFailure(Exception exception) => RecordedFailure = exception;

        public Exception? GetFailureException() => RecordedFailure;

        public void AddInputWait(TimeSpan duration)
        {
        }

        public void Dispose()
        {
        }
    }
}

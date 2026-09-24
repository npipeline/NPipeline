using System.Runtime.CompilerServices;
using AwesomeAssertions;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Execution.Strategies;

namespace NPipeline.Tests.Reliability;

/// <summary>
///     The input a restarted node resumes from: read once, holding only the items between the checkpoint and the read head.
/// </summary>
public sealed class ResumableInputTests
{
    [Fact]
    public async Task AReopenedInput_StartsAtTheCheckpoint_AndReplaysOnlyUndeliveredItems()
    {
        var source = new CountingSource(Enumerable.Range(0, 10));
        await using var input = new ResumableInput<int>(source.Stream, 100, CancellationToken.None);

        var (first, checkpoint) = input.Open(out var offset);
        offset.Should().Be(0);

        var read = await TakeAsync(first, 6);
        read.Should().Equal(0, 1, 2, 3, 4, 5);

        // Items 0-3 were delivered; 4 and 5 were read but not delivered.
        checkpoint.Advance(4);

        var (second, _) = input.Open(out offset);
        offset.Should().Be(4);
        (await DrainAsync(second)).Should().Equal(4, 5, 6, 7, 8, 9);
        source.Reads.Should().Be(10, "every item is read from the source exactly once");
    }

    [Fact]
    public async Task AFullWindow_StopsReading_UntilTheCheckpointAdvances()
    {
        var source = new CountingSource(Enumerable.Range(0, 100));
        await using var input = new ResumableInput<int>(source.Stream, 3, CancellationToken.None);
        var (stream, checkpoint) = input.Open(out _);
        await using var enumerator = stream.GetAsyncEnumerator();

        for (var i = 0; i < 3; i++)
        {
            (await enumerator.MoveNextAsync()).Should().BeTrue();
        }

        // Three items are held and none delivered, so the next read waits: backpressure, not an error.
        var next = enumerator.MoveNextAsync().AsTask();
        await Task.Delay(50);
        next.IsCompleted.Should().BeFalse();
        source.Reads.Should().Be(3);

        checkpoint.Advance(1);

        (await next.WaitAsync(TimeSpan.FromSeconds(5))).Should().BeTrue();
        enumerator.Current.Should().Be(3);
    }

    [Fact]
    public async Task OpeningAgain_EndsTheEarlierEnumeration()
    {
        await using var input = new ResumableInput<int>(new CountingSource(Enumerable.Range(0, 10)).Stream, 100, CancellationToken.None);
        var (stale, staleCheckpoint) = input.Open(out _);
        await using var staleEnumerator = stale.GetAsyncEnumerator();
        (await staleEnumerator.MoveNextAsync()).Should().BeTrue();

        var (current, _) = input.Open(out _);

        // The superseded run, such as a parallel feeder still winding down, stops rather than taking items.
        (await staleEnumerator.MoveNextAsync()).Should().BeFalse();

        // And its reports are ignored: the current run resumed from the old checkpoint and is processing those items again.
        staleCheckpoint.Advance(1);
        (await DrainAsync(current)).Should().Equal(Enumerable.Range(0, 10));
    }

    [Fact]
    public async Task AFailedSource_IsReportedAsTheInputFault()
    {
        var failure = new IOException("connection dropped");
        await using var input = new ResumableInput<int>(new DataStream<int>(FailAfterOne(failure)), 10, CancellationToken.None);
        var (stream, _) = input.Open(out _);

        var act = () => DrainAsync(stream);

        (await act.Should().ThrowAsync<IOException>()).Which.Should().BeSameAs(failure);
        input.InputFault.Should().BeSameAs(failure);
    }

    [Fact]
    public async Task Disposing_DisposesTheSourceEnumerator()
    {
        var source = new CountingSource(Enumerable.Range(0, 10));
        var input = new ResumableInput<int>(source.Stream, 10, CancellationToken.None);
        _ = await TakeAsync(input.Open(out _).Input, 2);

        await input.DisposeAsync();

        source.Disposed.Should().BeTrue();
    }

    [Fact]
    public void TheCheckpoint_MovesOverOutOfOrderCompletions_OnceTheGapIsFilled()
    {
        var advancedTo = new List<long>();
        var checkpoint = new RestartCheckpoint(10, advancedTo.Add);

        checkpoint.Complete(12);
        checkpoint.Complete(11);
        checkpoint.Watermark.Should().Be(10, "item 10 has not been delivered");

        checkpoint.Complete(10);
        checkpoint.Watermark.Should().Be(13);

        checkpoint.Advance(12);
        checkpoint.Watermark.Should().Be(13, "the checkpoint never moves back");
        advancedTo.Should().Equal(13);
    }

    private static async Task<List<int>> TakeAsync(IAsyncEnumerable<int> stream, int count)
    {
        List<int> items = [];

        await foreach (var item in stream)
        {
            items.Add(item);

            if (items.Count == count)
                break;
        }

        return items;
    }

    private static async Task<List<int>> DrainAsync(IAsyncEnumerable<int> stream)
    {
        List<int> items = [];

        await foreach (var item in stream)
        {
            items.Add(item);
        }

        return items;
    }

    private static async IAsyncEnumerable<int> FailAfterOne(Exception failure)
    {
        await Task.Yield();
        yield return 0;

        throw failure;
    }

    /// <summary>
    ///     A forward-only source that counts what is read from it and records its disposal.
    /// </summary>
    private sealed class CountingSource(IEnumerable<int> items)
    {
        private int _reads;

        public int Reads => _reads;

        public bool Disposed { get; private set; }

        public DataStream<int> Stream => new(Produce());

        private async IAsyncEnumerable<int> Produce([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            try
            {
                foreach (var item in items)
                {
                    await Task.Yield();
                    cancellationToken.ThrowIfCancellationRequested();
                    _ = Interlocked.Increment(ref _reads);
                    yield return item;
                }
            }
            finally
            {
                Disposed = true;
            }
        }
    }
}

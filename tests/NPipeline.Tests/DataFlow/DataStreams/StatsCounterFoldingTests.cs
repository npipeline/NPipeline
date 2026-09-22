using AwesomeAssertions;
using NPipeline.DataFlow.DataStreams;

namespace NPipeline.Tests.DataFlow.DataStreams;

/// <summary>
///     The counting streams used to take an <c>Interlocked.Increment</c> on the shared run counter for every item of
///     every node, which put an atomic on the hot path and made every parallel worker contend for one cache line.
///     They now count locally and fold the batch in once, so these tests pin the observable contract that survived:
///     the total is complete once a stream's enumeration ends, however it ends.
/// </summary>
public sealed class StatsCounterFoldingTests
{
    [Fact]
    public async Task CountingPassthrough_FoldsTotal_WhenEnumerationCompletes()
    {
        StatsCounter counter = new();
        await using var stream = new CountingPassthroughDataStream<int>(new DataStream<int>(Range(5), "Source"), counter);

        await foreach (var _ in stream)
        {
            // Drain.
        }

        counter.Total.Should().Be(5);
    }

    [Fact]
    public async Task CountingPassthrough_FoldsItemsSeen_WhenConsumerStopsEarly()
    {
        StatsCounter counter = new();
        await using var stream = new CountingPassthroughDataStream<int>(new DataStream<int>(Range(100), "Source"), counter);

        var seen = 0;

        await foreach (var _ in stream)
        {
            if (++seen == 3)
                break;
        }

        counter.Total.Should().Be(3);
    }

    [Fact]
    public async Task CountingPassthrough_FoldsItemsSeen_WhenTheSourceThrows()
    {
        StatsCounter counter = new();
        await using var stream = new CountingPassthroughDataStream<int>(new DataStream<int>(ThrowingAfter(2), "Source"), counter);

        var act = async () =>
        {
            await foreach (var _ in stream)
            {
                // Drain until the source faults.
            }
        };

        _ = await act.Should().ThrowAsync<InvalidOperationException>();
        counter.Total.Should().Be(2);
    }

    [Fact]
    public async Task CountingPassthrough_TotalsAcrossStreamsSharingOneCounter()
    {
        StatsCounter counter = new();

        await using var first = new CountingPassthroughDataStream<int>(new DataStream<int>(Range(4), "First"), counter);
        await using var second = new CountingPassthroughDataStream<int>(new DataStream<int>(Range(6), "Second"), counter);

        await Task.WhenAll(Drain(first), Drain(second));

        counter.Total.Should().Be(10);
    }

    private static async Task Drain(CountingPassthroughDataStream<int> stream)
    {
        await foreach (var _ in stream)
        {
            await Task.Yield();
        }
    }

    private static async IAsyncEnumerable<int> Range(int count)
    {
        for (var i = 0; i < count; i++)
        {
            yield return i;
        }

        await Task.CompletedTask;
    }

    private static async IAsyncEnumerable<int> ThrowingAfter(int count)
    {
        for (var i = 0; i < count; i++)
        {
            yield return i;
        }

        await Task.CompletedTask;
        throw new InvalidOperationException("Source faulted.");
    }
}

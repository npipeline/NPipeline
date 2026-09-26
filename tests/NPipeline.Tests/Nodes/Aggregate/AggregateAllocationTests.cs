using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.Timestamping;
using NPipeline.DataFlow.Windowing;
using NPipeline.Nodes;

namespace NPipeline.Tests.Nodes.Aggregate;

/// <summary>
///     Verifies the aggregate hot path over many items and keys: state is bounded by the live
///     (window, key) pairs and released as windows close, every group is emitted exactly once, and the steady state does
///     not allocate per item.
/// </summary>
public sealed class AggregateAllocationTests
{
    private static readonly DateTimeOffset T0 = new(2020, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private sealed record Reading(int Key, DateTimeOffset At) : ITimestamped
    {
        public DateTimeOffset Timestamp => At;
    }

    private sealed class CountingNode() : AggregateNode<Reading, int, int>(new AggregateNodeConfiguration<Reading>(
        WindowAssigner.Tumbling(TimeSpan.FromMinutes(1)), MaxOutOfOrderness: TimeSpan.Zero))
    {
        public override int GetKey(Reading item) => item.Key;

        public override int CreateAccumulator() => 0;

        public override int Accumulate(int accumulator, Reading item) => accumulator + 1;
    }

    [Fact]
    public async Task Aggregate_Over100kItemsAnd1000Keys_EmitsEveryGroupAndKeepsStateBounded()
    {
        const int itemCount = 100_000;
        const int keyCount = 1000;

        var node = new CountingNode();

        var output = (IAsyncEnumerable<object?>)(await node.ExecuteAsync(Items(itemCount, keyCount)))!;
        var emitted = 0;
        await foreach (var _ in output)
        {
            emitted++;
        }

        // Each item advances event time by 10 ms, so the stream spans about 16.7 minutes: roughly 17
        // windows, each holding one group per key. Groups are emitted once per (window, key) pair.
        const int expectedGroups = 17;
        var (totalProcessed, totalClosed, maxConcurrent) = node.GetMetrics();

        totalProcessed.Should().Be(keyCount * expectedGroups);
        totalClosed.Should().Be(keyCount * expectedGroups);
        // A new window's first group opens before the expired window closes (the watermark check runs
        // after window assignment), so the maximum reaches exactly one group above the key count.
        maxConcurrent.Should().BeLessThanOrEqualTo(keyCount + 1);
        node.GetActiveWindowCount().Should().Be(0);
        node.LateItemsDropped.Should().Be(0);
    }

    [Fact]
    public async Task Aggregate_HotPath_AllocatesLittlePerItem()
    {
        const int itemCount = 100_000;
        const int keyCount = 100;

        var node = new CountingNode();
        var items = PreallocatedItems(itemCount, keyCount);

        // Warm up JIT and type initialisation so the measurement is the steady state.
        await DrainAsync(new CountingNode(), PreallocatedItems(1_000, keyCount));

        var before = GC.GetAllocatedBytesForCurrentThread();
        await DrainAsync(node, items);
        var perItem = (GC.GetAllocatedBytesForCurrentThread() - before) / (double)itemCount;

        // The steady state reuses the window and the per-key accumulators, so only emitted results allocate. A new
        // window object per item (about 40 bytes) would fail this.
        perItem.Should().BeLessThan(8, "the aggregate's hot path must not allocate per item");
    }

    private static async Task DrainAsync(CountingNode node, IReadOnlyList<object?> items)
    {
        var output = (IAsyncEnumerable<object?>)(await node.ExecuteAsync(Replay(items)))!;

        await foreach (var _ in output)
        {
        }
    }

    private static IReadOnlyList<object?> PreallocatedItems(int count, int keyCount) =>
        [.. Enumerable.Range(0, count).Select(i => (object?)new Reading(i % keyCount, T0.AddMilliseconds(10 * i)))];

    private static async IAsyncEnumerable<object?> Replay(IReadOnlyList<object?> items)
    {
        for (var i = 0; i < items.Count; i++)
            yield return items[i];

        await Task.CompletedTask;
    }

    private static async IAsyncEnumerable<object?> Items(int count, int keyCount)
    {
        for (var i = 0; i < count; i++)
        {
            yield return new Reading(i % keyCount, T0.AddMilliseconds(10 * i));
        }

        await Task.CompletedTask;
    }
}
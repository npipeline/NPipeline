using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.Timestamping;
using NPipeline.DataFlow.Windowing;
using NPipeline.Nodes;

namespace NPipeline.Tests.Nodes.Aggregate;

/// <summary>
///     Verifies the aggregate hot path over many items and keys: state is bounded by the live
///     (window, key) pairs and released as windows close, and every group is emitted exactly once.
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
        // after window assignment), so the maximum can briefly reach one group above the key count.
        maxConcurrent.Should().BeLessThanOrEqualTo(keyCount * 2);
        node.GetActiveWindowCount().Should().Be(0);
        node.LateItemsDropped.Should().Be(0);
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
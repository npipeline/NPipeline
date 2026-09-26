using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.Timestamping;
using NPipeline.DataFlow.Windowing;
using NPipeline.Nodes;

namespace NPipeline.Tests.Nodes.Aggregate;

/// <summary>
///     Verifies that windowed aggregates assign windows and compute watermarks from the same event-time source:
///     the item's <see cref="ITimestamped" /> timestamp, or the configured extractor, or arrival time.
/// </summary>
public sealed class AggregateWatermarkTests
{
    private static readonly DateTime T0 = new(2023, 1, 1, 10, 0, 0, DateTimeKind.Utc);

    public sealed record Sale(string Cat, int Amount, DateTime At);

    private sealed class SumNode(TimestampExtractor<Sale>? extractor)
        : AggregateNode<Sale, string, int>(new AggregateNodeConfiguration<Sale>(
            WindowAssigner.Tumbling(TimeSpan.FromMinutes(1)), extractor, TimeSpan.Zero))
    {
        public override string GetKey(Sale s) => s.Cat;
        public override int CreateAccumulator() => 0;
        public override int Accumulate(int a, Sale s) => a + s.Amount;
    }

    private static async Task<List<int>> RunAggregate(SumNode node, params Sale[] items)
    {
        var output = (IAsyncEnumerable<object?>)(await node.ExecuteAsync(items.Cast<object?>().ToAsyncEnumerable()))!;
        var list = new List<int>();
        await foreach (var r in output) list.Add((int)r!);
        return list;
    }

    [Fact]
    public async Task Aggregate_WithExtractor_UsesEventTimeForWatermarks()
    {
        var res = await RunAggregate(new SumNode(s => s.At),
            new Sale("a", 1, T0), new Sale("a", 2, T0.AddSeconds(10)), new Sale("a", 3, T0.AddSeconds(20)));
        res.Should().Equal(6);
    }

    [Fact]
    public async Task Aggregate_WithoutExtractor_FallsBackToArrivalTime()
    {
        var act = () => RunAggregate(new SumNode(null), new Sale("a", 1, T0), new Sale("a", 2, T0));
        await act.Should().NotThrowAsync();
    }

    public sealed record TSale(string Cat, int Amount, DateTime At) : ITimestamped
    {
        public DateTimeOffset Timestamp => At;
    }

    private sealed class TSumNode()
        : AggregateNode<TSale, string, int>(new AggregateNodeConfiguration<TSale>(
            WindowAssigner.Tumbling(TimeSpan.FromMinutes(1)), null, TimeSpan.Zero))
    {
        public override string GetKey(TSale s) => s.Cat;
        public override int CreateAccumulator() => 0;
        public override int Accumulate(int a, TSale s) => a + s.Amount;
    }

    [Fact]
    public async Task Aggregate_LateItem_DoesNotReopenClosedWindow()
    {
        var node = new TSumNode();
        TSale[] items = [new("a", 1, T0), new("a", 2, T0.AddMinutes(2)), new("a", 4, T0.AddSeconds(10))];
        var output = (IAsyncEnumerable<object?>)(await node.ExecuteAsync(items.Cast<object?>().ToAsyncEnumerable()))!;
        var list = new List<int>();
        await foreach (var r in output) list.Add((int)r!);
        list.Should().Equal(1, 2);
        node.LateItemsDropped.Should().Be(1);
    }

    [Fact]
    public async Task Aggregate_SlidingWindow_EmitsPerWindowSums()
    {
        // Sliding(2 min, 1 min): an item at t belongs to every window of size 2 min, slid by 1 min, containing t.
        // Items at t0 (1) and t0+10s (2) land in [t0-1, t0+1) and [t0, t0+2).
        // The item at t0+90s (4) lands in [t0, t0+2) and [t0+1, t0+3).
        var node = new SlidingSumNode(s => s.At);
        var res = await RunSliding(node,
            new Sale("a", 1, T0),
            new Sale("a", 2, T0.AddSeconds(10)),
            new Sale("a", 4, T0.AddSeconds(90)));
        res.Should().BeEquivalentTo([3, 7, 4]);
    }

    private sealed class SlidingSumNode(TimestampExtractor<Sale>? extractor)
        : AggregateNode<Sale, string, int>(new AggregateNodeConfiguration<Sale>(
            WindowAssigner.Sliding(TimeSpan.FromMinutes(2), TimeSpan.FromMinutes(1)), extractor, TimeSpan.Zero))
    {
        public override string GetKey(Sale s) => s.Cat;
        public override int CreateAccumulator() => 0;
        public override int Accumulate(int a, Sale s) => a + s.Amount;
    }

    private static async Task<List<int>> RunSliding(SlidingSumNode node, params Sale[] items)
    {
        var output = (IAsyncEnumerable<object?>)(await node.ExecuteAsync(items.Cast<object?>().ToAsyncEnumerable()))!;
        var list = new List<int>();
        await foreach (var r in output) list.Add((int)r!);
        return list;
    }

    [Fact]
    public async Task Aggregate_EventTimeDrivesWindowClosing()
    {
        // With MaxOutOfOrderness = 0, a window closes as soon as an item
        // from a strictly later window arrives. Pull items one at a time to observe it.
        var node = new TSumNode();
        var items = new List<TSale>
        {
            new("a", 1, T0),
            new("a", 2, T0.AddMinutes(2)), // advances the watermark past window [t0, t0+1): closes it
        };

        var output = (IAsyncEnumerable<object?>)(await node.ExecuteAsync(items.Select<TSale, object?>(static i => i).ToAsyncEnumerable()))!;
        await using var enumerator = output.GetAsyncEnumerator();

        (await enumerator.MoveNextAsync()).Should().BeTrue("the first window closes once a later item arrives");
        enumerator.Current.Should().Be(1);
        (await enumerator.MoveNextAsync()).Should().BeTrue();
        enumerator.Current.Should().Be(2);
        (await enumerator.MoveNextAsync()).Should().BeFalse();
    }

    private sealed class NullAccumulatorNode(TimestampExtractor<Sale>? extractor)
        : AdvancedAggregateNode<Sale, string, string?, string?>(new AggregateNodeConfiguration<Sale>(
            WindowAssigner.Tumbling(TimeSpan.FromMinutes(1)), extractor, TimeSpan.Zero))
    {
        public override string GetKey(Sale s) => s.Cat;
        public override string? CreateAccumulator() => null;
        public override string? Accumulate(string? a, Sale s) => null;
        public override string? GetResult(string? accumulator) => accumulator;
    }

    [Fact]
    public async Task Aggregate_NullAccumulator_EmittedSameForWatermarkAndEndOfStream()
    {
        // C40: a group whose accumulator is legitimately null is emitted the same way whether a
        // watermark closes it (a later item moves the watermark past its window) or the end of the stream does,
        // and it never silently disappears.
        var watermarkClosed = new NullAccumulatorNode(s => s.At);
        var byWatermark = await RunNulls(watermarkClosed,
            new Sale("a", 1, T0), new Sale("b", 2, T0), new Sale("c", 3, T0.AddMinutes(2)));

        byWatermark.Should().Equal(null, null, null);

        var streamClosed = new NullAccumulatorNode(s => s.At);
        var byStreamEnd = await RunNulls(streamClosed, new Sale("a", 1, T0), new Sale("b", 2, T0));

        byStreamEnd.Should().Equal(null, null);
        streamClosed.GetActiveWindowCount().Should().Be(0);
    }

    private static async Task<List<string?>> RunNulls(NullAccumulatorNode node, params Sale[] items)
    {
        var output = (IAsyncEnumerable<object?>)(await node.ExecuteAsync(items.Cast<object?>().ToAsyncEnumerable()))!;
        var list = new List<string?>();
        await foreach (var r in output) list.Add((string?)r);
        return list;
    }

    [Fact]
    public async Task Aggregate_StateIsClearedAfterDrain()
    {
        var node = new SumNode(s => s.At);
        await RunAggregate(node, new Sale("a", 1, T0), new Sale("a", 2, T0.AddMinutes(2)));
        node.GetActiveWindowCount().Should().Be(0);
    }

    private sealed class LatenessNode(TimeSpan maxOutOfOrderness)
        : AggregateNode<Sale, string, int>(new AggregateNodeConfiguration<Sale>(
            WindowAssigner.Tumbling(TimeSpan.FromMinutes(1)), s => s.At, maxOutOfOrderness))
    {
        public override string GetKey(Sale s) => s.Cat;
        public override int CreateAccumulator() => 0;
        public override int Accumulate(int a, Sale s) => a + s.Amount;
    }

    [Fact]
    public void Aggregate_NegativeMaxOutOfOrderness_Throws()
    {
        var act = () => new LatenessNode(TimeSpan.FromMinutes(-1));

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public async Task Aggregate_ConsumerLeavingEarly_ReleasesItsActiveWindows()
    {
        var node = new SumNode(s => s.At);

        var input = new[]
        {
            new Sale("a", 1, T0), new Sale("b", 1, T0), new Sale("a", 1, T0.AddMinutes(1)), new Sale("a", 1, T0.AddMinutes(5)),
        }.Cast<object?>().ToAsyncEnumerable();

        var output = (IAsyncEnumerable<object?>)(await node.ExecuteAsync(input))!;

        await using (var enumerator = output.GetAsyncEnumerator())
        {
            (await enumerator.MoveNextAsync()).Should().BeTrue();
        }

        node.GetActiveWindowCount().Should().Be(0, "an abandoned execution's windows are no longer active");
    }

    private sealed class SlidingCountNode()
        : AggregateNode<Sale, string, int>(new AggregateNodeConfiguration<Sale>(
            WindowAssigner.Sliding(TimeSpan.FromMinutes(10), TimeSpan.FromMinutes(1)), s => s.At, TimeSpan.Zero))
    {
        public override string GetKey(Sale s) => s.Cat;
        public override int CreateAccumulator() => 0;
        public override int Accumulate(int a, Sale s) => a + 1;
    }

    [Fact]
    public async Task Aggregate_SlidingWindowAtMinValue_DoesNotThrow()
    {
        // A default timestamp (an unset field) must not fail the pipeline by computing windows before MinValue.
        var output = (IAsyncEnumerable<object?>)(await new SlidingCountNode().ExecuteAsync(
            new object?[] { new Sale("a", 1, DateTime.SpecifyKind(DateTime.MinValue, DateTimeKind.Utc)) }.ToAsyncEnumerable()))!;

        var results = new List<int>();
        await foreach (var r in output) results.Add((int)r!);

        results.Should().Equal(1);
    }

    [Fact]
    public async Task Aggregate_FastReplay_ClosesWindowsAsItGoes()
    {
        // 10,000 items over 100 one-minute windows, read in well under a second. The watermark follows event time on
        // every item, so windows close as the replay passes them instead of all being held until the stream ends.
        var node = new SumNode(s => s.At);
        var input = Enumerable.Range(0, 10_000)
            .Select(i => (object?)new Sale("a", 1, T0.AddMilliseconds(i * 600)))
            .ToAsyncEnumerable();

        var output = (IAsyncEnumerable<object?>)(await node.ExecuteAsync(input))!;
        var results = new List<int>();
        var maxActiveMidway = 0;

        await foreach (var r in output)
        {
            results.Add((int)r!);

            if (results.Count < 99)
                maxActiveMidway = Math.Max(maxActiveMidway, node.GetActiveWindowCount());
        }

        results.Should().HaveCount(100).And.AllSatisfy(sum => sum.Should().Be(100));
        maxActiveMidway.Should().BeLessThanOrEqualTo(2);
    }
}

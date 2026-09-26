using System.Collections.Concurrent;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Attributes.Nodes;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.DataFlow.Windowing;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Nodes.Join;

public sealed class TimeWindowedJoinNodeTests
{
    private static readonly DateTime BaseTime = new(2023, 1, 1, 10, 0, 0);

    [Fact]
    public async Task TimeWindowedJoinNode_WithTumblingWindow_ShouldJoinItemsInSameWindow()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<EnrichedEvent>>();
        services.AddNPipeline(typeof(TimeWindowedJoinNodeTests).Assembly);
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.CreateDefault();

        // Act
        await runner.RunAsync<TumblingWindowJoinPipeline>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<EnrichedEvent>>();
        resultStore.Should().HaveCount(2);

        resultStore.Should().BeEquivalentTo([
            new EnrichedEvent(1, "Event1", "Metadata1", new DateTime(2023, 1, 1, 10, 0, 15)),
            new EnrichedEvent(2, "Event2", "Metadata2", new DateTime(2023, 1, 1, 10, 1, 30)),
        ]);
    }

    // Test Data Models
    private sealed record Event(int Id, string Name, DateTime EventTimestamp) : ITimestamped
    {
        public DateTimeOffset Timestamp => EventTimestamp;
    }

    private sealed record EventMetadata(int Id, string Metadata, DateTime MetadataTimestamp) : ITimestamped
    {
        public DateTimeOffset Timestamp => MetadataTimestamp;
    }

    private sealed record EnrichedEvent(int Id, string? Name, string? Metadata, DateTime Timestamp);

    // Test Node Implementations

    private sealed class EventSource : SourceNode<Event>
    {
        public override IDataStream<Event> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            var events = new[]
            {
                new Event(1, "Event1", new DateTime(2023, 1, 1, 10, 0, 15)),
                new Event(2, "Event2", new DateTime(2023, 1, 1, 10, 1, 30)),
                new Event(3, "Event3", new DateTime(2023, 1, 1, 10, 2, 45)), // Outside window
            };

            return new DataStream<Event>(events.ToAsyncEnumerable(), "EventStream");
        }
    }

    private sealed class EventMetadataSource : SourceNode<EventMetadata>
    {
        public override IDataStream<EventMetadata> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            var metadata = new[]
            {
                new EventMetadata(1, "Metadata1", new DateTime(2023, 1, 1, 10, 0, 10)),
                new EventMetadata(2, "Metadata2", new DateTime(2023, 1, 1, 10, 1, 20)),
                new EventMetadata(4, "Metadata4", new DateTime(2023, 1, 1, 10, 3, 0)), // Outside window and no matching event
            };

            return new DataStream<EventMetadata>(metadata.ToAsyncEnumerable(), "MetadataStream");
        }
    }

    [KeySelector(typeof(Event), nameof(Event.Id))]
    [KeySelector(typeof(EventMetadata), nameof(EventMetadata.Id))]
    private sealed class EventEnrichmentNode()
        : TimeWindowedJoinNode<int, Event, EventMetadata, EnrichedEvent>(new TumblingWindowAssigner(TimeSpan.FromMinutes(1)))
    {
        public override EnrichedEvent CreateOutput(Event item1, EventMetadata item2) => new(item1.Id, item1.Name, item2.Metadata, item1.EventTimestamp);

        public override EnrichedEvent CreateOutputFromLeft(Event item1) => new(item1.Id, item1.Name, null, item1.EventTimestamp);

        public override EnrichedEvent CreateOutputFromRight(EventMetadata item2) => new(item2.Id, null, item2.Metadata, item2.MetadataTimestamp);
    }

    private sealed class EnrichedEventSink(ConcurrentQueue<EnrichedEvent> store) : SinkNode<EnrichedEvent>
    {
        public override async Task ConsumeAsync(IDataStream<EnrichedEvent> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                store.Enqueue(item);
            }
        }
    }

    // Test Definition

    private sealed class TumblingWindowJoinPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var eventSource = builder.AddSource<EventSource, Event>("event_source");
            var metadataSource = builder.AddSource<EventMetadataSource, EventMetadata>("metadata_source");
            var enrichmentNode = builder.AddJoin<EventEnrichmentNode, Event, EventMetadata, EnrichedEvent>("enrichment_node");
            var sink = builder.AddSink<EnrichedEventSink, EnrichedEvent>("sink");

            builder.Connect(eventSource, enrichmentNode);
            builder.Connect(metadataSource, enrichmentNode);
            builder.Connect(enrichmentNode, sink);
        }
    }

    // ---------- Direct-execution node tests ----------

    private sealed record Result(int? OrderId, string? CustomerName);

    private sealed record TimedCustomer(int CustomerId, string Name, DateTime At) : ITimestamped
    {
        public DateTimeOffset Timestamp => At;
    }

    private sealed record TimedOrder(int OrderId, int CustomerId, DateTime At) : ITimestamped
    {
        public DateTimeOffset Timestamp => At;
    }

    [KeySelector(typeof(TimedCustomer), nameof(TimedCustomer.CustomerId))]
    [KeySelector(typeof(TimedOrder), nameof(TimedOrder.CustomerId))]
    private sealed class WindowedOrderCustomerJoin()
        : TimeWindowedJoinNode<int, TimedCustomer, TimedOrder, Result>(
            new TumblingWindowAssigner(TimeSpan.FromMinutes(1)),
            maxOutOfOrderness: TimeSpan.Zero,
            watermarkInterval: TimeSpan.Zero)
    {
        public override Result CreateOutput(TimedCustomer item1, TimedOrder item2) => new(item2.OrderId, item1.Name);

        public override Result CreateOutputFromLeft(TimedCustomer item1) => new(null, item1.Name);

        public override Result CreateOutputFromRight(TimedOrder item2) => new(item2.OrderId, null);
    }

    private static async Task<List<Result>> RunAsync(IJoinNode node, params object[] items)
    {
        var output = await node.ExecuteAsync(items.ToAsyncEnumerable(), PipelineContext.CreateDefault());
        var results = new List<Result>();

        await foreach (var item in output)
        {
            results.Add((Result)item!);
        }

        return results;
    }

    [Fact]
    public async Task TimeWindowedJoin_ManyItemsInSameWindow_EmitsEveryPairing()
    {
        var node = new WindowedOrderCustomerJoin();

        var results = await RunAsync(node,
            new TimedCustomer(1, "Alice", BaseTime.AddSeconds(5)),
            new TimedOrder(10, 1, BaseTime.AddSeconds(10)),
            new TimedOrder(11, 1, BaseTime.AddSeconds(20)),
            new TimedCustomer(1, "Alicia", BaseTime.AddSeconds(25)),
            new TimedOrder(12, 1, BaseTime.AddMinutes(1).AddSeconds(5))); // Next window

        results.Should().BeEquivalentTo([
            new Result(10, "Alice"),
            new Result(11, "Alice"),
            new Result(10, "Alicia"),
            new Result(11, "Alicia"),
        ]);
    }

    [Fact]
    public async Task TimeWindowedJoin_LeftOuter_EmitsUnmatchedItemsWhenTheirWindowExpires()
    {
        var node = new WindowedOrderCustomerJoin { JoinType = JoinType.LeftOuter };

        var results = await RunAsync(node,
            new TimedCustomer(1, "Alice", BaseTime.AddSeconds(5)),
            new TimedOrder(10, 1, BaseTime.AddSeconds(10)),
            new TimedOrder(11, 1, BaseTime.AddSeconds(20)),
            new TimedCustomer(2, "Bob", BaseTime.AddSeconds(30)),
            new TimedCustomer(3, "Carol", BaseTime.AddMinutes(2))); // Advances the watermark past the first window

        results.Should().Equal(
            new Result(10, "Alice"),
            new Result(11, "Alice"),
            new Result(null, "Bob"),
            new Result(null, "Carol"));
    }

    // ---------- C12: the watermark follows the slower input ----------

    [Fact]
    public async Task TimeWindowedJoin_WatermarkFollowsTheSlowerInput()
    {
        // One input running ahead must not evict the other input's windows: the watermark is the
        // minimum of the two inputs' watermarks, so window t0 survives until the right input catches up.
        var node = new WindowedOrderCustomerJoin();

        var results = await RunAsync(node,
            new TimedCustomer(1, "Alice", BaseTime.AddSeconds(5)),
            new TimedCustomer(2, "Bob", BaseTime.AddMinutes(3)), // left input runs 3 minutes ahead
            new TimedOrder(10, 1, BaseTime.AddSeconds(10)));      // right item for window t0 still matches

        results.Should().BeEquivalentTo([
            new Result(10, "Alice"),
        ]);
    }

    [Fact]
    public async Task TimeWindowedJoin_InputThatNeverProduces_HoldsStateUntilEndOfStream()
    {
        // The watermark stays at MinValue until both inputs have produced an item. Left items spanning several
        // windows would otherwise close their windows as the left input advances; here they are all held until the
        // stream ends, then emitted as unmatched.
        var node = new WindowedOrderCustomerJoin { JoinType = JoinType.LeftOuter };
        var inputCompleted = false;

        async IAsyncEnumerable<object?> LeftOnly()
        {
            yield return new TimedCustomer(1, "A", BaseTime);
            yield return new TimedCustomer(2, "B", BaseTime.AddMinutes(3));
            yield return new TimedCustomer(3, "C", BaseTime.AddMinutes(10));
            await Task.Yield();
            inputCompleted = true;
        }

        var output = await node.ExecuteAsync(LeftOnly(), PipelineContext.CreateDefault());
        var results = new List<Result>();
        var emittedBeforeEnd = 0;

        await foreach (var item in output)
        {
            if (!inputCompleted)
                emittedBeforeEnd++;

            results.Add((Result)item!);
        }

        emittedBeforeEnd.Should().Be(0, "no window may close while the right input has produced nothing");
        results.Should().BeEquivalentTo([
            new Result(null, "A"),
            new Result(null, "B"),
            new Result(null, "C"),
        ]);
    }

    [Fact]
    public void TimeWindowedJoin_NegativeMaxOutOfOrderness_Throws()
    {
        var act = () => new NegativeLatenessJoin();

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [KeySelector(typeof(TimedCustomer), nameof(TimedCustomer.CustomerId))]
    [KeySelector(typeof(TimedOrder), nameof(TimedOrder.CustomerId))]
    private sealed class NegativeLatenessJoin()
        : TimeWindowedJoinNode<int, TimedCustomer, TimedOrder, Result>(
            new TumblingWindowAssigner(TimeSpan.FromMinutes(1)),
            maxOutOfOrderness: TimeSpan.FromMinutes(-1))
    {
        public override Result CreateOutput(TimedCustomer item1, TimedOrder item2) => new(item2.OrderId, item1.Name);
    }

    // ---------- C01: windows and watermarks use the extractors' event time ----------

    private sealed record UntimedCustomer(int CustomerId, string Name, DateTime At);

    private sealed record UntimedOrder(int OrderId, int CustomerId, DateTime At);

    [KeySelector(typeof(UntimedCustomer), nameof(UntimedCustomer.CustomerId))]
    [KeySelector(typeof(UntimedOrder), nameof(UntimedOrder.CustomerId))]
    private sealed class ExtractorWindowedJoin()
        : TimeWindowedJoinNode<int, UntimedCustomer, UntimedOrder, Result>(
            new TumblingWindowAssigner(TimeSpan.FromMinutes(1)),
            timestampExtractor1: c => c.At,
            timestampExtractor2: o => o.At,
            maxOutOfOrderness: TimeSpan.Zero,
            watermarkInterval: TimeSpan.Zero)
    {
        public override Result CreateOutput(UntimedCustomer item1, UntimedOrder item2) => new(item2.OrderId, item1.Name);

        public override Result CreateOutputFromLeft(UntimedCustomer item1) => new(null, item1.Name);

        public override Result CreateOutputFromRight(UntimedOrder item2) => new(item2.OrderId, null);
    }

    [Fact]
    public async Task TimeWindowedJoin_WithExtractors_UsesEventTimeForWindows()
    {
        // Non-ITimestamped records joined through extractors, with historical (2023) timestamps.
        // Windows and watermarks both use event time, so every pairing inside a window is emitted.
        var node = new ExtractorWindowedJoin();

        var results = await RunAsync(node,
            (object)new UntimedCustomer(1, "Alice", BaseTime.AddSeconds(5)),
            (object)new UntimedOrder(10, 1, BaseTime.AddSeconds(10)),
            (object)new UntimedOrder(11, 1, BaseTime.AddSeconds(20)),
            (object)new UntimedCustomer(1, "Alicia", BaseTime.AddSeconds(25)),
            (object)new UntimedOrder(12, 1, BaseTime.AddMinutes(1).AddSeconds(5)));

        results.Should().BeEquivalentTo([
            new Result(10, "Alice"),
            new Result(11, "Alice"),
            new Result(10, "Alicia"),
            new Result(11, "Alicia"),
        ]);
    }

    // ---------- C11: late items never pair with zombie windows ----------

    [Fact]
    public async Task TimeWindowedJoin_LateLeftItem_EmittedOnceAsUnmatched()
    {
        // The window [t0, t0+1) closes once both inputs have advanced past it (Carol and Order99 at t0+2min).
        // The late customer "Late" (window already closed) is emitted exactly once, as unmatched, and never
        // pairs with a recreated zombie window.
        var node = new WindowedOrderCustomerJoin { JoinType = JoinType.LeftOuter };

        var results = await RunAsync(node,
            new TimedCustomer(1, "Alice", BaseTime.AddSeconds(5)),
            new TimedOrder(10, 1, BaseTime.AddSeconds(10)),
            new TimedCustomer(3, "Carol", BaseTime.AddMinutes(2)),      // both inputs advance below
            new TimedOrder(99, 3, BaseTime.AddMinutes(2)),               // watermark passes window t0
            new TimedCustomer(2, "Late", BaseTime.AddSeconds(30)));      // late: window already closed

        results.Should().BeEquivalentTo([
            new Result(10, "Alice"),
            new Result(99, "Carol"),
            new Result(null, "Late"),
        ]);
        results.Count(r => r.CustomerName == "Late").Should().Be(1);
        node.LateItemsDropped.Should().Be(0, "a late item the outer join emits as unmatched is not dropped");
    }

    [Fact]
    public async Task TimeWindowedJoin_LateInnerJoinItem_IsDroppedAndCounted()
    {
        var node = new WindowedOrderCustomerJoin { JoinType = JoinType.Inner };

        var results = await RunAsync(node,
            new TimedCustomer(1, "Alice", BaseTime.AddSeconds(5)),
            new TimedOrder(10, 1, BaseTime.AddSeconds(10)),
            new TimedCustomer(3, "Carol", BaseTime.AddMinutes(2)),
            new TimedOrder(99, 3, BaseTime.AddMinutes(2)),           // watermark passes window t0
            new TimedOrder(11, 1, BaseTime.AddSeconds(20)));          // late: dropped by an inner join

        results.Should().BeEquivalentTo([
            new Result(10, "Alice"),
            new Result(99, "Carol"),
        ]);
        node.LateItemsDropped.Should().Be(1);
    }

    // ---------- C31: null keys never match ----------

    private sealed record NullableKeyCustomer(string? CustomerId, string Name, DateTime At) : ITimestamped
    {
        public DateTimeOffset Timestamp => At;
    }

    private sealed record NullableKeyOrder(int OrderId, string? CustomerId, DateTime At) : ITimestamped
    {
        public DateTimeOffset Timestamp => At;
    }

    [KeySelector(typeof(NullableKeyCustomer), nameof(NullableKeyCustomer.CustomerId))]
    [KeySelector(typeof(NullableKeyOrder), nameof(NullableKeyOrder.CustomerId))]
    private sealed class NullableKeyWindowedJoin()
        : TimeWindowedJoinNode<string, NullableKeyCustomer, NullableKeyOrder, Result>(
            new TumblingWindowAssigner(TimeSpan.FromMinutes(1)),
            maxOutOfOrderness: TimeSpan.Zero,
            watermarkInterval: TimeSpan.Zero)
    {
        public override Result CreateOutput(NullableKeyCustomer item1, NullableKeyOrder item2) => new(item2.OrderId, item1.Name);

        public override Result CreateOutputFromLeft(NullableKeyCustomer item1) => new(null, item1.Name);

        public override Result CreateOutputFromRight(NullableKeyOrder item2) => new(item2.OrderId, null);
    }

    [Theory]
    [InlineData(JoinType.Inner)]
    [InlineData(JoinType.LeftOuter)]
    [InlineData(JoinType.RightOuter)]
    [InlineData(JoinType.FullOuter)]
    public async Task TimeWindowedJoin_NullKeyLeft_DoesNotCrashAndFollowsJoinType(JoinType joinType)
    {
        var node = new NullableKeyWindowedJoin { JoinType = joinType };

        var results = await RunAsync(node,
            new NullableKeyCustomer(null, "Anon", BaseTime.AddSeconds(5)),
            new NullableKeyOrder(10, "x", BaseTime.AddSeconds(10)));

        // The null-key customer never matches. The "x" order also never matches (the only customer has a
        // null key), so it is emitted when its own side is preserved.
        var expected = new List<Result>();
        if (joinType is JoinType.LeftOuter or JoinType.FullOuter)
            expected.Add(new Result(null, "Anon"));
        if (joinType is JoinType.RightOuter or JoinType.FullOuter)
            expected.Add(new Result(10, null));

        results.Should().BeEquivalentTo(expected);
    }

    [Theory]
    [InlineData(JoinType.Inner)]
    [InlineData(JoinType.LeftOuter)]
    [InlineData(JoinType.RightOuter)]
    [InlineData(JoinType.FullOuter)]
    public async Task TimeWindowedJoin_NullKeyRight_DoesNotCrashAndFollowsJoinType(JoinType joinType)
    {
        var node = new NullableKeyWindowedJoin { JoinType = joinType };

        var results = await RunAsync(node,
            new NullableKeyCustomer("x", "Alice", BaseTime.AddSeconds(5)),
            new NullableKeyOrder(10, null, BaseTime.AddSeconds(10)));

        var expected = new List<Result>();
        if (joinType is JoinType.LeftOuter or JoinType.FullOuter)
            expected.Add(new Result(null, "Alice"));
        if (joinType is JoinType.RightOuter or JoinType.FullOuter)
            expected.Add(new Result(10, null));

        results.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public async Task TimeWindowedJoin_NullKeyOnBothSides_InnerJoinDoesNotCrashOrPair()
    {
        var node = new NullableKeyWindowedJoin { JoinType = JoinType.Inner };

        var results = await RunAsync(node,
            new NullableKeyCustomer(null, "Anon", BaseTime.AddSeconds(5)),
            new NullableKeyOrder(10, null, BaseTime.AddSeconds(10)));

        results.Should().BeEmpty("null keys never match in SQL semantics");
    }

    // ---------- C48: sliding-window per-window semantics ----------

    [KeySelector(typeof(TimedCustomer), nameof(TimedCustomer.CustomerId))]
    [KeySelector(typeof(TimedOrder), nameof(TimedOrder.CustomerId))]
    private sealed class SlidingWindowedJoin()
        : TimeWindowedJoinNode<int, TimedCustomer, TimedOrder, Result>(
            new SlidingWindowAssigner(TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(5)),
            maxOutOfOrderness: TimeSpan.Zero,
            watermarkInterval: TimeSpan.Zero)
    {
        public override Result CreateOutput(TimedCustomer item1, TimedOrder item2) => new(item2.OrderId, item1.Name);

        public override Result CreateOutputFromLeft(TimedCustomer item1) => new(null, item1.Name);

        public override Result CreateOutputFromRight(TimedOrder item2) => new(item2.OrderId, null);
    }

    [Fact]
    public async Task TimeWindowedJoin_SlidingWindow_LeftOuterEmitsItemMatchedInAnotherWindowAsUnmatched()
    {
        // A left item at t=12 lives in [5,15) and [10,20). A right item at t=17 matches it only in [10,20).
        // Per-window semantics (as in Flink): the pair is emitted once, and the left item is also emitted
        // as unmatched from [5,15) in a left-outer join.
        var node = new SlidingWindowedJoin { JoinType = JoinType.LeftOuter };

        var results = await RunAsync(node,
            new TimedCustomer(1, "Alice", BaseTime.AddSeconds(12)),
            new TimedOrder(10, 1, BaseTime.AddSeconds(17)));

        results.Should().BeEquivalentTo([
            new Result(10, "Alice"),    // matched in [10,20)
            new Result(null, "Alice"),   // unmatched in [5,15)
        ]);
    }

    [Fact]
    public async Task TimeWindowedJoin_SlidingWindow_InnerJoinEmitsPairPerSharedWindow()
    {
        // Inner joins emit one pair per window the two items share. Here the items share two windows.
        var node = new SlidingWindowedJoin { JoinType = JoinType.Inner };

        var results = await RunAsync(node,
            new TimedCustomer(1, "Alice", BaseTime.AddSeconds(12)),
            new TimedOrder(10, 1, BaseTime.AddSeconds(13)));

        results.Should().BeEquivalentTo([
            new Result(10, "Alice"),
            new Result(10, "Alice"),
        ]);
    }
}
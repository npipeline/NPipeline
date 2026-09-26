using AwesomeAssertions;
using NPipeline.Attributes.Nodes;
using NPipeline.DataFlow;
using NPipeline.DataFlow.Windowing;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Nodes.Join;

/// <summary>
///     Verifies that joins pair every item with every item on the other side that shares its key,
///     for each join type, rather than consuming a stored item on its first match.
/// </summary>
public sealed class JoinCardinalityTests
{
    private static readonly DateTime BaseTime = new(2023, 1, 1, 10, 0, 0);

    [Theory]
    [InlineData(JoinType.Inner)]
    [InlineData(JoinType.LeftOuter)]
    [InlineData(JoinType.RightOuter)]
    [InlineData(JoinType.FullOuter)]
    public async Task KeyedJoin_OneLeftItemAndManyRightItems_MatchesEveryRightItem(JoinType joinType)
    {
        var node = new OrderCustomerJoin { JoinType = joinType };

        var results = await RunAsync(node,
            new Customer(1, "Alice"),
            new Order(10, 1),
            new Order(11, 1),
            new Order(12, 1));

        results.Should().BeEquivalentTo([
            new Result(10, "Alice"),
            new Result(11, "Alice"),
            new Result(12, "Alice"),
        ]);
    }

    [Theory]
    [InlineData(JoinType.Inner)]
    [InlineData(JoinType.LeftOuter)]
    [InlineData(JoinType.RightOuter)]
    [InlineData(JoinType.FullOuter)]
    public async Task KeyedJoin_ManyRightItemsArrivingBeforeLeftItem_MatchesEveryRightItem(JoinType joinType)
    {
        var node = new OrderCustomerJoin { JoinType = joinType };

        var results = await RunAsync(node,
            new Order(10, 1),
            new Order(11, 1),
            new Customer(1, "Alice"),
            new Order(12, 1));

        results.Should().BeEquivalentTo([
            new Result(10, "Alice"),
            new Result(11, "Alice"),
            new Result(12, "Alice"),
        ]);
    }

    [Fact]
    public async Task KeyedJoin_ManyToMany_EmitsEveryPairing()
    {
        var node = new OrderCustomerJoin();

        var results = await RunAsync(node,
            new Customer(1, "Alice"),
            new Order(10, 1),
            new Customer(1, "Alicia"),
            new Order(11, 1));

        results.Should().BeEquivalentTo([
            new Result(10, "Alice"),
            new Result(10, "Alicia"),
            new Result(11, "Alice"),
            new Result(11, "Alicia"),
        ]);
    }

    [Fact]
    public async Task KeyedJoin_FullOuter_EmitsAllPairingsAndOnlyTrulyUnmatchedItems()
    {
        var node = new OrderCustomerJoin { JoinType = JoinType.FullOuter };

        var results = await RunAsync(node,
            new Customer(1, "Alice"),
            new Customer(2, "Bob"),
            new Order(10, 1),
            new Order(11, 1),
            new Order(12, 3),
            new Order(13, 1));

        results.Should().BeEquivalentTo([
            new Result(10, "Alice"),
            new Result(11, "Alice"),
            new Result(13, "Alice"),
            new Result(null, "Bob"),
            new Result(12, null),
        ]);
    }

    [Fact]
    public async Task KeyedJoin_LeftOuter_EmitsEachUnmatchedLeftItemIncludingDuplicateKeys()
    {
        var node = new OrderCustomerJoin { JoinType = JoinType.LeftOuter };

        var results = await RunAsync(node,
            new Customer(1, "Alice"),
            new Customer(1, "Alicia"),
            new Customer(2, "Bob"),
            new Order(10, 2),
            new Order(11, 3));

        results.Should().BeEquivalentTo([
            new Result(10, "Bob"),
            new Result(null, "Alice"),
            new Result(null, "Alicia"),
        ]);
    }

    [Fact]
    public async Task KeyedJoin_RightOuter_EmitsEachUnmatchedRightItemIncludingDuplicateKeys()
    {
        var node = new OrderCustomerJoin { JoinType = JoinType.RightOuter };

        var results = await RunAsync(node,
            new Order(10, 1),
            new Order(11, 1),
            new Order(12, 2),
            new Customer(2, "Bob"),
            new Customer(3, "Carol"));

        results.Should().BeEquivalentTo([
            new Result(12, "Bob"),
            new Result(10, null),
            new Result(11, null),
        ]);
    }

    [Fact]
    public async Task KeyedJoin_WhenCapacityReached_StillMatchesRetainedItemsAndEmitsUnretainedUnmatchedItems()
    {
        var node = new OrderCustomerJoin { JoinType = JoinType.FullOuter, MaxCapacity = 1 };

        var results = await RunAsync(node,
            new Customer(1, "Alice"),
            new Customer(2, "Bob"), // Not retained: left side is at capacity and nothing has matched it yet
            new Order(10, 1),
            new Order(11, 1), // Not retained, but still matches the retained customer
            new Order(12, 2)); // Bob was not retained, so this order cannot match

        results.Should().BeEquivalentTo([
            new Result(null, "Bob"),
            new Result(10, "Alice"),
            new Result(11, "Alice"),
            new Result(12, null),
        ]);
    }

    [Theory]
    [InlineData(JoinType.Inner)]
    [InlineData(JoinType.LeftOuter)]
    [InlineData(JoinType.RightOuter)]
    [InlineData(JoinType.FullOuter)]
    public async Task KeyedJoin_NullKeyLeft_DoesNotCrashAndFollowsJoinType(JoinType joinType)
    {
        // C31: in SQL semantics a null key matches nothing, and outer joins still emit the row.
        var node = new NullKeyJoin { JoinType = joinType };

        var results = await RunAsync(node,
            new NullKeyCustomer(null, "Anon"),
            new NullKeyOrder(10, "x"));

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
    public async Task KeyedJoin_NullKeyRight_DoesNotCrashAndFollowsJoinType(JoinType joinType)
    {
        var node = new NullKeyJoin { JoinType = joinType };

        var results = await RunAsync(node,
            new NullKeyCustomer("x", "Alice"),
            new NullKeyOrder(10, null));

        var expected = new List<Result>();
        if (joinType is JoinType.LeftOuter or JoinType.FullOuter)
            expected.Add(new Result(null, "Alice"));
        if (joinType is JoinType.RightOuter or JoinType.FullOuter)
            expected.Add(new Result(10, null));

        results.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public async Task KeyedJoin_NullKeysOnBothSides_NeverPair()
    {
        var node = new NullKeyJoin { JoinType = JoinType.FullOuter };

        var results = await RunAsync(node,
            new NullKeyCustomer(null, "Anon"),
            new NullKeyOrder(10, null));

        results.Should().BeEquivalentTo([
            new Result(null, "Anon"),
            new Result(10, null),
        ]);
    }

    private sealed record NullKeyCustomer(string? CustomerId, string Name);

    private sealed record NullKeyOrder(int OrderId, string? CustomerId);

    [KeySelector(typeof(NullKeyCustomer), nameof(NullKeyCustomer.CustomerId))]
    [KeySelector(typeof(NullKeyOrder), nameof(NullKeyOrder.CustomerId))]
    private sealed class NullKeyJoin : KeyedJoinNode<string, NullKeyCustomer, NullKeyOrder, Result>
    {
        public override Result CreateOutput(NullKeyCustomer item1, NullKeyOrder item2) => new(item2.OrderId, item1.Name);

        public override Result CreateOutputFromLeft(NullKeyCustomer item1) => new(null, item1.Name);

        public override Result CreateOutputFromRight(NullKeyOrder item2) => new(item2.OrderId, null);
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

    private static async Task<List<Result>> RunAsync<TNode>(TNode node, params object[] items) where TNode : IJoinNode
    {
        var output = await node.ExecuteAsync(items.ToAsyncEnumerable(), PipelineContext.CreateDefault());
        var results = new List<Result>();

        await foreach (var item in output)
        {
            results.Add((Result)item!);
        }

        return results;
    }

    private sealed record Customer(int CustomerId, string Name);

    private sealed record Order(int OrderId, int CustomerId);

    private sealed record Result(int? OrderId, string? CustomerName);

    private sealed record TimedCustomer(int CustomerId, string Name, DateTime At) : ITimestamped
    {
        public DateTimeOffset Timestamp => At;
    }

    private sealed record TimedOrder(int OrderId, int CustomerId, DateTime At) : ITimestamped
    {
        public DateTimeOffset Timestamp => At;
    }

    [KeySelector(typeof(Customer), nameof(Customer.CustomerId))]
    [KeySelector(typeof(Order), nameof(Order.CustomerId))]
    private sealed class OrderCustomerJoin : KeyedJoinNode<int, Customer, Order, Result>
    {
        public override Result CreateOutput(Customer item1, Order item2) => new(item2.OrderId, item1.Name);

        public override Result CreateOutputFromLeft(Customer item1) => new(null, item1.Name);

        public override Result CreateOutputFromRight(Order item2) => new(item2.OrderId, null);
    }

    [KeySelector(typeof(TimedCustomer), nameof(TimedCustomer.CustomerId))]
    [KeySelector(typeof(TimedOrder), nameof(TimedOrder.CustomerId))]
    private sealed class WindowedOrderCustomerJoin()
        : TimeWindowedJoinNode<int, TimedCustomer, TimedOrder, Result>(
            new TumblingWindowAssigner(TimeSpan.FromMinutes(1)),
            maxOutOfOrderness: TimeSpan.Zero)
    {
        public override Result CreateOutput(TimedCustomer item1, TimedOrder item2) => new(item2.OrderId, item1.Name);

        public override Result CreateOutputFromLeft(TimedCustomer item1) => new(null, item1.Name);

        public override Result CreateOutputFromRight(TimedOrder item2) => new(item2.OrderId, null);
    }
}

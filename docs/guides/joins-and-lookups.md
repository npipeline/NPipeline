---
title: "Joins and Lookups"
description: "Combine data from multiple streams using keyed joins, time-windowed joins, and lookups."
order: 7
---

# Joins and Lookups

> **Prerequisites:** [Defining Pipelines](defining-pipelines.md), [Key Concepts](../getting-started/key-concepts.md)

When a pipeline has multiple data sources, you often need to combine them. NPipeline provides three patterns: keyed joins, time-windowed joins, and lookups.

## Keyed Joins

A keyed join matches items from two streams by a shared key value. Define a join node by extending `KeyedJoinNode` and marking key properties with `[KeySelector]`:

```csharp
[KeySelector(typeof(Order), nameof(Order.CustomerId))]
[KeySelector(typeof(Customer), nameof(Customer.CustomerId))]
public class OrderCustomerJoin : KeyedJoinNode<int, Order, Customer, OrderWithCustomer>
{
    public override OrderWithCustomer CreateOutput(Order order, Customer customer)
        => new(order, customer);

    public override OrderWithCustomer CreateOutputFromLeft(Order order)
        => new(order, Customer: null); // For outer joins
}
```

Register and connect:

```csharp
var orders = builder.AddSource<OrderSource, Order>("orders");
var customers = builder.AddSource<CustomerSource, Customer>("customers");
var join = builder.AddJoin<OrderCustomerJoin, Order, Customer, OrderWithCustomer>("join");
var sink = builder.AddSink<ResultSink, OrderWithCustomer>("results");

builder.Connect(orders, join);     // first input
builder.Connect(customers, join);  // second input
builder.Connect(join, sink);
```

### Join Types

Set the `JoinType` property to control matching behavior:

| Type | Behavior | Unmatched Items |
|------|----------|-----------------|
| `Inner` (default) | Emit only when both sides match | Discarded |
| `LeftOuter` | Emit all left items; match right when available | Left items call `CreateOutputFromLeft` |
| `RightOuter` | Emit all right items; match left when available | Right items call `CreateOutputFromRight` |
| `FullOuter` | Emit all items from both sides | Both fallback methods called |

Each item is paired with every item on the other side that shares its key. For example, one customer and three orders with the same `CustomerId` produce three outputs. Many-to-many keys produce every pairing. Outer joins call the fallback methods only for items that never matched anything.

### Memory Limits

A keyed join can't know whether another item with the same key will arrive later, so it keeps every item from both inputs in memory until the input streams end. This applies to every join type, including `Inner`. Set `MaxCapacity` to limit how many items each input retains:

```csharp
public class MyJoin : KeyedJoinNode<int, Order, Customer, Result>
{
    public MyJoin() { MaxCapacity = 10_000; }
}
```

When an input reaches capacity, its new items are still matched against the items already retained from the other input, but aren't retained themselves. As a result, they can't match items that arrive later. If an item that isn't retained also matched nothing, and the join type keeps its side (for example, a left item in a `LeftOuter` join), the join emits it immediately as unmatched. Otherwise, the join discards it.

## Time-Windowed Joins

For streams where items arrive over time and should be matched within a time window, use `TimeWindowedJoinNode`:

```csharp
public class TradeSettlementJoin
    : TimeWindowedJoinNode<string, Trade, Settlement, MatchedTrade>
{
    public TradeSettlementJoin() : base(
        WindowAssigner.Tumbling(TimeSpan.FromMinutes(5)),
        timestampExtractor1: trade => trade.ExecutedAt,
        timestampExtractor2: settlement => settlement.SettledAt,
        maxOutOfOrderness: TimeSpan.FromMinutes(2))
    { }

    public override MatchedTrade CreateOutput(Trade trade, Settlement settlement)
        => new(trade, settlement);
}
```

Within a window, time-windowed joins pair items the same way keyed joins do. They use [watermarks](../reference/glossary.md#watermark) to close expired windows and release memory. For outer joins, the unmatched items in a window are emitted when that window closes.

## In-Memory Lookups

For enriching items from a static dictionary, use the lambda-based `AddInMemoryLookup`:

```csharp
var categories = new Dictionary<int, string>
{
    [1] = "Electronics", [2] = "Clothing", [3] = "Food"
};

var lookup = builder.AddInMemoryLookup<Product, int, string, EnrichedProduct>(
    "category-lookup",
    lookupData: categories,
    keyExtractor: product => product.CategoryId,
    outputCreator: (product, categoryName) =>
        new EnrichedProduct(product, categoryName ?? "Unknown"));

builder.Connect(source, lookup);
```

### Custom Lookup Nodes

For dynamic lookups (database, API), extend `LookupNode`:

```csharp
public class CustomerLookup : LookupNode<Order, int, Customer, EnrichedOrder>
{
    protected override int ExtractKey(Order input, PipelineContext context)
        => input.CustomerId;

    protected override async Task<Customer?> LookupAsync(
        int key, PipelineContext context, CancellationToken ct)
        => await _db.FindCustomerAsync(key, ct);

    protected override EnrichedOrder CreateOutput(
        Order input, Customer? customer, PipelineContext context)
        => new(input, customer);
}
```

## Choosing the Right Pattern

| Pattern | Use When |
|---------|----------|
| Keyed Join | Two live streams, match by key, both streams are finite or bounded |
| Time-Windowed Join | Two live streams, match by key within a time window, continuous processing |
| In-Memory Lookup | One live stream + one static reference dataset |
| Custom Lookup | One live stream + dynamic lookups (DB, API) per item |

A join reads both of its inputs concurrently, so an unbounded live input on one side does not starve the other. A consequence is that the order in which items from the two sides reach the join is not deterministic: pairing and output content are deterministic, but the order of join output across the two inputs is not.

## Self-Joins

To join a stream with itself (e.g., matching related events), use `AddSelfJoin`:

```csharp
builder.AddSelfJoin<Event, string, MatchedEvent>(
    leftSource, rightSource, "self-join",
    outputFactory: (e1, e2) => new MatchedEvent(e1, e2),
    leftKeySelector: e => e.CorrelationId);
```

## Next Steps

- [Aggregation](aggregation.md) - compute metrics over windowed data
- [Batching and Windowing](batching-and-windowing.md) - window types and watermarks
- [Branching and Merging](branching-and-merging.md) - fan-out and merge patterns

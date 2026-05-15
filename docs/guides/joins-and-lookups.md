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

### Memory Limits

For outer joins, unmatched items are held in memory until the stream ends. Set `MaxCapacity` to bound this:

```csharp
public class MyJoin : KeyedJoinNode<int, Order, Customer, Result>
{
    public MyJoin() { MaxCapacity = 10_000; }
}
```

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

Time-windowed joins use [watermarks](../reference/glossary.md#watermark) to close expired windows and release memory.

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

## Self-Joins

To join a stream with itself (e.g., matching related events), use `AddSelfJoin`:

```csharp
builder.AddSelfJoin<Event, string, MatchedEvent>(
    leftSource, rightSource, "self-join",
    outputFactory: (e1, e2) => new MatchedEvent(e1, e2),
    leftKeySelector: e => e.CorrelationId);
```

## Next Steps

- [Aggregation](aggregation.md) — compute metrics over windowed data
- [Batching and Windowing](batching-and-windowing.md) — window types and watermarks
- [Branching and Merging](branching-and-merging.md) — fan-out and merge patterns

---
title: "Aggregation"
description: "Group items by key and time window to compute running metrics and summaries."
order: 8
---

# Aggregation

> **Prerequisites:** [Batching and Windowing](batching-and-windowing.md), [Key Concepts](../getting-started/key-concepts.md)

Aggregation groups [items](../reference/glossary.md#item) by a key and a time [window](../reference/glossary.md#tumbling-window), accumulates state per group, and emits results when windows close.

## How Aggregation Works

1. Each item is assigned a **key** (via `GetKey`) and a **window** (via the configured `WindowAssigner`)
2. Per `(key, window)` pair, an **accumulator** is maintained
3. Each item updates its accumulator via `Accumulate`
4. When a [watermark](../reference/glossary.md#watermark) closes a window, `GetResult` is called and the result is emitted downstream

## Writing an Aggregate Node

Extend `AggregateNode<TIn, TKey, TResult>` when the accumulator type is the same as the result:

```csharp
public class SalesTotal : AggregateNode<Sale, string, SalesSummary>
{
    public SalesTotal() : base(new AggregateNodeConfiguration<Sale>(
        WindowAssigner.Tumbling(TimeSpan.FromHours(1)),
        TimestampExtractor: sale => sale.Timestamp)) { }

    public override string GetKey(Sale item) => item.Category;

    public override SalesSummary CreateAccumulator()
        => new(Count: 0, Total: 0m);

    public override SalesSummary Accumulate(SalesSummary acc, Sale item)
        => acc with { Count = acc.Count + 1, Total = acc.Total + item.Amount };
}
```

Register and connect:

```csharp
var source = builder.AddSource<SaleSource, Sale>("sales");
var aggregate = builder.AddAggregate<SalesTotal, Sale, string, SalesSummary>("totals");
var sink = builder.AddSink<ReportSink, SalesSummary>("report");

builder.Connect(source, aggregate);
builder.Connect(aggregate, sink);
```

## Advanced Aggregation

When the accumulator differs from the result (e.g., you accumulate a list but emit a computed summary), extend `AdvancedAggregateNode<TIn, TKey, TAccumulate, TResult>`:

```csharp
public class StatsAggregator
    : AdvancedAggregateNode<Measurement, string, RunningStats, StatsSummary>
{
    public StatsAggregator() : base(new AggregateNodeConfiguration<Measurement>(
        WindowAssigner.Sliding(TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(1)),
        TimestampExtractor: m => m.Timestamp)) { }

    public override string GetKey(Measurement item) => item.SensorId;
    public override RunningStats CreateAccumulator() => new();
    public override RunningStats Accumulate(RunningStats stats, Measurement item)
        => stats.Add(item.Value);
    public override StatsSummary GetResult(RunningStats stats)
        => new(stats.Mean, stats.StdDev, stats.Count);
}
```

## Lambda-Based Aggregation

For simple cases, use the fluent grouping API:

```csharp
// Tumbling window
var totals = builder.GroupItems<Sale>()
    .ForTemporalCorrectness(
        windowSize: TimeSpan.FromHours(1),
        keySelector: sale => sale.Category,
        initialValue: () => 0m,
        accumulator: (sum, sale) => sum + sale.Amount,
        timestampExtractor: sale => sale.Timestamp);

// Sliding window
var averages = builder.GroupItems<Measurement>()
    .ForTemporalCorrectnessWithSlidingWindows(
        windowSize: TimeSpan.FromMinutes(5),
        slide: TimeSpan.FromMinutes(1),
        keySelector: m => m.SensorId,
        initialValue: () => (Sum: 0.0, Count: 0),
        accumulator: (acc, m) => (acc.Sum + m.Value, acc.Count + 1),
        timestampExtractor: m => m.Timestamp);
```

## Configuration Options

`AggregateNodeConfiguration<TIn>` controls window behavior:

| Property | Default | Description |
|----------|---------|-------------|
| `WindowAssigner` | (required) | Tumbling or sliding window strategy |
| `TimestampExtractor` | `null` | Extracts event time from items; uses `ITimestamped.Timestamp` if null |
| `MaxOutOfOrderness` | 5 minutes | How late an item can arrive and still be included |
| `WatermarkInterval` | 30 seconds | How often watermarks advance |
| `UseThreadSafeAccumulator` | `true` | Use concurrent data structures for parallel access |

## Monitoring Aggregation

Call `GetMetrics()` on your aggregate node to inspect state:

```csharp
var (totalProcessed, totalClosed, maxConcurrent) = aggregateNode.GetMetrics();
int activeWindows = aggregateNode.GetActiveWindowCount();
```

## Next Steps

- [Batching and Windowing](batching-and-windowing.md) - window types and watermark mechanics
- [Streaming Large Datasets](streaming-large-datasets.md) - memory management for windowed aggregation
- [Parallel Execution](parallel-execution.md) - thread-safe accumulation

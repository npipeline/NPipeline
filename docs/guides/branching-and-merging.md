---
title: "Branching and Merging"
description: "Send items to multiple destinations, observe streams, and merge multiple inputs."
order: 5
---

# Branching and Merging

> **Prerequisites:** [Defining Pipelines](defining-pipelines.md), [Custom Nodes](custom-nodes.md)

Pipelines are directed acyclic graphs, not just linear chains. NPipeline supports fan-out (one source, multiple downstream paths), side-channel observation (taps), and fan-in (multiple sources merging into one node).

## Fan-Out with Connect

The simplest fan-out: call `Connect` from the same source handle to multiple targets:

```csharp
public void Define(PipelineBuilder builder, PipelineContext context)
{
    var orders = builder.AddSource<OrderSource, Order>("orders");
    var analytics = builder.AddTransform<AnalyticsProcessor, Order, AnalyticsEvent>("analytics");
    var fulfillment = builder.AddTransform<FulfillmentProcessor, Order, Shipment>("fulfillment");
    var notifications = builder.AddTransform<NotificationProcessor, Order, Alert>("notify");

    builder.Connect(orders, analytics);
    builder.Connect(orders, fulfillment);
    builder.Connect(orders, notifications);
}
```

Each downstream node receives its own independent copy of the [stream](../reference/glossary.md#stream). Items flow to all branches concurrently.

## Tap Nodes

A tap sends each [item](../reference/glossary.md#item) to a side-channel sink without affecting the main flow. The item passes through unchanged:

```csharp
var source = builder.AddSource<OrderSource, Order>("orders");
var auditTap = builder.AddTap<Order>(
    () => new AuditSink(logger), "audit");
var transform = builder.AddTransform<ProcessOrder, Order, Result>("process");

builder.Connect(source, auditTap);
builder.Connect(auditTap, transform);  // items continue downstream
```

Taps are useful for logging, metrics collection, or debugging without altering the pipeline's data flow.

You can chain multiple taps:

```csharp
builder.Connect(source, auditTap);
builder.Connect(auditTap, metricsTap);
builder.Connect(metricsTap, alertTap);
builder.Connect(alertTap, mainTransform);
```

## Branch Nodes

A branch executes one or more side-effect handlers for each item, then passes the item through:

```csharp
var branch = builder.AddBranch<Order>(async order =>
{
    await SendNotificationAsync(order);
}, "notify-branch");

// Or with multiple handlers (all execute in parallel)
var branch = builder.AddBranch<Order>(new Func<Order, Task>[]
{
    async order => await LogAsync(order),
    async order => await NotifyAsync(order),
    async order => await UpdateMetricsAsync(order),
}, "multi-branch");
```

### Error Handling in Branches

Configure how branch handler errors are handled:

| Mode | Behavior |
|------|----------|
| `RouteToErrorHandler` (default) | Errors go through the resilience policy |
| `CollectAndThrow` | All errors collected and thrown as `AggregateException` |
| `LogAndContinue` | Errors logged and swallowed |

### Branch vs Tap

| Feature | Tap | Branch |
|---------|-----|--------|
| Side channel | Full `ISinkNode<T>` | `Func<T, Task>` handlers |
| Error isolation | Sink manages its own errors | Configurable via `BranchErrorHandlingMode` |
| Use when | Sending to a persistent store | Lightweight side effects |

## Fan-In: Merging Multiple Sources

### Default Merge

When multiple sources connect to the same downstream node, items are interleaved in arrival order:

```csharp
var nyse = builder.AddSource<NyseSource, Trade>("nyse");
var nasdaq = builder.AddSource<NasdaqSource, Trade>("nasdaq");
var processor = builder.AddTransform<TradeProcessor, Trade, ProcessedTrade>("process");

builder.Connect(nyse, processor);
builder.Connect(nasdaq, processor);
```

### Custom Merge

For control over how streams are combined, extend `CustomMergeNode<T>`:

```csharp
public class PriorityMerge : CustomMergeNode<Trade>
{
    public override async Task<IDataStream<Trade>> MergeAsync(
        IEnumerable<IDataStream> pipes, CancellationToken ct)
    {
        // Custom merge logic — e.g., priority-based interleaving
        var typedPipes = pipes.Cast<IDataStream<Trade>>();
        return new DataStream<Trade>(
            MergeByPriorityAsync(typedPipes, ct), "priority-merged");
    }
}
```

Register the merge node as a preconfigured instance:

```csharp
var mergeNode = new PriorityMerge();
var merge = builder.AddTransform<PriorityMerge, Trade, Trade>("merge");
builder.AddPreconfiguredNodeInstance("merge", mergeNode);

builder.Connect(nyse, merge);
builder.Connect(nasdaq, merge);
```

> 📝 **Note:** For combining different types from two sources, use [Joins and Lookups](joins-and-lookups.md) instead.

## Next Steps

- [Joins and Lookups](joins-and-lookups.md) — combine data from different stream types
- [Batching and Windowing](batching-and-windowing.md) — group items by count or time
- [Pipeline Composition](pipeline-composition.md) — embed sub-pipelines as reusable units

---
title: "Streaming Large Datasets"
description: "Process datasets larger than available memory using lazy streams and bounded buffering."
order: 13
---

# Streaming Large Datasets

> **Prerequisites:** [Key Concepts](../getting-started/key-concepts.md), [Custom Nodes](custom-nodes.md)

NPipeline is designed for streaming. Data flows lazily through the pipeline - only one [item](../reference/glossary.md#item) at a time needs to be in memory per node. This page explains how to keep it that way when processing large datasets.

## The Streaming Default

Every `IDataStream<T>` is an `IAsyncEnumerable<T>`. When a source node returns a `DataStream<T>`, items are produced one at a time and immediately consumed by the next node:

```csharp
public override IDataStream<Order> OpenStream(
    PipelineContext context, CancellationToken ct)
{
    // Streaming: items flow one by one, constant memory
    return new DataStream<Order>(ReadOrdersAsync(ct), "orders");
}

private static async IAsyncEnumerable<Order> ReadOrdersAsync(
    [EnumeratorCancellation] CancellationToken ct)
{
    await using var reader = new StreamReader("orders.csv");
    while (await reader.ReadLineAsync(ct) is { } line)
    {
        yield return ParseOrder(line);
    }
}
```

With this pattern, a 10 GB file uses constant memory regardless of file size.

## What Breaks Streaming

### Materializing in Source Nodes

Loading all data into a collection defeats streaming:

```csharp
// ✗ Loads entire dataset into memory
public override IDataStream<Order> OpenStream(
    PipelineContext context, CancellationToken ct)
{
    var allOrders = File.ReadAllLines("orders.csv")  // entire file in memory!
        .Select(ParseOrder)
        .ToList();
    return new InMemoryDataStream<Order>(allOrders, "orders");
}
```

The `SourceNodeStreamingAnalyzer` (NP9107) warns about this pattern.

### Collecting in Transform Nodes

Buffering all items in a transform also breaks streaming:

```csharp
// ✗ Holds all items in memory
public override ValueTask<IReadOnlyList<Order>> TransformAsync(
    Order item, PipelineContext context, CancellationToken ct)
{
    _buffer.Add(item);  // unbounded growth
    // ...
}
```

## Stream Implementations

NPipeline provides several stream types, each with different memory characteristics:

| Stream | Memory | Replayable | Use Case |
|--------|--------|------------|----------|
| `DataStream<T>` | Constant | No (forward-only) | Default: lazy streaming |
| `InMemoryDataStream<T>` | O(n) | Yes | Small, bounded reference data |
| `MulticastDataStream<T>` | Bounded | N/A | Branching to multiple consumers |

### Forward-Only Streams

`DataStream<T>` implements `IForwardOnlyDataStream<T>`, signaling that it cannot be replayed. Node restart doesn't need a replayable input: it keeps only the items it may have to process again.

### Bounded Replay

A transform with node restart resumes from its checkpoint. It holds at most `NodeRestartOptions.MaxReplayWindow` items
for replay, and stops reading input while the window is full. For more information, see
[Node restart and the replay window](../error-handling/materialization.md).

## Best Practices

1. **Return `DataStream<T>` from sources** - wrap `IAsyncEnumerable<T>` in `DataStream`, not `InMemoryDataStream`
2. **Use `yield return`** - stream items lazily from I/O sources
3. **Size `MaxReplayWindow`** for node restart - it bounds how many items are held for replay
4. **Use batching for bulk I/O** - batch items before database inserts instead of holding all items
5. **Avoid LINQ in hot paths** - `.ToList()`, `.OrderBy()`, `.GroupBy()` materialize sequences. The `LinqInHotPathsAnalyzer` (NP9103) warns about this
6. **Forward cancellation tokens** - always pass `CancellationToken` and use `.WithCancellation(ct)` on async enumerables

## Monitoring Memory

With the `NPipeline.Extensions.Observability` package, enable memory metrics:

```csharp
services.AddNPipelineObservability(new ObservabilityExtensionOptions
{
    EnableMemoryMetrics = true  // samples GC.GetTotalMemory at node boundaries
});
```

## Next Steps

- [Batching and Windowing](batching-and-windowing.md) - batch items for efficient bulk operations
- [Parallel Execution](parallel-execution.md) - bounded queues and backpressure
- [Node restart and the replay window](../error-handling/materialization.md) - how node restart bounds the items it holds for replay

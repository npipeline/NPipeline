---
title: "Batching and Windowing"
description: "Group items by count, time, or event timestamps for bulk processing and temporal analysis."
order: 6
---

# Batching and Windowing

> **Prerequisites:** [Defining Pipelines](defining-pipelines.md), [Key Concepts](../getting-started/key-concepts.md)

Batching collects individual [items](../reference/glossary.md#item) into groups for bulk processing. Windowing assigns items to time-based groups for temporal analysis. Both are stream-level transforms.

## Batching

### Creating Batches

Use `AddBatcher` to group items by count with a time limit:

```csharp
var source = builder.AddSource<SensorSource, SensorReading>("sensors");
var batcher = builder.AddBatcher<SensorReading>("batch", batchSize: 100, timespan: TimeSpan.FromSeconds(5));
var bulkInsert = builder.AddTransform<BulkInsert, IReadOnlyCollection<SensorReading>, int>("insert");

builder.Connect(source, batcher);
builder.Connect(batcher, bulkInsert);
```

The batcher emits a batch when either condition is met:

- The batch reaches `batchSize` items, **or**
- The `timespan` expires since the first item in the current batch

> ⚠️ **Warning:** A large `batchSize` with a short `timespan` (or vice versa) triggers analyzer NP9004. Match the values to your workload - high throughput streams need larger batches, low throughput streams need longer time windows.

### Splitting Batches

Use `AddUnbatcher` to flatten collections back into individual items:

```csharp
var unbatcher = builder.AddUnbatcher<SensorReading>("unbatch");
builder.Connect(batchedSource, unbatcher);
```

### Intent-Driven Batching

The grouping extensions provide a higher-level API that expresses your intent:

```csharp
var batched = builder.GroupItems<Order>()
    .ForOperationalEfficiency(batchSize: 100, maxWait: TimeSpan.FromSeconds(5));
```

This creates the same underlying `BatchingNode` but makes the purpose explicit.

## Windowing

Windowing assigns items to time-based windows using event timestamps. NPipeline supports two window types.

### Tumbling Windows

Fixed-size, non-overlapping windows. Each item belongs to exactly one window:

```
Window 1: [00:00 – 01:00)
Window 2: [01:00 – 02:00)
Window 3: [02:00 – 03:00)
```

```csharp
WindowAssigner.Tumbling(TimeSpan.FromMinutes(1))
```

### Sliding Windows

Fixed-size windows that advance by a smaller slide interval. Items can belong to multiple overlapping windows:

```
Window 1: [00:00 – 00:30)
Window 2: [00:10 – 00:40)  ← overlaps with window 1
Window 3: [00:20 – 00:50)
```

```csharp
WindowAssigner.Sliding(
    windowSize: TimeSpan.FromSeconds(30),
    slide: TimeSpan.FromSeconds(10))
```

### How Windows Close

Windows are closed by [watermarks](../reference/glossary.md#watermark) - timestamps that signal "no more items earlier than this will arrive." When a watermark passes a window's end time, the window closes and emits its results.

Configure watermark behavior:

```csharp
new AggregateNodeConfiguration<SensorReading>(
    WindowAssigner.Tumbling(TimeSpan.FromMinutes(5)),
    TimestampExtractor: reading => reading.EventTime,
    MaxOutOfOrderness: TimeSpan.FromMinutes(2),   // default: 5 min
    WatermarkInterval: TimeSpan.FromSeconds(15));  // default: 30 sec
```

- **`MaxOutOfOrderness`** - how late an item can arrive and still be assigned to its window
- **`WatermarkInterval`** - how often watermarks are generated

> 📝 **Note:** Windowing is used primarily with [Aggregation](aggregation.md). See that guide for complete examples of window-based aggregation.

## Choosing Batching vs Windowing

| Batching | Windowing |
|----------|-----------|
| Groups by item count + wall-clock timeout | Groups by event timestamp |
| For operational efficiency (bulk I/O) | For temporal analysis (metrics, analytics) |
| Items belong to exactly one batch | Items may belong to multiple windows (sliding) |
| No notion of "event time" | Requires timestamps on items |

## Next Steps

- [Aggregation](aggregation.md) - compute results over windowed groups
- [Joins and Lookups](joins-and-lookups.md) - time-windowed joins
- [Streaming Large Datasets](streaming-large-datasets.md) - memory management for high-throughput streams

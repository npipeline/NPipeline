---
title: Performance Characteristics
description: Memory usage, throughput, and scalability of NPipeline.
sidebar_position: 8
---

# Performance Characteristics

**This page explains WHAT the performance characteristics are for different patterns.** For HOW TO optimize your pipeline, see [Advanced Topics](../advanced-topics/index.md).

NPipeline is designed from the ground up for performance. Understanding its characteristics helps you build efficient pipelines.

## Memory Usage

### Streaming Model

Memory usage is proportional to the number of items **in flight**, not total dataset size:

```text
Lazy Evaluation (NPipeline):
Item 1: [Read] → [Transform] → [Write] → [GC] → Item 2
Memory: ~1 item worth of data at any time

Eager Evaluation (.ToList()):
[All items in memory] → Process all → [GC]
Memory: ~N × item_size for N items
```

**Real-world Example:**

```csharp
// Processing 1 million CSV records (500 bytes each)
// Streaming: ~1-2 MB peak memory (1-2 items buffered)
// Eager (.ToList()): ~500 MB+ required
var pipeline = PipelineBuilder
    .AddSourceNode<CsvSourceNode>()
    .AddTransformNode<TransformNode>()
    .AddSinkNode<SinkNode>()
    .BuildPipeline();
```

### Memory Per Item

The memory used per item in the pipeline is minimal:

```csharp
// Memory footprint:
// - Source item: varies (100 bytes - 10 KB typical)
// - Transform: composes items, minimal overhead
// - Sink: determines lifetime of item reference
```

## Throughput Characteristics

### Sequential Processing

```text
Time:    0ms         10ms        20ms        30ms
         ↓           ↓           ↓           ↓
Item 1: [Read]→[Trans]→[Write]
Item 2:              [Read]→[Trans]→[Write]
Item 3:                         [Read]→[Trans]→[Write]

Throughput: 1 item / 10ms = 100 items/second
```

### Parallel Processing

Using `ParallelismExtension`:

```text
Time:    0ms         10ms        20ms        30ms
         ↓           ↓           ↓           ↓
Item 1: [Read]→[Trans]→[Write]
Item 2: [Read]→[Trans]→[Write]
Item 3: [Read]→[Trans]→[Write]

Throughput: 3 items / 10ms = 300 items/second (3x speedup)
```

**Implementation:**

```csharp
var pipeline = PipelineBuilder
    .AddSourceNode<SourceNode>()
    .AddTransformNode<SlowTransform>(parallelism: 4)
    .AddSinkNode<SinkNode>()
    .BuildPipeline();
```

## Scalability

### Vertical Scaling

Scale within a single machine:

```csharp
// Use parallelism for CPU-bound transforms
.AddTransformNode<CpuIntensiveTransform>(parallelism: Environment.ProcessorCount)

// Use batching for IO-bound transforms
.AddNode(new BatchNode<Item>(batchSize: 100))
.AddTransformNode<DatabaseInsertTransform>()
```

### Horizontal Scaling

Scale across multiple machines:

```csharp
// Partition source data
var machineId = GetMachineId();
var totalMachines = GetTotalMachines();

var pipeline = PipelineBuilder
    .AddSourceNode(new PartitionedSourceNode(machineId, totalMachines))
    .AddTransformNode<TransformNode>()
    .AddSinkNode(new CentralizedSinkNode()) // Write to shared storage
    .BuildPipeline();
```

## Comparative Performance

### NPipeline vs Alternatives

| Aspect | NPipeline | LINQ Streaming | Message Queues | Direct Iteration |
|--------|-----------|----------------|----------------|------------------|
| **Memory** | O(k) active items* | O(1) per item | O(batch) | O(N) all items |
| **Latency** | < 1ms first item | < 1ms first item | 10-100ms | N/A (batch) |
| **Setup** | Low | Low | High | Very Low |
| **Typed Composition** | Yes | Yes | Weak | No |
| **Error Handling** | Flexible | Basic | Rich | None |
| **Observability** | Built-in | Limited | Rich | None |

*k = number of items actively in the pipeline's processing stages at any given time (typically 1-2 for sequential execution, k = parallelism factor for parallel execution). This is independent of total dataset size N.

## Optimization Tips

### 1. Use Async/Await Properly

```csharp
// Good - respects async model
public async IAsyncEnumerable<Output> ProcessAsync(
    Input input,
    [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
    var result = await _service.ProcessAsync(input, cancellationToken);
    yield return result;
}

// Bad - blocks thread
public async IAsyncEnumerable<Output> ProcessAsync(
    Input input,
    [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
    var result = _service.Process(input); // Blocks! Use await instead
    yield return result;
}
```

### 2. Batch Expensive Operations

```csharp
// Process 10,000 items one-by-one: 10,000 DB roundtrips (slow)
.AddTransformNode<SingleInsertTransform>()

// vs batch them: 100 DB roundtrips (100x faster)
.AddNode(new BatchNode<Order>(batchSize: 100))
.AddTransformNode<BatchInsertTransform>()
```

### 3. Avoid Materialization

```csharp
// Bad - materializes everything
.AddNode(new MaterializationNode<Item>())
.AddTransformNode<Transform>()

// Good - processes streaming
.AddTransformNode<Transform>()
```

### 4. Use Parallelism for CPU Work

```csharp
// CPU-bound: parallelize
.AddTransformNode<JsonParsingTransform>(parallelism: 8)

// IO-bound: lower parallelism needed
.AddTransformNode<DatabaseQueryTransform>(parallelism: 2)
```

## Benchmarking

Run your own benchmarks with realistic data:

```csharp
var stopwatch = Stopwatch.StartNew();

var pipeline = BuildPipeline();
var context = PipelineContext.Default;
var result = await runner.ExecuteAsync(pipeline, context);

stopwatch.Stop();
Console.WriteLine($"Processed {result.ItemsProcessed} items in {stopwatch.ElapsedMilliseconds}ms");
Console.WriteLine($"Throughput: {result.ItemsProcessed / stopwatch.Elapsed.TotalSeconds:F0} items/sec");
```

## Next Steps

- **[Extension Points](extension-points.md)** - Build custom nodes optimized for your use case
- **[Advanced Topics - Performance Hygiene](../advanced-topics/performance-hygiene.md)** - Deep dive into performance optimization


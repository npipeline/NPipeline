---
title: "Parallelism"
description: "Parallel execution strategies with configurable backpressure, queue policies, order preservation, and thread safety."
order: 8
---

# Parallelism

The `NPipeline.Extensions.Parallelism` package provides parallel execution strategies for transform nodes. Configure the degree of parallelism, backpressure policy, queue bounds, and whether output order is preserved.

## Installation

```bash
dotnet add package NPipeline.Extensions.Parallelism
```

## Quick Start

```csharp
var source = builder.AddSource(...);
var transform = builder.AddTransform<MyNode, TIn, TOut>("transform");
var sink = builder.AddSink(...);

builder.Connect(source, transform);
builder.Connect(transform, sink);

builder.WithParallelOptions(transform, new ParallelOptions
{
    MaxDegreeOfParallelism = 8,
    MaxQueueLength = 1000,
    QueuePolicy = BoundedQueuePolicy.Block,
    PreserveOrdering = true
});
```

## Execution Strategies

All strategies are built on lightweight `System.Threading.Channels` with a fixed pool of worker tasks.

| Strategy | Behavior |
|----------|----------|
| `ParallelExecutionStrategy` (default) | Facade that picks the concrete strategy from the configured queue policy |
| `BlockingParallelStrategy` | Blocking backpressure - pauses producer when the in-flight window is full; restores input order by default |
| `DropNewestParallelStrategy` | Discards incoming items when queue is full |
| `DropOldestParallelStrategy` | Discards oldest queued items to make room |

## Configuration

### ParallelOptions

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `MaxDegreeOfParallelism` | `int?` | `null` (processor count) | Maximum concurrent workers |
| `MaxQueueLength` | `int?` | `null` (unbounded) | Bound on total in-flight items (queued + processing + buffered) |
| `QueuePolicy` | `BoundedQueuePolicy` | `Block` | What happens when queue is full |
| `OutputBufferCapacity` | `int?` | `null` | Output buffer for end-to-end throttling |
| `PreserveOrdering` | `bool` | `true` | Maintain input order in output via a reorder buffer |
| `MetricsInterval` | `TimeSpan?` | `null` (1 second) | Metrics reporting interval |
| `EnableInputWaitTiming` | `bool` | `false` | Opt-in per-item input-wait timing attribution |

### Workload Type Presets

For common workload patterns, use `RunParallel` with a preset to get automatically optimized settings:

```csharp
builder
    .AddTransform<MyTransform, Input, Output>()
    .RunParallel(builder, ParallelWorkloadType.IoBound);
```

| Workload Type | DOP | Queue | Buffer | Best For |
|---------------|-----|-------|--------|----------|
| `General` (default) | `ProcessorCount × 2` | `ProcessorCount × 4` | `ProcessorCount × 8` | Mixed CPU and I/O |
| `CpuBound` | `ProcessorCount` | `ProcessorCount × 2` | `ProcessorCount × 4` | Math, parsing, compression |
| `IoBound` | `ProcessorCount × 4` | `ProcessorCount × 8` | `ProcessorCount × 16` | File I/O, database calls |
| `NetworkBound` | `min(ProcessorCount × 8, 100)` | `200` | `400` | HTTP, remote services |

### Builder API

For fine-grained control beyond presets:

```csharp
builder
    .AddTransform<MyTransform, Input, Output>()
    .RunParallel(builder, opt => opt
        .MaxDegreeOfParallelism(8)
        .MaxQueueLength(100)
        .DropOldestOnBackpressure()
        .OutputBufferCapacity(50)
        .AllowUnorderedOutput()
        .MetricsInterval(TimeSpan.FromSeconds(2)));
```

`ParallelOptionsBuilder` methods:

| Method | Description |
|--------|-------------|
| `MaxDegreeOfParallelism(int)` | Set concurrent workers |
| `MaxQueueLength(int)` | Set input queue capacity |
| `BlockOnBackpressure()` | Block producer when full (default) |
| `DropOldestOnBackpressure()` | Discard oldest queued items |
| `DropNewestOnBackpressure()` | Discard incoming items |
| `OutputBufferCapacity(int)` | Limit output buffer size |
| `AllowUnorderedOutput()` | Disable order preservation |
| `EnableInputWaitTiming()` | Opt in to per-item input-wait timing |
| `MetricsInterval(TimeSpan)` | Set metrics reporting interval |

### Comparison: Configuration Methods

| Approach | Lines | Best For |
|----------|-------|----------|
| Preset API: `.RunParallel(builder, WorkloadType.IoBound)` | 1 | Common patterns, prototyping |
| Builder API: `.RunParallel(builder, opt => opt.MaxDegreeOfParallelism(8))` | 2–3 | Custom needs |
| Manual: `builder.WithParallelOptions(handle, new ParallelOptions { ... })` | 5–6 | Advanced tuning |

### Queue Policies

| Policy | When Full | Use Case |
|--------|-----------|----------|
| `Block` | Producer waits | Most pipelines - ensures no data loss |
| `DropNewest` | Incoming items discarded | Real-time streams where freshness matters less than throughput |
| `DropOldest` | Oldest queued items discarded | Real-time streams where freshness matters most |

When item-level lineage is enabled and `LineageOptions.EmitBackpressureDropRecords` is `true`, dropped items produce terminal lineage records with `OutcomeReason = DroppedByBackpressure` for queryable drop visibility.

### Order Preservation

| Aspect | `PreserveOrdering: true` (default) | `PreserveOrdering: false` |
|--------|-------------------------------------|---------------------------|
| Throughput | Good | Excellent |
| Output order | Matches input | May be out of order |
| Memory | Higher (buffering) | Lower |
| Latency | Higher (waits for slow items) | Lower (emits immediately) |

Disable ordering when downstream processing doesn't depend on input order:

```csharp
// Shorthand extension - blocking backpressure, no data loss, completion-order output
transform.WithUnorderedParallelism(builder, maxDegreeOfParallelism: 16, maxQueueLength: 100);

// Or via options
builder.WithParallelOptions(transform, new ParallelOptions
{
    MaxDegreeOfParallelism = 16,
    PreserveOrdering = false
});
```

## Thread Safety

> **CRITICAL:** `PipelineContext.Items`, `Parameters`, and `Properties` dictionaries are **NOT thread-safe**. Do not access them from parallel worker threads.

### The Unsafe Pattern

```csharp
// ❌ WRONG - data race across worker threads
public override async Task<int> TransformAsync(int input, PipelineContext context, CancellationToken ct)
{
    var count = context.Items.GetValueOrDefault("processed", 0);
    context.Items["processed"] = count + 1;  // ← DATA RACE
    return input;
}
```

Thread A reads `count = 5`, Thread B reads `count = 5` (before A writes), both write `6` - one update is lost.

### The Safe Pattern

```csharp
// ✅ CORRECT - use atomic operations or locks
public class SafeTransform : TransformNode<int, int>
{
    private long _processedCount = 0;

    public override async Task<int> TransformAsync(int input, PipelineContext context, CancellationToken ct)
    {
        Interlocked.Increment(ref _processedCount);
        return input * 2;
    }
}
```

### Three Approaches to Shared State

| Approach | When to Use |
|----------|-------------|
| `Interlocked` | Simple counters and flags |
| `lock` | Short critical sections with multiple operations |
| `IPipelineStateManager` | Complex state that needs coordination or persistence |

```csharp
// Atomic operations for counters
Interlocked.Increment(ref _counter);
Interlocked.Add(ref _sum, input);

// Locks for short critical sections
lock (_syncLock) { _total += input; }

// State manager for coordination
var stateManager = context.StateManager;
stateManager?.MarkNodeCompleted(context.CurrentNodeId, context);
```

### Thread Safety Rules

**DO:**

- Process independent data items in parallel (inherently safe)
- Use `Interlocked` for atomic counter operations
- Use `lock` for simple critical sections - keep them short
- Use `IPipelineStateManager` for persistent shared state

**DON'T:**

- Access `context.Items` or `context.Parameters` from worker threads
- Share mutable state between nodes without synchronization
- Hold locks across `await` calls (causes deadlocks or contention)
- Create complex multi-step interlocked sequences (use locks instead)

## Validation

NPipeline includes a `ParallelConfigurationRule` that validates parallel settings at build time:

```csharp
var result = builder.Validate();

if (result.Warnings.Count > 0)
    foreach (var warning in result.Warnings)
        Console.WriteLine($"⚠️  {warning}");
```

### Validation Rules

| Rule | Trigger | Fix |
|------|---------|-----|
| Queue limits with high parallelism | DOP > 4 without `MaxQueueLength` | Set `MaxQueueLength` to 2–10× DOP |
| Order preservation overhead | `PreserveOrdering` with DOP > 8 | Use `.AllowUnorderedOutput()` if order is not needed |
| Drop policies without queue bounds | `DropOldest`/`DropNewest` without `MaxQueueLength` | Set `MaxQueueLength` |
| Thread explosion | DOP > `ProcessorCount × 4` | Reduce DOP or verify workload requires it |

### Quick Fix Examples

```csharp
// ⚠️ High parallelism without queue limits
new ParallelOptions { MaxDegreeOfParallelism = 16 }

// ✅ Fix: bound the queue
new ParallelOptions { MaxDegreeOfParallelism = 16, MaxQueueLength = 100 }

// ⚠️ Preserving order with high parallelism
.RunParallel(builder, opt => opt.MaxDegreeOfParallelism(16))

// ✅ Fix: disable ordering for throughput
.RunParallel(builder, opt => opt.MaxDegreeOfParallelism(16).AllowUnorderedOutput())
```

## Metrics

`ParallelExecutionMetrics` tracks per-node metrics:

| Metric | Description |
|--------|-------------|
| `Processed` | Total items completed |
| `Enqueued` | Total items queued |
| `DroppedNewest` | Items dropped (DropNewest policy) |
| `DroppedOldest` | Items dropped (DropOldest policy) |
| `RetryEvents` | Total retry attempts |
| `ItemsWithRetry` | Items that required at least one retry |
| `MaxItemRetryAttempts` | Highest retry count for a single item |

Metrics are reported to the `IExecutionObserver` at `MetricsInterval`. Adjust the interval based on your monitoring needs:

```csharp
// Fine-grained monitoring for real-time systems
new ParallelOptions { MetricsInterval = TimeSpan.FromMilliseconds(500) }

// Reduced overhead for batch processing
new ParallelOptions { MetricsInterval = TimeSpan.FromSeconds(10) }
```

## Best Practices

### Choosing Degree of Parallelism

- Start with `ProcessorCount` for CPU-bound work
- Use `ProcessorCount × 2–4` for I/O-bound work
- Profile to find the optimal balance - too high causes context switching overhead
- Start small (DOP = 2) and increase incrementally while monitoring

### Bounding Queues

- **Always** set `MaxQueueLength` for DOP > 4 to prevent unbounded memory growth
- Rule of thumb: `MaxQueueLength` = 2–10× `MaxDegreeOfParallelism`
- Use `OutputBufferCapacity` to limit how far ahead parallel nodes get

### Resource Contention

- Use connection pooling for databases
- Implement rate limiting for external APIs
- Consider batching requests to reduce contention
- Avoid blocking I/O in worker threads - use `async`/`await`

### Debugging

- Include thread IDs in log messages: `logger.LogInformation("Item {Id} on thread {ThreadId}", item.Id, Environment.CurrentManagedThreadId)`
- Use structured logging with correlation IDs
- Monitor queue depths and worker utilization

### Performance Optimization Checklist

- [ ] Profile baseline before adding parallelism
- [ ] Choose appropriate workload type or DOP
- [ ] Set queue limits to prevent unbounded growth
- [ ] Disable order preservation when not needed
- [ ] Implement thread-safe shared state access
- [ ] Validate configuration before production deployment
- [ ] Monitor metrics and tune accordingly

### Common Pitfalls

| Pitfall | Consequence | Fix |
|---------|-------------|-----|
| Over-parallelization | Context switching, thread starvation | Profile and find optimal DOP |
| Unsynchronized shared state | Data races, silent corruption | Use `Interlocked`, `lock`, or `IPipelineStateManager` |
| Unbounded queues | Out-of-memory under load | Set `MaxQueueLength` |
| Unnecessary ordering | Higher latency and memory | Set `PreserveOrdering = false` |
| Blocking calls in workers | Thread pool starvation | Use async/await |

## Example: Multi-Stage Pipeline

```csharp
public class FileProcessingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(new CsvSourceNode<RawRecord>(uri), "source");

        // File I/O stage - I/O-bound
        var read = builder.AddTransform<FileReaderNode, RawRecord, FileContent>("read");
        read.RunParallel(builder, ParallelWorkloadType.IoBound);

        // Parse stage - CPU-bound
        var parse = builder.AddTransform<ParserNode, FileContent, ParsedData>("parse");
        parse.RunParallel(builder, ParallelWorkloadType.CpuBound);

        // Upload stage - network-bound
        var upload = builder.AddTransform<UploaderNode, ParsedData, UploadResult>("upload");
        upload.RunParallel(builder, ParallelWorkloadType.NetworkBound);

        var sink = builder.AddSink(new DatabaseSinkNode<UploadResult>(config), "sink");

        builder.Connect(source, read);
        builder.Connect(read, parse);
        builder.Connect(parse, upload);
        builder.Connect(upload, sink);
    }
}
```

## See Also

- [Parallel Execution Guide](../guides/parallel-execution.md) - step-by-step walkthrough
- [Extensions Overview](index.md)

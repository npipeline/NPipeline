---
title: "Parallel Execution"
description: "Run transform nodes concurrently with configurable degree of parallelism and backpressure."
order: 12
---

# Parallel Execution

> **Prerequisites:** [Defining Pipelines](defining-pipelines.md), [Custom Nodes](custom-nodes.md)

By default, NPipeline processes items sequentially through each [node](../reference/glossary.md#node). The `NPipeline.Extensions.Parallelism` package lets you run transform nodes concurrently with bounded parallelism and backpressure control.

## Installation

```bash
dotnet add package NPipeline.Extensions.Parallelism
```

## Quick Start: Workload Presets

The simplest approach uses workload presets that set sensible defaults:

```csharp
var transform = builder.AddTransform<CallApi, Request, Response>("api-call");
transform.RunParallel(builder, ParallelWorkloadType.IoBound);
```

| Preset | DOP | Queue Length | Best For |
|--------|-----|-------------|----------|
| `General` | `CPU × 2` | `CPU × 4` | Mixed workloads |
| `CpuBound` | `CPU` | `CPU × 2` | CPU-intensive transforms |
| `IoBound` | `CPU × 4` | `CPU × 8` | Database, HTTP, file I/O |
| `NetworkBound` | `min(CPU × 8, 100)` | `200` | High-latency network calls |

## Custom Configuration

Use the fluent builder for fine-grained control:

```csharp
transform.RunParallel(builder, opt => opt
    .MaxDegreeOfParallelism(8)
    .MaxQueueLength(50)
    .BlockOnBackpressure()
    .OutputBufferCapacity(200)
    .AllowUnorderedOutput());
```

### Key Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `MaxDegreeOfParallelism` | varies by preset | Max concurrent items being processed |
| `MaxQueueLength` | varies by preset | Max items waiting in the input queue |
| `PreserveOrdering` | `true` | Output items in the same order as input |
| `OutputBufferCapacity` | unset | Buffer size for output items |
| `MetricsInterval` | unset | How often to report throughput metrics |

## Backpressure Policies

When the input queue is full, three policies control what happens:

```csharp
// Block upstream until queue has space (default)
opt.BlockOnBackpressure();

// Drop the oldest queued item to make room
opt.DropOldestOnBackpressure();

// Drop the newest incoming item
opt.DropNewestOnBackpressure();
```

| Policy | Data Loss | Use When |
|--------|-----------|----------|
| `Block` (default) | None | Every item must be processed |
| `DropOldest` | Oldest items | Real-time systems where stale data is less valuable |
| `DropNewest` | Newest items | Rate-limiting with preference for older items |

> ⚠️ **Warning:** `DropOldest` and `DropNewest` silently discard items. Use them only when data loss is acceptable, and always monitor dropped-item counts.

## Explicit Strategy Methods

For maximum control, use the strategy-specific extension methods:

```csharp
// Blocking with explicit parameters
transform.WithBlockingParallelism(builder,
    maxDegreeOfParallelism: 4,
    maxQueueLength: 100,
    outputBufferCapacity: 200);

// Drop-oldest
transform.WithDropOldestParallelism(builder,
    maxDegreeOfParallelism: 4,
    maxQueueLength: 100);

// Drop-newest
transform.WithDropNewestParallelism(builder,
    maxDegreeOfParallelism: 4,
    maxQueueLength: 100);
```

## Thread Safety

When using parallel execution, your transform node's `TransformAsync` method is called concurrently from multiple threads. You must ensure:

1. **No shared mutable state** - or protect it with locks/concurrent collections
2. **No direct writes to context dictionaries** - see the safety matrix below
3. **All dependencies are thread-safe** - `HttpClient` (via `IHttpClientFactory`), connection pools, etc.

### Context Dictionary Thread Safety

Thread safety of context dictionaries depends on the [optimization profile](optimization-profiles.md):

**Default profile** - dictionaries are `ConcurrentDictionary` (thread-safe):

| Dictionary | Read | Write | Notes |
|-----------|------|-------|-------|
| `context.Parameters` | Safe | Safe | Concurrent reads and writes supported |
| `context.Items` | Safe | Safe | Concurrent reads and writes supported |
| `context.Properties` | Safe | Safe | Framework-managed; avoid writing from nodes |

**HighThroughput profile** - dictionaries are plain `Dictionary` (not thread-safe):

| Dictionary | Read | Write | Notes |
|-----------|------|-------|-------|
| `context.Parameters` | Safe | **Unsafe** | Populated before execution, treat as read-only |
| `context.Items` | **Unsafe** | **Unsafe** | Use `IPipelineStateManager` for shared state |
| `context.Properties` | Safe | **Unsafe** | Framework-managed; do not write from nodes |

### Dictionary Implementation by Profile

In the `Default` profile, context dictionaries use `ConcurrentDictionary<string, object>` internally. This eliminates the most common source of bugs when developers first enable parallel execution - concurrent writes to `context.Items` no longer throw or corrupt data.

In the `HighThroughput` profile, context dictionaries use pooled `Dictionary<string, object>` instances for zero locking overhead. This avoids memory barriers on every dictionary access, which matters at millions of operations per second. The trade-off is that concurrent writes are unsafe - use `IPipelineStateManager` for shared state in parallel scenarios.

### Safe and Unsafe Patterns

```csharp
// ✓ Safe: no shared state
public override Task<Result> TransformAsync(
    Input item, PipelineContext context, CancellationToken ct)
{
    return Task.FromResult(new Result(item.Value * 2));
}

// ✓ Safe: atomic operations for simple counters
private int _count;
public override Task<Result> TransformAsync(
    Input item, PipelineContext context, CancellationToken ct)
{
    Interlocked.Increment(ref _count);
    return Task.FromResult(new Result(item.Value));
}

// ✗ Unsafe: shared mutable state without synchronization
private int _count;
public override Task<Result> TransformAsync(
    Input item, PipelineContext context, CancellationToken ct)
{
    _count++; // Race condition!
    return Task.FromResult(new Result(item.Value));
}
```

### IPipelineStateManager

For complex shared state across parallel nodes, implement `IPipelineStateManager`:

```csharp
public interface IPipelineStateManager
{
    ValueTask CreateSnapshotAsync(PipelineContext context, CancellationToken ct, bool forceFullSnapshot = false);
    ValueTask<bool> TryRestoreAsync(PipelineContext context, CancellationToken ct);
    void MarkNodeCompleted(string nodeId, PipelineContext context);
    void MarkNodeError(string nodeId, PipelineContext context);
}
```

This interface provides checkpoint/restore semantics for pipeline state, enabling safe state management with parallel execution and resilience features. The framework calls `CreateSnapshotAsync` before node execution and `TryRestoreAsync` on retry, so your shared state can be rolled back after failures.

> 📝 **Note:** Ordering guarantees come with a performance cost. Set `AllowUnorderedOutput()` if downstream nodes don't depend on item order.

## Combining with Resilience

Parallel execution and resilience work together. The `ResilientExecutionStrategy` wraps the parallel strategy:

```csharp
var transform = builder.AddTransform<MyTransform, In, Out>("transform");
transform.RunParallel(builder, ParallelWorkloadType.IoBound);
transform.WithResilience(builder);
builder.WithRetryOptions(transform, new PipelineRetryOptions { MaxItemRetries = 3 });
```

## Next Steps

- [Parallelism Extension Reference](../extensions/parallelism.md) - strategies, options, metrics, and queue policies
- [Streaming Large Datasets](streaming-large-datasets.md) - memory management with parallel processing
- [Pipeline Validation](pipeline-validation.md) - analyzer rules for parallel configuration
- [Error Handling](../error-handling/index.md) - resilience with parallel nodes

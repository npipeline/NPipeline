---
title: "Performance Best Practices"
description: "Guidelines for building high-throughput pipelines with minimal overhead."
order: 2
---

# Performance Best Practices

> **Prerequisites:** [Defining Pipelines](../guides/defining-pipelines.md), [Custom Nodes](../guides/custom-nodes.md)

NPipeline includes [analyzers](../analyzers/index.md) that detect many of these issues at build time.

## Do's

### Use ValueTask Fast Paths for Synchronous Transforms

If your transform completes synchronously (e.g., mapping, filtering, simple calculations), override `ExecuteValueTaskAsync` to avoid `Task` allocations:

```csharp
public class ToUpper : TransformNode<string, string>
{
    public override Task<string> TransformAsync(
        string item, PipelineContext ctx, CancellationToken ct)
        => Task.FromResult(item.ToUpperInvariant());

    // Fast path - avoids Task allocation
    protected internal override ValueTask<string> ExecuteValueTaskAsync(
        string item, PipelineContext ctx, CancellationToken ct)
        => new(item.ToUpperInvariant());
}
```

See [Synchronous Fast Paths](synchronous-fast-paths.md) for details.

### Stream Large Datasets

Use `DataStream<T>` lazy streaming rather than materializing entire datasets:

```csharp
// Good - streams one item at a time
public override DataStream<Record> OpenStream(PipelineContext ctx, CancellationToken ct)
    => DataStream.FromAsyncEnumerable(ReadRecordsAsync(ct));

// Bad - loads everything into memory
public override DataStream<Record> OpenStream(PipelineContext ctx, CancellationToken ct)
    => DataStream.FromEnumerable(ReadAllRecords()); // OOM risk
```

### Use Batching for I/O-Bound Operations

Batch database writes and API calls to amortize per-call overhead:

```csharp
handle.WithBatching(builder, new BatchingOptions { BatchSize = 100 });
```

### Use Parallel Execution for CPU-Bound Transforms

```csharp
handle.WithParallelExecution(builder, options =>
    options.WithDegreeOfParallelism(Environment.ProcessorCount));
```

## Don'ts

### Avoid LINQ in Hot Paths

LINQ allocates enumerator objects and delegates on every call. In `TransformAsync` methods that run per-item, use loops instead:

```csharp
// Bad - allocates on every item (NP9103)
var filtered = items.Where(x => x.IsValid).ToList();

// Good - no allocations
foreach (var item in items)
{
    if (item.IsValid) results.Add(item);
}
```

### Avoid Blocking on Async Code

Never use `.Result`, `.Wait()`, or `.GetAwaiter().GetResult()` in nodes (NP9101):

```csharp
// Bad - deadlock risk and thread pool starvation
var data = httpClient.GetAsync(url).Result;

// Good
var data = await httpClient.GetAsync(url, ct);
```

### Avoid String Concatenation in Loops

Use `StringBuilder` instead of `+` in hot paths (NP9104):

```csharp
// Bad - O(n²) allocations
foreach (var item in items) result += item.ToString();

// Good
var sb = new StringBuilder();
foreach (var item in items) sb.Append(item);
```

### Avoid Anonymous Objects in Hot Paths

Anonymous object creation causes GC pressure (NP9105). Use records or structs instead.

## Analyzer-Backed Rules

NPipeline's Roslyn analyzers enforce these practices at build time:

| Rule | Severity | What It Catches |
|------|----------|----------------|
| NP9101 | Warning | Blocking on async (.Result, .Wait()) |
| NP9103 | Warning | LINQ in TransformAsync hot paths |
| NP9104 | Warning | String concatenation in loops |
| NP9105 | Warning | Anonymous object allocations in hot paths |
| NP9106 | Info | Missing ValueTask fast path override |

See [Build-Time Analyzers](../analyzers/index.md) for the complete list.

## Performance Characteristics

### Memory Model

NPipeline streams data lazily via `IAsyncEnumerable<T>`. Memory usage is proportional to items **in flight**, not total dataset size:

```
Streaming (default):
  Item 1: [Read] → [Transform] → [Write] → [GC] → Item 2
  Memory: O(k) where k = items in flight (typically 1–2)

Eager (.ToList()):
  [All N items in memory] → Process → [GC]
  Memory: O(N) - entire dataset
```

For a 1 million row CSV at 500 bytes per row: streaming uses ~1–2 MB; `.ToList()` requires ~500 MB.

### Throughput

**Sequential** (default): items processed end-to-end, one at a time. Throughput = 1 item per processing cycle.

**Parallel** (with `NPipeline.Extensions.Parallelism`): multiple items processed concurrently. Throughput scales with `MaxDegreeOfParallelism` for CPU-bound work, and higher for I/O-bound work (where threads are mostly waiting).

### Built-In Optimizations

| Optimization | Impact | Configuration |
|-------------|--------|---------------|
| **Context caching** | ~150–250μs saved per 1K items by caching retry options, tracer, and logger at node scope | Automatic |
| **ValueTask fast path** | Up to 90% reduction in GC pressure for synchronous transforms | Override `ExecuteValueTaskAsync` |
| **Compiled expression factories** | Node instantiation as fast as `new()` after first call | Automatic |
| **Execution plan caching** | Skips type inspection on repeated pipeline runs | Automatic (disable with `WithoutExecutionPlanCache()`) |
| **Object pooling** | Reuses common collection types during orchestration | Automatic |

### NPipeline vs Alternatives

| Aspect | NPipeline | LINQ Streaming | Message Queues | Manual Iteration |
|--------|-----------|----------------|----------------|------------------|
| **Memory** | O(k) active items | O(1) per item | O(batch) | O(N) all items |
| **Latency to first item** | < 1ms | < 1ms | 10–100ms | N/A (batch) |
| **Typed composition** | Yes | Yes | Weak | No |
| **Error handling** | Retry, skip, dead-letter, circuit breaker | Basic try/catch | Rich (platform-specific) | Manual |
| **Observability** | Built-in extension | Limited | Rich (platform-specific) | Manual |
| **Parallel execution** | Bounded with backpressure | PLINQ (unbounded) | Consumer groups | Manual threading |

## Next Steps

- [Synchronous Fast Paths](synchronous-fast-paths.md) - eliminate Task allocations
- [Execution Plan Caching](execution-plan-caching.md) - avoid reflection on repeated runs
- [Parallel Execution](../guides/parallel-execution.md) - scale CPU-bound work

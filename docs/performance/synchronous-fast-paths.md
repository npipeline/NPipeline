---
title: "Synchronous Fast Paths"
description: "Avoid Task allocations for synchronous transforms using ValueTask and ExecuteValueTaskAsync."
order: 3
---

# Synchronous Fast Paths

> **Prerequisites:** [Custom Nodes](../guides/custom-nodes.md)

Many transforms complete synchronously - mapping one type to another, filtering, simple calculations. By default, these still allocate a `Task<T>` on the heap. NPipeline provides a `ValueTask<T>` fast path that eliminates this allocation.

## The Problem

Every call to `Task.FromResult(value)` allocates a `Task<T>` object. In a pipeline processing millions of items, this creates significant GC pressure:

```csharp
// Allocates a Task<string> on every call
public override Task<string> TransformAsync(
    string item, PipelineContext ctx, CancellationToken ct)
    => Task.FromResult(item.ToUpperInvariant());
```

## The Solution: ExecuteValueTaskAsync

`TransformNode<TIn, TOut>` implements the internal `IValueTaskTransform<TIn, TOut>` interface. Override `ExecuteValueTaskAsync` to return a `ValueTask<T>` that completes synchronously without allocation:

```csharp
public class ToUpper : TransformNode<string, string>
{
    public override Task<string> TransformAsync(
        string item, PipelineContext ctx, CancellationToken ct)
        => Task.FromResult(item.ToUpperInvariant());

    protected internal override ValueTask<string> ExecuteValueTaskAsync(
        string item, PipelineContext ctx, CancellationToken ct)
        => new(item.ToUpperInvariant()); // no allocation
}
```

Both methods must be implemented. `TransformAsync` is the contract required by `ITransformNode<TIn, TOut>`. `ExecuteValueTaskAsync` is the optimization the execution engine uses when available.

## How the Engine Uses It

The `SequentialExecutionStrategy` checks for `IValueTaskTransform<TIn, TOut>` once per node:

```csharp
var valueTaskTransform = node as IValueTaskTransform<TIn, TOut>;
```

Then for each item, it calls the fast path when available:

```csharp
var result = valueTaskTransform?.ExecuteValueTaskAsync(item, context, ct)
             ?? new ValueTask<TOut>(node.TransformAsync(item, context, ct));
```

If `ExecuteValueTaskAsync` completes synchronously, the result is extracted without any `Task` allocation. The `PerItemRetryExecutor` and other execution services use the same pattern.

## When to Use

Override `ExecuteValueTaskAsync` when your transform:

- Performs no I/O (no HTTP calls, no database queries, no file access)
- Completes synchronously in the common case
- Is called per-item at high throughput

> **Tip:** Analyzer rule **NP9106** detects transforms that use `Task.FromResult` but don't override `ExecuteValueTaskAsync`, and suggests the optimization.

## When NOT to Use

Keep using `TransformAsync` alone when your transform:

- Performs async I/O (the ValueTask would just wrap an incomplete Task anyway)
- Has complex async logic with multiple awaits
- Is not on a hot path

## Bridging Back to Task

If you need to call async code conditionally, use the `FromValueTask` helper to bridge back:

```csharp
protected internal override ValueTask<Result> ExecuteValueTaskAsync(
    Order item, PipelineContext ctx, CancellationToken ct)
{
    if (item.IsCached)
        return new(item.CachedResult); // sync fast path

    return new(TransformAsync(item, ctx, ct)); // fall back to async
}
```

## Related Optimizations

### CachedNodeExecutionContext

The execution engine creates a `CachedNodeExecutionContext` struct once per node, caching the node ID, retry options, and feature flags. This avoids per-item dictionary lookups in `PipelineContext`.

### Object Pooling

`PipelineObjectPool` pools common collection types (`List<string>`, `Dictionary<string, object>`, `HashSet<string>`, etc.) to reduce GC pressure during pipeline orchestration. This is handled automatically by the framework.

## Next Steps

- [Execution Plan Caching](execution-plan-caching.md) - avoid reflection on repeated runs
- [Performance Best Practices](best-practices.md) - broader optimization guidance

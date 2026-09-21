---
title: "Synchronous Fast Paths"
description: "How TransformAsync avoids per-item allocations when your transform completes synchronously."
order: 3
---

# Synchronous Fast Paths

> **Prerequisites:** [Custom Nodes](../guides/custom-nodes.md)

Many transforms complete synchronously - mapping one type to another, filtering, simple calculations. `TransformAsync`
returns `ValueTask<T>` precisely so those transforms allocate nothing per item. There is nothing to opt into.

## Why ValueTask

A `Task<T>` is a heap object. Returning one from a transform that already has its answer allocates once per item, which
in a pipeline processing millions of items is pure GC pressure for no benefit. A `ValueTask<T>` that completes
synchronously is a struct: no allocation at all.

```csharp
public class ToUpper : TransformNode<string, string>
{
    public override ValueTask<string> TransformAsync(
        string item, PipelineContext ctx, CancellationToken ct)
        => ValueTask.FromResult(item.ToUpperInvariant());   // no allocation
}
```

## Writing an Async Transform

A transform that genuinely awaits is written exactly as you would expect. Mark it `async` and return `ValueTask<T>`;
the compiler does the rest:

```csharp
public class Enrich : TransformNode<Order, EnrichedOrder>
{
    private readonly ICustomerApi _api;

    public Enrich(ICustomerApi api) => _api = api;

    public override async ValueTask<EnrichedOrder> TransformAsync(
        Order item, PipelineContext ctx, CancellationToken ct)
    {
        var customer = await _api.GetCustomerAsync(item.CustomerId, ct);
        return new EnrichedOrder(item, customer);
    }
}
```

## Mixing Both

The common shape is a cache hit that returns immediately and a miss that awaits. One method covers both, and only the
miss allocates:

```csharp
public override async ValueTask<Result> TransformAsync(
    Order item, PipelineContext ctx, CancellationToken ct)
{
    if (_cache.TryGetValue(item.Id, out var cached))
        return cached;                       // synchronous, no allocation

    var result = await _service.ComputeAsync(item, ct);
    _cache[item.Id] = result;
    return result;
}
```

## The One Rule

A `ValueTask<T>` may be consumed **once**. Await it directly, return it, or pass it straight to another call - do not
store it in a local and await it twice. This matters only if you hold a `ValueTask` yourself; the pipeline consumes
what your transform returns exactly once.

If you need a `Task<T>` - to pass to `Task.WhenAll`, or to an API that demands one - call `.AsTask()` on it.

## Related Optimizations

### CachedNodeExecutionContext

The execution engine creates a `CachedNodeExecutionContext` struct once per node, caching the node ID, retry options,
and feature flags. This avoids per-item dictionary lookups in `PipelineContext`.

## Next Steps

- [Execution Plan Caching](execution-plan-caching.md) - avoid reflection on repeated runs
- [Performance Best Practices](best-practices.md) - broader optimization guidance

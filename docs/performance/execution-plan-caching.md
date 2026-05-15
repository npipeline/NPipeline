---
title: "Execution Plan Caching"
description: "Cache pre-bound execution plans to eliminate reflection overhead on repeated pipeline runs."
order: 4
---

# Execution Plan Caching

> **Prerequisites:** [Defining Pipelines](../guides/defining-pipelines.md)

When a pipeline runs, NPipeline builds a `NodeExecutionPlan` for each node — pre-binding delegates for source, transform, join, sink, and aggregate execution. This eliminates reflection during the hot execution path, but the plan-building step itself involves type inspection. Execution plan caching stores these plans so subsequent runs of the same pipeline skip the setup work entirely.

## How It Works

1. **First run:** The pipeline graph is built, a SHA256 hash is computed from node IDs, types, edges, and execution strategies. `NodeExecutionPlan` objects are created for each node and cached under the composite key `(TypeName, GraphHash)`.
2. **Subsequent runs:** If the pipeline definition type and graph hash match a cached entry, the pre-bound plans are reused. No type inspection or delegate binding occurs.
3. **Cache invalidation:** If you change the pipeline definition (add/remove nodes, change connections), the graph hash changes and a new entry is cached.

## Default Behavior

Execution plan caching is **enabled by default**. `PipelineRunner.Create()` uses `InMemoryPipelineExecutionPlanCache` with these characteristics:

- Maximum 100 cached entries
- Approximate LRU eviction when the limit is reached
- Lock-free cache hits; locks only during eviction
- Thread-safe via `ConcurrentDictionary`

## Configuration

### Custom Cache

```csharp
var runner = PipelineRunner.CreateBuilder()
    .WithExecutionPlanCache(new InMemoryPipelineExecutionPlanCache())
    .Build();
```

### Disabling Caching

```csharp
var runner = PipelineRunner.CreateBuilder()
    .WithoutExecutionPlanCache()
    .Build();
```

This uses `NullPipelineExecutionPlanCache.Instance`, which always misses and stores nothing.

### Custom Implementation

Implement `IPipelineExecutionPlanCache` for distributed or persistent caching:

```csharp
public interface IPipelineExecutionPlanCache
{
    int Count { get; }

    bool TryGetCachedPlans(
        Type pipelineDefinitionType,
        PipelineGraph graph,
        out Dictionary<string, NodeExecutionPlan>? cachedPlans);

    void CachePlans(
        Type pipelineDefinitionType,
        PipelineGraph graph,
        Dictionary<string, NodeExecutionPlan> plans);

    void Clear();
}
```

## When Caching Is Skipped

Caching is automatically bypassed when the pipeline graph contains pre-configured node instances (`PreconfiguredNodeInstances.Count > 0`). Pre-configured instances are nodes supplied directly rather than resolved from DI, which means their execution plans can't be reused safely across runs.

## Next Steps

- [Synchronous Fast Paths](synchronous-fast-paths.md) — eliminate per-item Task allocations
- [Performance Best Practices](best-practices.md) — broader optimization guidance

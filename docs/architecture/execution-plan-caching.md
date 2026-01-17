---
title: Execution Plan Caching
description: How NPipeline caches compiled execution plans to eliminate repetitive expression compilation overhead.
sidebar_position: 14
---

# Execution Plan Caching: Eliminating Compilation Overhead

This page provides a deep dive into how NPipeline caches compiled execution plans to eliminate repetitive expression compilation overhead. Understanding execution plan caching helps you reason about performance characteristics and configuration options.

## The Problem: Compilation Overhead

When a pipeline runs, NPipeline compiles execution plans for each node:

```csharp
// For each pipeline run:
// 1. Compile expression trees for source initialization
// 2. Compile delegates for transform execution
// 3. Build execution plans for sinks and joins
```

For pipelines that run multiple times:

- **Small datasets (1K items):** 1.6-1.9ms of per-run overhead (mostly compilation)
- **Impact:** This represents ~40-50% of total pipeline execution time for small datasets
- **Root cause:** Expression tree compilation happens even though the pipeline structure never changes

## The Solution: Cache Compiled Plans

Instead of recompiling on every run, NPipeline caches the compiled execution plans keyed by pipeline definition type and graph structure:

```csharp
// First run: Compile and cache
var runner = PipelineRunner.Create();
await runner.RunAsync<MyPipeline>(); // 1.9ms (with compilation)

// Subsequent runs: Use cached plans
await runner.RunAsync<MyPipeline>(); // 0.4-0.5ms (cached)
```

**Expected Performance Improvement:**

| Scenario | Before Caching | After Caching | Improvement |
|----------|---|---|---|
| First Run | 1.9ms | 1.9ms | — |
| Subsequent Runs | 1.9ms | 0.4-0.5ms | **75% reduction** |
| Per-run overhead | 300-500μs | 50μs | **250-450μs saved** |

---

## Architecture: The Caching System

### Cache Interface (`IPipelineExecutionPlanCache`)

The caching system is built on a flexible abstraction that allows different implementations:

```csharp
public interface IPipelineExecutionPlanCache
{
    /// <summary>
    /// Attempts to retrieve cached execution plans for the specified pipeline.
    /// </summary>
    bool TryGetCachedPlans(
        Type pipelineDefinitionType,
        PipelineGraph graph,
        out Dictionary<string, NodeExecutionPlan>? cachedPlans);

    /// <summary>
    /// Stores execution plans in the cache.
    /// </summary>
    void CachePlans(
        Type pipelineDefinitionType,
        PipelineGraph graph,
        Dictionary<string, NodeExecutionPlan> plans);

    /// <summary>
    /// Clears all cached plans.
    /// </summary>
    void Clear();

    /// <summary>
    /// Gets the number of cached pipeline definitions.
    /// </summary>
    int Count { get; }
}
```

### Default Implementation: `InMemoryPipelineExecutionPlanCache`

The default cache provides:

- **Thread-safe caching** via `ConcurrentDictionary`
- **Automatic cache keying** based on pipeline type and graph structure
- **Zero configuration** - works out of the box
- **Observable** - check cache count for diagnostics

**Cache Key Generation:**

Cache keys are generated from:

1. **Pipeline definition type** - Full name for type distinction
2. **Graph structure hash** - SHA256 hash of:
   - Node IDs and types
   - Input/output types
   - Edge connections
   - Execution strategies

This ensures:

- Structurally identical pipelines **share** cached plans
- Any structural change **invalidates** the cache automatically
- No manual cache invalidation needed
- Type-safe - mismatched types can't accidentally share plans

### Null Cache: `NullPipelineExecutionPlanCache`

For scenarios where caching should be disabled:

```csharp
var runner = new PipelineRunnerBuilder()
    .WithoutExecutionPlanCache()
    .Build();

// No caching occurs
await runner.RunAsync<MyPipeline>();
```

The null cache implementation:

- Always reports cache misses
- Discards any plans provided to it
- Has zero overhead when caching is disabled

---

## How It Works: Execution Flow

### First Run: Compilation & Caching

```
1. PipelineRunner.RunAsync() called
2. Check cache for compiled plans
3. Cache miss ✗
4. Call executionCoordinator.BuildPlans()
   └─ Compile expression trees for all nodes
   └─ Create delegates for transforms/sinks
   └─ Build execution plan dictionary
5. Store plans in cache
6. Execute pipeline with cached plans
```

### Subsequent Runs: Cache Hit

```
1. PipelineRunner.RunAsync() called
2. Check cache for compiled plans
3. Cache hit ✓
4. Retrieve cached plans immediately
5. Execute pipeline with cached plans
   └─ No expression compilation
   └─ No reflection overhead
```

---

## Safety: When Caching Is Disabled

Caching is **automatically disabled** for scenarios where it wouldn't be safe:

### 1. Pipelines with Preconfigured Nodes

If a pipeline has preconfigured node instances, caching is skipped:

```csharp
var pipeline = builder.Build();

// If graph has preconfigured instances:
graph.PreconfiguredNodeInstances.Count > 0  // ← Cache disabled

// Why: Preconfigured nodes may have state that changes between runs
// The compiled plan might reference mutable state
```

### 2. Explicit Null Cache

When explicitly disabled:

```csharp
var runner = new PipelineRunnerBuilder()
    .WithoutExecutionPlanCache()
    .Build();

// Cache disabled - plans recompiled every run
await runner.RunAsync<MyPipeline>();
```

### 3. Null Cache Implementation

When using the null cache intentionally:

```csharp
var runner = new PipelineRunnerBuilder()
    .WithExecutionPlanCache(NullPipelineExecutionPlanCache.Instance)
    .Build();

// Effectively disables caching
```

---

## Usage Patterns

### 1. Default (Automatic Caching)

**No configuration needed** - caching is enabled by default:

```csharp
// Caching enabled automatically
var runner = PipelineRunner.Create();

// First execution: compiles and caches
await runner.RunAsync<DataProcessingPipeline>();

// Second execution: uses cache (75% faster!)
await runner.RunAsync<DataProcessingPipeline>();
```

### 2. Custom Cache with Monitoring

Track cache effectiveness:

```csharp
var cache = new InMemoryPipelineExecutionPlanCache();
var runner = new PipelineRunnerBuilder()
    .WithExecutionPlanCache(cache)
    .Build();

// Monitor cache size
Console.WriteLine($"Cached pipelines: {cache.Count}");

// Clear cache if needed
cache.Clear();
```

### 3. Disabling Caching

For testing or dynamic pipelines:

```csharp
var runner = new PipelineRunnerBuilder()
    .WithoutExecutionPlanCache()
    .Build();

// Fresh compilation every run
// Useful for: testing, dynamic pipelines, debugging
await runner.RunAsync<MyPipeline>();
```

### 4. Custom Cache Implementation

For distributed scenarios:

```csharp
public class RedisPipelineExecutionPlanCache : IPipelineExecutionPlanCache
{
    private readonly IConnectionMultiplexer _redis;

    public bool TryGetCachedPlans(Type type, PipelineGraph graph, 
        out Dictionary<string, NodeExecutionPlan>? plans)
    {
        var key = GenerateCacheKey(type, graph);
        var cached = _redis.GetDatabase().StringGet(key);
        
        if (cached.HasValue)
        {
            plans = JsonSerializer.Deserialize<Dictionary<string, NodeExecutionPlan>>(cached);
            return true;
        }
        
        plans = null;
        return false;
    }

    public void CachePlans(Type type, PipelineGraph graph, 
        Dictionary<string, NodeExecutionPlan> plans)
    {
        var key = GenerateCacheKey(type, graph);
        var json = JsonSerializer.Serialize(plans);
        _redis.GetDatabase().StringSet(key, json, 
            expiry: TimeSpan.FromHours(24));
    }

    public void Clear()
    {
        // Clear Redis cache
        _redis.GetDatabase().FlushAll();
    }

    public int Count => _redis.GetDatabase().Keys().Count();
}

// Usage
var cache = new RedisPipelineExecutionPlanCache(redisConnection);
var runner = new PipelineRunnerBuilder()
    .WithExecutionPlanCache(cache)
    .Build();

// Cache is shared across instances/processes!
```

---

## When to Disable Caching

### Testing

When unit testing pipeline behavior that requires isolated compilation (stateful tests, benchmarks):

```csharp
[Fact]
public async Task StatefulNode_MaintainsStateAcrossItems()
{
    // Use helper from NPipeline.Tests.Common for convenience
    var runner = TestCachingHelpers.CreateRunnerWithoutCaching();

    // Each test gets fresh compilation, ensuring isolation
    await runner.RunAsync<TestPipeline>();
}
```

For the majority of tests, caching can remain enabled (default behavior). Different pipeline types/structures result in automatic cache misses, providing natural test isolation:

```csharp
[Fact]
public async Task DataFlow_ProcessesCorrectly()
{
    // Standard approach - caching enabled but with natural isolation
    var runner = new PipelineRunnerBuilder()
        .WithPipelineFactory(pipelineFactory)
        .WithNodeFactory(nodeFactory)
        .Build();

    await runner.RunAsync<MyPipeline>();
}
```

### Dynamic Pipelines

When pipeline structure changes frequently:

```csharp
// Pipeline structure varies based on user input
var builder = new PipelineBuilder();
ApplyUserDefinedStages(builder); // Structure changes each time

var runner = new PipelineRunnerBuilder()
    .WithoutExecutionPlanCache()
    .Build();
```

### Memory Constraints

When running thousands of unique pipeline definitions:

```csharp
// Generating many different pipeline types dynamically
// Caching would cause unbounded memory growth
var runner = new PipelineRunnerBuilder()
    .WithoutExecutionPlanCache()
    .Build();

// Or implement a custom cache with LRU eviction
```

### Debugging Compilation

When troubleshooting expression tree compilation:

```csharp
// Disable cache to see compilation overhead
var runner = new PipelineRunnerBuilder()
    .WithoutExecutionPlanCache()
    .Build();

// Profile to measure compilation time
var sw = Stopwatch.StartNew();
await runner.RunAsync<MyPipeline>();
sw.Stop();
Console.WriteLine($"Execution time: {sw.ElapsedMilliseconds}ms");
```

---

## Performance Characteristics

### Memory Usage

**Per-pipeline cache entry:**

- Small pipelines (3-5 nodes): ~1-2 KB
- Medium pipelines (10-20 nodes): ~5-10 KB
- Large pipelines (50+ nodes): ~20-50 KB

**Total cache overhead:**

- Typical application (5-10 pipelines): 10-100 KB
- Large application (50+ pipelines): 1-5 MB

### Cache Hit Rate

Cache effectiveness depends on pipeline reuse:

- **Batch processing:** 100% hit rate after first run (pipeline runs many times)
- **Request-driven:** 95%+ hit rate (same pipelines serve many requests)
- **Dynamic pipelines:** 0% hit rate (each structure is unique)

### CPU Impact

**Expression Compilation (per pipeline run without cache):**

- Cost: 300-500μs
- GC allocations: Several KB per pipeline
- CPU time: ~1-2% of typical pipeline execution

**Cache Lookup (per pipeline run with cache):**

- Cost: &lt;1μs (hash table lookup)
- GC allocations: None
- CPU time: Negligible

---

## Integration Points

### PipelineRunner

The pipeline runner accepts an optional cache:

```csharp
public sealed class PipelineRunner(
    // ... other dependencies ...
    IPipelineExecutionPlanCache? executionPlanCache = null) : IPipelineRunner
{
    private readonly IPipelineExecutionPlanCache _executionPlanCache = 
        executionPlanCache ?? new InMemoryPipelineExecutionPlanCache();
}
```

### PipelineExecutionCoordinator

The coordinator implements the caching logic:

```csharp
public Dictionary<string, NodeExecutionPlan> BuildPlansWithCache(
    Type pipelineDefinitionType,
    PipelineGraph graph,
    IReadOnlyDictionary<string, INode> nodeInstances,
    IPipelineExecutionPlanCache cache)
{
    // Try cache first
    if (cache.TryGetCachedPlans(pipelineDefinitionType, graph, out var cached))
        return cached;

    // Cache miss - compile and store
    var plans = nodeInstantiationService.BuildPlans(graph, nodeInstances);
    cache.CachePlans(pipelineDefinitionType, graph, plans);
    
    return plans;
}
```

---

## FAQ

**Q: Is caching enabled by default?**

A: Yes. Caching is enabled by default with the `InMemoryPipelineExecutionPlanCache`. It can be explicitly disabled via `WithoutExecutionPlanCache()`.

**Q: Should I always cache?**

A: Yes, for most applications. Cache only when:

- Testing requires fresh compilation
- Pipeline structure changes frequently  
- Memory is severely constrained
- You're debugging compilation

**Q: What's the cache key based on?**

A: The cache key includes:

1. Pipeline definition type (full name)
2. Graph structure hash (nodes, edges, types, strategies)

This means structurally identical pipelines share cached plans, but any structural change creates a new cache entry.

**Q: Is it thread-safe?**

A: Yes. `InMemoryPipelineExecutionPlanCache` uses `ConcurrentDictionary` for thread-safe operations.

**Q: Can I use a distributed cache?**

A: Yes! Implement `IPipelineExecutionPlanCache` and pass it to `PipelineRunnerBuilder.WithExecutionPlanCache()`.

**Q: What if my pipeline graph changes?**

A: Cache miss - the hash changes, so a new plan is compiled and cached separately.

**Q: Can different pipeline types share cached plans?**

A: No - the cache key includes the pipeline definition type for isolation.

---

## See Also

- [Pipeline Runner Configuration](../core-concepts/pipeline-execution/ipipelinerunner.md) - Configuring the pipeline runner
- [Optimization Principles](./optimization-principles.md) - Performance design decisions
- [Performance Characteristics](./performance-characteristics.md) - Understanding pipeline performance
- [Advanced Topics](../advanced-topics/index.md) - Performance tuning and optimization

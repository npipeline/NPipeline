# Thread Safety Guidelines

NPipeline is designed for high-performance streaming data processing. This document clarifies thread safety requirements and best practices.

## Overview

**NPipeline is primarily designed for single-threaded pipeline execution.** However, it provides extensive support for parallel processing of individual data items. Understanding the distinction is critical.

## Single-Pipeline Execution (Default)

In the most common scenario, a single pipeline processes a stream of data items sequentially:

```csharp
var context = new PipelineContext();
var pipeline = builder.Build();
await pipeline.ExecuteAsync(dataSource, context);
```

In this case:

- ✅ No synchronization needed
- ✅ Direct access to `context.Items`, `context.Parameters`, and `context.Properties` is safe
- ✅ All operations are single-threaded
- ✅ Maximum performance with zero overhead

## Parallel Node Execution

When using parallel execution strategies, NPipeline processes **independent data items** concurrently:

```csharp
var context = new PipelineContext();
var pipeline = builder
    .AddNode(sourceNode)
    .AddNode(transformNode)
    .ConfigureParallel(parallelNode, options => options.WithMaxDegreeOfParallelism(4))
    .AddNode(sinkNode)
    .Build();
```

### Critical Distinction

Each worker thread processes **different data items**, not shared state. The `PipelineContext` itself is not shared across threads—only node instances and immutable configuration are shared.

### Shared State During Parallel Execution

If you need **shared mutable state** during parallel execution, **DO NOT** access `context.Items` or `context.Parameters` directly. Instead:

#### Option 1: Use IPipelineStateManager (Recommended)

```csharp
// During context setup
var stateManager = new MyThreadSafeStateManager();
context.Properties["NPipeline.StateManager"] = stateManager;

// In your transform node
public override async ValueTask<TOut> TransformAsync(TIn input, PipelineContext context, CancellationToken ct)
{
    var stateManager = context.StateManager;
    if (stateManager != null)
    {
        // Thread-safe operations
        var state = await stateManager.GetStateAsync("myKey", ct);
        // ... process ...
        await stateManager.UpdateStateAsync("myKey", newState, ct);
    }
    return output;
}
```

#### Option 2: Node-Level Synchronization

```csharp
public class ThreadSafeTransform<T> : TransformNode<T, T>
{
    private readonly object _syncLock = new();
    
    public override async ValueTask<T> TransformAsync(T input, PipelineContext context, CancellationToken ct)
    {
        lock (_syncLock)
        {
            // Synchronize access to shared state
            var value = context.Items.TryGetValue("key", out var v) ? v : null;
            context.Items["key"] = UpdateSharedState(value);
        }
        return input;
    }
}
```

#### Option 3: Atomic Operations for Simple Counters

For simple counters or flags, use `System.Threading.Interlocked`:

```csharp
public class CountingTransform : TransformNode<int, int>
{
    private long _processedCount = 0;
    
    public override async ValueTask<int> TransformAsync(int input, PipelineContext context, CancellationToken ct)
    {
        Interlocked.Increment(ref _processedCount);
        return input;
    }
}
```

## Context Dictionary Thread Safety

### Parameters Dictionary

- **Thread Safe?** NO
- **When Safe?** During initialization (before pipeline execution)
- **Use Case?** Configuration values that don't change during execution
- **Recommendation?** Populate this during setup phase only

### Items Dictionary

- **Thread Safe?** NO
- **When Safe?** Single-threaded pipeline execution
- **Use Case?** Node-to-node communication, metrics storage
- **Recommendation?** In parallel scenarios, use `IPipelineStateManager`

### Properties Dictionary

- **Thread Safe?** NO
- **When Safe?** Single-threaded pipeline execution
- **Use Case?** Extension points, plugin configuration
- **Recommendation?** Store thread-safe objects (like `IPipelineStateManager`) here

## Why Not ConcurrentDictionary?

You might wonder: "Why not just use `ConcurrentDictionary` for thread safety?"

**Design Rationale:**

1. **Performance:** Thread-safe operations add overhead (locks, memory barriers, allocations)
2. **Common Case:** ~99% of pipelines run single-threaded; paying the overhead for all is wasteful
3. **Philosophy:** NPipeline follows "pay only for what you use"
4. **Alternatives:** When thread-safe state IS needed, `IPipelineStateManager` is more purpose-built

## Parallel Execution Best Practices

### ✅ DO

- ✅ Use `IPipelineStateManager` for shared state in parallel scenarios
- ✅ Process independent data items in parallel workers
- ✅ Store immutable configuration in context properties
- ✅ Use atomic operations for simple counters
- ✅ Synchronize access to shared mutable state

### ❌ DON'T

- ❌ Directly modify `context.Items` from multiple threads without synchronization
- ❌ Assume context dictionaries are thread-safe
- ❌ Share mutable state between parallel workers without explicit synchronization
- ❌ Access `context.Parameters` after pipeline execution has started (in parallel scenarios)

## Example: Safe Parallel Processing

```csharp
public class SafeParallelTransform : TransformNode<DataItem, ProcessedItem>
{
    public override async ValueTask<ProcessedItem> TransformAsync(
        DataItem input, 
        PipelineContext context, 
        CancellationToken ct)
    {
        // Process the individual item (independent work)
        var processed = ProcessItem(input);
        
        // If you need to share state across parallel workers:
        var stateManager = context.StateManager;
        if (stateManager != null)
        {
            // Use thread-safe state manager
            await stateManager.UpdateMetricsAsync("processed_count", 1, ct);
        }
        
        return processed;
    }
    
    private ProcessedItem ProcessItem(DataItem input)
    {
        // Independent processing - no shared state access
        return new ProcessedItem { /* ... */ };
    }
}
```

## See Also

- [Parallel Execution](../extensions/parallelism.md)
- [State Management](../advanced-topics/state-management.md)
- [Error Handling](../core-concepts/error-handling.md)

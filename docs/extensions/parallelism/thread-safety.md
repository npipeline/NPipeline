---
title: Thread Safety
description: Comprehensive guide to thread safety and shared state management in parallel execution.
sidebar_position: 2
---

# Thread Safety in Parallel Execution

One of the most important aspects of parallel processing is understanding and managing thread safety correctly. NPipeline's parallel execution model is designed to be safe by default, but requires careful attention when accessing shared state.

> 🚨 **CRITICAL THREAD SAFETY TRAP**
>
> **DO NOT access `context.Items` or `context.Parameters` during parallel item processing.**
>
> These dictionaries are NOT thread-safe. If multiple worker threads try to access them simultaneously, you will get data races that cause:
>
> - Silent data corruption
> - Non-deterministic crashes
> - Impossible-to-reproduce bugs that only show up under production load
>
> The cost of learning this the hard way in production is enormous.

## The Unsafe Pattern (DO NOT USE)

```csharp
// ❌ WRONG - This is a data race
public class UnsafeMetricsTransform : TransformNode<int, int>
{
    public override async ValueTask<int> TransformAsync(
        int input,
        PipelineContext context,
        CancellationToken ct)
    {
        // PROBLEM: Multiple threads access this without synchronization
        var count = context.Items.GetValueOrDefault("processed", 0);
        context.Items["processed"] = count + 1;  // ← DATA RACE!
        
        // PROBLEM: Multiple threads read/write shared state
        if (context.Items.ContainsKey("sum"))
            context.Items["sum"] = (int)context.Items["sum"] + input;
        
        return input;
    }
}
```

**Why this breaks:**

- Thread A reads count = 5
- Thread B reads count = 5 (before A writes)
- Thread A writes count = 6
- Thread B writes count = 6 (overwrites A's write!)
- Expected: count = 7; Actual: count = 6 (lost update)
- This happens randomly under load - impossible to debug in development

## The Safe Pattern (RECOMMENDED)

```csharp
// ✅ CORRECT - Use IPipelineStateManager for shared state
public class SafeMetricsTransform : TransformNode<int, int>
{
    private long _processedCount = 0;
    private long _sum = 0;
    
    public override async ValueTask<int> TransformAsync(
        int input,
        PipelineContext context,
        CancellationToken ct)
    {
        // SAFE: Use atomic operations for simple counters
        Interlocked.Increment(ref _processedCount);
        Interlocked.Add(ref _sum, input);
        
        // SAFE: IPipelineStateManager can be used for state persistence
        // (snapshots, recovery, node completion tracking)
        var stateManager = context.StateManager;
        if (stateManager != null)
        {
            stateManager.MarkNodeCompleted(context.CurrentNodeId, context);
        }
        
        return input;
    }
}
```

**Why this is safe:**

- Atomic operations (`Interlocked`) ensure thread-safe counter updates
- State manager handles synchronization for persistence operations
- No data races - thread-safe by design
- Performance is optimized with lock-free atomic operations

## Key Principles

### Independent Item Processing

Each worker thread processes a different data item. The core processing is inherently thread-safe because workers operate on independent data.

```csharp
// SAFE: Each thread processes different items independently
public override async ValueTask<TOut> TransformAsync(
    TIn input,                    // Each thread gets a different item
    PipelineContext context,
    CancellationToken ct)
{
    // Safe to process input without synchronization
    return await ProcessItemAsync(input, ct);
}
```

### Shared State is NOT Thread-Safe

The `PipelineContext` dictionaries (Items, Parameters, Properties) are NOT thread-safe. If multiple worker threads need to access or modify shared state, you must use explicit synchronization.

```csharp
// UNSAFE: Multiple threads accessing context.Items without synchronization
context.Items["counter"] = (int)context.Items.GetValueOrDefault("counter", 0) + 1;

// SAFE: Use atomic operations for simple counters
Interlocked.Increment(ref _counter);

// SAFE: Use IPipelineStateManager for state persistence (snapshots, recovery)
var stateManager = context.StateManager;
if (stateManager != null)
{
    stateManager.MarkNodeCompleted(context.CurrentNodeId, context);
}
```

## Three Approaches to Shared State

See [Thread Safety Guidelines](../../core-concepts/thread-safety.md) for comprehensive guidance, but here's a quick summary for parallel scenarios:

### 1. IPipelineStateManager (Recommended)

For complex shared state that needs coordination across parallel workers:

```csharp
public override async ValueTask<TOut> TransformAsync(
    TIn input,
    PipelineContext context,
    CancellationToken ct)
{
    var result = ProcessItem(input);
    
    // Thread-safe state persistence via state manager
    var stateManager = context.StateManager;
    if (stateManager != null)
    {
        // Mark node as completed for state tracking
        stateManager.MarkNodeCompleted(context.CurrentNodeId, context);
    }
    
    return result;
}
```

### 2. Node-Level Synchronization

For simple synchronization within a single node:

```csharp
public class SynchronizedTransform : TransformNode<int, int>
{
    private readonly object _syncLock = new();
    private int _total = 0;
    
    public override async ValueTask<int> TransformAsync(
        int input,
        PipelineContext context,
        CancellationToken ct)
    {
        lock (_syncLock)
        {
            _total += input;
        }
        return input;
    }
}
```

### 3. Atomic Operations for Simple Counters

For single-value counters without additional logic:

```csharp
public class CountingTransform : TransformNode<int, int>
{
    private long _processedCount = 0;
    
    public override async ValueTask<int> TransformAsync(
        int input,
        PipelineContext context,
        CancellationToken ct)
    {
        Interlocked.Increment(ref _processedCount);
        return input;
    }
}
```

## Thread Safety DO's

- Process independent data items in parallel (inherently safe)
- Use `IPipelineStateManager` for shared state
- Use `lock` for simple critical sections
- Use `Interlocked` for atomic counter operations
- Keep synchronization scopes small and fast

## Thread Safety DON'Ts

- Directly access or modify `context.Items` from multiple threads
- Share mutable state between nodes without explicit synchronization
- Assume dictionaries in `PipelineContext` are thread-safe
- Hold locks across I/O operations (causes contention)
- Create complex multi-step interlocked sequences (use locks instead)

**For comprehensive guidance, see [Thread Safety Guidelines](../../core-concepts/thread-safety.md).**

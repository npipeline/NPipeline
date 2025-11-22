---
title: Synchronous Fast Paths and ValueTask Optimization
description: Deep-dive guide on optimizing transformer nodes for high-throughput scenarios using ValueTask to eliminate GC pressure.
sidebar_position: 2
---

# Synchronous Fast Paths and ValueTask Optimization

## Prerequisites

Before implementing ValueTask optimization, you should be familiar with:
- [Core Concepts Overview](../core-concepts/index.md) - Basic NPipeline concepts and terminology
- [Nodes Overview](../core-concepts/nodes/index.md) - Understanding how nodes process data
- [Transform Nodes](../core-concepts/nodes/transform-nodes.md) - Node implementation details
- [Optimization Principles](../architecture/optimization-principles.md) - Understanding why ValueTask improves performance

> :information_source: For general performance best practices, see [Performance Hygiene](performance-hygiene.md).

This is **definitive guide** for understanding and implementing `ValueTask<T>` pattern in transformer nodes. For a quick introduction, see [Performance Hygiene: Use ValueTask\<T\> for Fast Path Scenarios](performance-hygiene.md#use-valuetaskt-for-fast-path-scenarios).

## The Performance Paradox

A common performance pitfall in high-throughput ETL pipelines is contradiction between advice and implementation:

> **The advice says:** "Minimize memory allocations to reduce GC pressure"
> **The code does:** Return `Task<T>` for all transformer nodes

This creates a subtle but critical performance problem: even when your transform work is **completely synchronous** (a cache hit, a simple calculation), you're still creating a heap-allocated `Task<T>` object to wrap the result.

In a pipeline processing **millions of items per second**, where many transforms are synchronous or have high synchronous fast-path rates, you can easily be creating **millions of tiny heap allocations per second**. This creates constant pressure on garbage collector, causing pauses that directly undermine your throughput goals.

## The Solution: ValueTask

`ValueTask<T>` is a struct-based alternative to `Task<T>` that:

- **Allocates on stack** (not heap) when result is available synchronously
- **Zero allocations** for common case in cache-hit or synchronous scenarios
- **Seamlessly transitions** to true async work when needed

The tradeoff: You need to implement a two-path pattern—checking for synchronous case first.

## The Pattern: Synchronous Fast Path + Asynchronous Slow Path

Here's pattern that balances performance with practicality:

```csharp
using NPipeline;
using NPipeline.Nodes;
using NPipeline.Pipeline;

/// <summary>
/// High-performance transform with cache optimization using ValueTask.
/// Demonstrates zero-allocation pattern for high-throughput scenarios.
/// In pipelines with 90% cache hits processing 10k items/sec,
/// this eliminates ~9000 allocations/sec compared to Task&lt;T&gt;.
/// </summary>
public sealed class CachedTransform : TransformNode<string, UserData>
{
    private readonly ConcurrentDictionary<string, UserData> _cache = new();

    /// <summary>
    /// Processes user data with cache-first strategy.
    /// Fast path: cache hit - no Task allocation
    /// Slow path: cache miss - async database call
    /// </summary>
    public override ValueTask<UserData> ExecuteAsync(
        string userId, 
        PipelineContext context, 
        CancellationToken cancellationToken)
    {
        // Fast path: cache hit - no Task allocation
        if (_cache.TryGetValue(userId, out var cached))
            return new ValueTask<UserData>(cached);

        // Slow path: async database call
        return new ValueTask<UserData>(FetchAndCacheAsync(userId, cancellationToken));
    }

    /// <summary>
    /// Fetches user data from database and caches the result.
    /// This method is only called on cache misses.
    /// </summary>
    private async Task<UserData> FetchAndCacheAsync(string userId, CancellationToken ct)
    {
        var data = await _database.GetUserAsync(userId, ct);
        _cache.TryAdd(userId, data);
        return data;
    }
}

// Supporting types for the example
public record UserData(string Id, string Name, string Email);
```

## Real-World Example: Cached Data Enrichment

Consider a typical ETL scenario where you enrich data by looking up additional information:

```csharp
using NPipeline;
using NPipeline.Nodes;
using NPipeline.Pipeline;

/// <summary>
/// Transform for enriching user data with caching optimization.
/// Demonstrates ValueTask pattern for real-world cache-hit scenarios.
/// </summary>
public sealed class UserEnrichmentTransform : TransformNode<UserId, EnrichedUser>
{
    private readonly ConcurrentDictionary<string, UserProfile> _profileCache = new();
    private readonly IUserDatabase _userDatabase;

    public UserEnrichmentTransform(IUserDatabase userDatabase)
    {
        _userDatabase = userDatabase;
    }

    /// <summary>
    /// Enriches user data with profile information.
    /// Uses cache-first strategy to minimize database calls.
    /// </summary>
    public override ValueTask<EnrichedUser> ExecuteAsync(
        UserId userId,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Check cache first (usually succeeds - fast path)
        if (_profileCache.TryGetValue(userId.Value, out var cachedProfile))
        {
            // Fast path: Return immediately, zero heap allocation
            return new ValueTask<EnrichedUser>(
                new EnrichedUser(userId, cachedProfile)
            );
        }

        // Cache miss: Fall back to database lookup (slow path)
        return new ValueTask<EnrichedUser>(
            FetchAndEnrichAsync(userId, context, cancellationToken)
        );
    }

    /// <summary>
    /// Fetches user profile from database and caches the result.
    /// Only executed on cache misses, then cached for future requests.
    /// </summary>
    private async Task<EnrichedUser> FetchAndEnrichAsync(
        UserId userId,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var profile = await _userDatabase.GetProfileAsync(userId.Value, cancellationToken);
        _profileCache.TryAdd(userId.Value, profile);
        return new EnrichedUser(userId, profile);
    }
}

// Supporting types for the example
public record UserId(string Value);
public record UserProfile(string Name, string Email, DateTime CreatedAt);
public record EnrichedUser(UserId Id, UserProfile Profile);

// Interface for database access
public interface IUserDatabase
{
    Task<UserProfile> GetProfileAsync(string userId, CancellationToken cancellationToken);
}
```

## Performance Impact

In a realistic high-volume ETL pipeline:

| Scenario | Items/Sec | Cache Hit Rate | Task Allocations/Sec | ValueTask Allocations/Sec | GC Pressure Reduction |
|----------|-----------|----------------|-------------------------|------------------------------|----------------------|
| Simple enrichment | 1,000,000 | 90% | 1,000,000 | 100,000 | **90%** |
| Lookup pipeline | 5,000,000 | 85% | 5,000,000 | 750,000 | **85%** |
| Stream transformation | 10,000,000 | 95% | 10,000,000 | 500,000 | **95%** |

**The compounding effect:** Reduced allocations → Less GC pressure → Fewer GC pauses → More throughput → Lower latency

## Practical Guidelines

### When to Use `ValueTask` for Transforms

✅ **Use `ValueTask` when:**

- The transform can complete synchronously in common case
- You have a cache, in-memory lookup, or fast path
- The synchronous case is likely to happen frequently
- You're optimizing for throughput in high-volume scenarios

❌ **Use `Task` when:**

- The transform is almost always asynchronous (database queries, network calls every time)
- You want simpler code and performance benefit is marginal
- You're building a library and want to keep interface simple
- The pipeline volume is low enough that allocations don't matter

### Implementation Checklist

- [ ] **Identify fast path:** Where can transform return synchronously?
- [ ] **Measure fast-path hit rate:** Is it worth optimizing? (Usually yes if > 50%)
- [ ] **Implement `TryGetSynchronousResult()`:** Extract synchronous case
- [ ] **Benchmark it:** Use BenchmarkDotNet to measure allocation reduction
- [ ] **Document it:** Explain why `ValueTask<T>` is used and what fast path is
- [ ] **Test both paths:** Ensure your code works when fast path returns and when it doesn't

## Code Examples

### Example 1: Simple Transformation (Always Synchronous)

If your transform is **always** synchronous, even simpler:

```csharp
using NPipeline;
using NPipeline.Nodes;
using NPipeline.Pipeline;

/// <summary>
/// Transform for simple synchronous calculations.
/// Demonstrates ValueTask for always-synchronous operations.
/// Using ValueTask eliminates Task allocation even for simple calculations.
/// </summary>
public sealed class SimpleCalculationTransform : TransformNode<int, int>
{
    /// <summary>
    /// Performs square operation on input integer.
    /// Pure synchronous work - no async operations needed.
    /// </summary>
    public override ValueTask<int> ExecuteAsync(
        int item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // No async work at all - direct calculation
        return new ValueTask<int>(item * item);
    }
}
```

### Example 2: Format Transformation with Optional Fallback

```csharp
using NPipeline;
using NPipeline.Nodes;
using NPipeline.Pipeline;

/// <summary>
/// Transform for data formatting with optional fallback.
/// Demonstrates hybrid pattern with local cache and network service.
/// </summary>
public sealed class DataFormatTransform : TransformNode<RawData, FormattedData>
{
    private readonly IFormatterCache _formatterCache;
    private readonly INetworkFormatterService _networkFormatter;

    public DataFormatTransform(
        IFormatterCache formatterCache,
        INetworkFormatterService networkFormatter)
    {
        _formatterCache = formatterCache;
        _networkFormatter = networkFormatter;
    }

    /// <summary>
    /// Formats raw data using cache-first strategy.
    /// Fast path: local cache hit
    /// Slow path: network service call
    /// </summary>
    public override ValueTask<FormattedData> ExecuteAsync(
        RawData item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Try local cache (fast synchronous path)
        if (_formatterCache.TryFormat(item, out var formatted))
        {
            return new ValueTask<FormattedData>(formatted);
        }

        // Fall back to network service if needed (slow async path)
        return new ValueTask<FormattedData>(
            _networkFormatter.FormatAsync(item, cancellationToken)
        );
    }
}

// Supporting types for the example
public record RawData(string Id, string Content);
public record FormattedData(string Id, string FormattedContent, DateTime ProcessedAt);

// Interfaces for dependencies
public interface IFormatterCache
{
    bool TryFormat(RawData data, out FormattedData formatted);
}

public interface INetworkFormatterService
{
    Task<FormattedData> FormatAsync(RawData data, CancellationToken cancellationToken);
}
```

## Critical Constraints: When NOT to Use ValueTask

While ValueTask is powerful, it comes with strict constraints. Understanding these is essential for correctness.

### Never Await More Than Once

**The Rule:** You can only `await` a `ValueTask<T>` exactly once. Multiple awaits on same `ValueTask<T>` are undefined behavior and will cause exceptions or incorrect results.

```csharp
// INCORRECT - DO NOT DO THIS
var valueTask = GetValueAsync("key");
var result1 = await valueTask;  // First await - OK
var result2 = await valueTask;  // Second await - UNDEFINED BEHAVIOR (exception or wrong result)
```

**Why?** The struct-based nature of `ValueTask<T>` means its state is mutable. After first await completes, internal state is consumed. A second await has nowhere to go.

**Correct usage:**

```csharp
// CORRECT
var result1 = await GetValueAsync("key");
var result2 = await GetValueAsync("key"); // Call method again
```

### Avoid When Always Asynchronous

If your method **always** performs async work—never returns synchronously—there's no benefit to `ValueTask<T>`. The wrapper adds complexity without any allocation savings.

```csharp
// WRONG - Always async, so no benefit
public ValueTask<int> ComputeExpensiveAsync()
{
    return new ValueTask<int>(ExpensiveComputationAsync());
}

// CORRECT - Use Task for always async operations
public Task<int> ComputeExpensiveAsync()
{
    return ExpensiveComputationAsync();
}
```

In this case, synchronous fast path never exists, so you're just adding complexity.

### Consider Public APIs Carefully

If this is a public API that external callers will use, consider using `Task<T>` instead. Your callers need to understand constraints of `ValueTask<T>` (single await, no ConfigureAwait) to use it correctly. If those constraints aren't explicitly documented and understood, you may create subtle bugs in calling code.

```csharp
// If this is internal or well-documented, ValueTask is fine
internal ValueTask<int> GetItemAsync(string key)
{
    // ...
}

// If this is public, consider whether ValueTask constraints are worth performance gain
// or whether Task is safer choice for API stability
public Task<int> GetItemAsync(string key)
{
    // Simpler contract, no surprise constraints
}
```

### ConfigureAwait Not Supported

`ValueTask<T>` does not support `ConfigureAwait()`. If your code requires `ConfigureAwait(false)` for library code or UI synchronization context handling, you cannot use `ValueTask<T>`.

```csharp
// WRONG - ConfigureAwait not supported on ValueTask
var result = await GetValueAsync().ConfigureAwait(false);

// CORRECT if you need ConfigureAwait
public Task<string> GetValueAsync()
{
    return FetchAsync().ConfigureAwait(false);
}
```

## See Also

- [Performance Hygiene](performance-hygiene.md) - Comprehensive performance best practices
- [Transform Nodes](../core-concepts/nodes/transform-nodes.md) - Node implementation details and ValueTask optimization patterns
- [Optimization Principles](../architecture/optimization-principles.md) - Understanding why ValueTask improves performance
- [Sample: High-Performance Transforms](../../samples/Sample_HighPerformanceTransform/) - Complete working example
- [Execution Strategies](../core-concepts/pipeline-execution/execution-strategies.md) - How ValueTask integrates with execution strategies
- [Error Handling Guide](../core-concepts/resilience/error-handling-guide.md) - Error handling with ValueTask patterns

## Next Steps

- [Performance Hygiene](performance-hygiene.md) - Comprehensive performance best practices
- [Transform Nodes](../core-concepts/nodes/transform-nodes.md) - Node implementation details and ValueTask optimization patterns
- [Optimization Principles](../architecture/optimization-principles.md) - Understanding why ValueTask improves performance
- [Sample: High-Performance Transforms](../../samples/Sample_HighPerformanceTransform/) - Complete working example
- [Execution Strategies](../core-concepts/pipeline-execution/execution-strategies.md) - How ValueTask integrates with execution strategies

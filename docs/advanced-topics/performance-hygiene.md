---
title: Performance Hygiene
description: Best practices for building high-performance, low-allocation data pipelines with NPipeline.
sidebar_position: 2
---

NPipeline is designed for high performance, but building an efficient pipeline requires careful consideration of how you write your nodes and structure your data flow. "Performance hygiene" refers to the practice of writing code that is mindful of memory allocations, CPU usage, and data transfer overhead.

> For specific optimization patterns and techniques, see [Synchronous Fast Paths](synchronous-fast-paths.md).

By following these best practices, you can ensure your pipelines run as fast and efficiently as possible.

## 1. Minimize Memory Allocations

In high-throughput data pipelines, memory allocation can become a major bottleneck. The .NET garbage collector (GC) is highly optimized, but frequent, large allocations can lead to GC pressure, causing pauses that hurt performance.

### Use `struct` for Small, Short-Lived Data

If you are passing small, simple data objects between nodes, consider using a `struct` instead of a `class`. Structs are value types and are allocated on the stack (in most cases), which avoids putting pressure on the GC.

**Good for performance:**

```csharp
// A struct is allocated on the stack, avoiding GC pressure.
public readonly struct Point
{
    public int X { get; }
    public int Y { get; }

    public Point(int x, int y)
    {
        X = x;
        Y = y;
    }
}
```

**Avoid for high-throughput data:**

```csharp
// A class is allocated on the heap, creating work for the GC.
public class Point
{
    public int X { get; set; }
    public int Y { get; set; }
}
```

**Caveat:** Be mindful of the size of your structs. Large structs can lead to expensive copy operations. As a rule of thumb, structs are ideal for types that are small (e.g., under 16 bytes) and immutable.

### Reuse Buffers

If your nodes process data in batches or chunks (e.g., reading from a network stream), reuse buffers instead of allocating a new one for each operation.

```csharp
// Zero-allocation buffer reuse with direct processing
public async IAsyncEnumerable<ReadOnlyMemory<byte>> ProcessStream(Stream stream, CancellationToken cancellationToken)
{
    // Rent a buffer from the ArrayPool to avoid allocations
    var buffer = ArrayPool<byte>.Shared.Rent(8192);
    try
    {
        int bytesRead;
        while ((bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length, cancellationToken)) > 0)
        {
            // Return a ReadOnlyMemory slice - no allocation!
            yield return new ReadOnlyMemory<byte>(buffer, 0, bytesRead);
        }
    }
    finally
    {
        // Return the buffer to the pool for reuse
        ArrayPool<byte>.Shared.Return(buffer);
    }
}
```

For even better performance, process the data directly without yielding:

```csharp
// Process data inline for maximum performance
public async Task ProcessStreamInline(Stream stream, Func<ReadOnlyMemory<byte>, Task> processor, CancellationToken cancellationToken)
{
    var buffer = ArrayPool<byte>.Shared.Rent(8192);
    try
    {
        int bytesRead;
        while ((bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length, cancellationToken)) > 0)
        {
            // Process immediately without yielding
            await processor(new ReadOnlyMemory<byte>(buffer, 0, bytesRead));
        }
    }
    finally
    {
        ArrayPool<byte>.Shared.Return(buffer);
    }
}
```

### Use `PipelineObjectPool` for Framework Collections

NPipeline now pools all of the hot-path dictionaries that back graph execution and `PipelineContext`. You can rely on `PipelineObjectPool` to rent these collections instead of allocating new instances for every run:

```csharp
var nodeOutputs = PipelineObjectPool.RentNodeOutputDictionary(graph.Nodes.Count);

try
{
    // Execute nodes using the shared dictionary...
}
finally
{
    foreach (var (_, pipe) in nodeOutputs)
    {
        await pipe?.DisposeAsync();
    }

    nodeOutputs.Clear();
    PipelineObjectPool.Return(nodeOutputs);
}
```

When you build contexts manually, allow `PipelineContext` to rent pooled dictionaries by not supplying `Parameters`, `Items`, or `Properties` explicitly. Always dispose the context you create so the dictionaries are returned to the pool:

```csharp
await using var context = new PipelineContext(PipelineContextConfiguration.WithCancellation(cancellationToken));

await runner.RunAsync<MyPipeline>(context, cancellationToken);
```

Supplying custom dictionaries is still supported—`PipelineContext` detects ownership and will skip pool returns for caller-managed instances. This lets you decide which allocations are worth pooling while still benefiting from the framework defaults.

## 2. Be Mindful of `async` and `await`

While `async/await` is essential for I/O-bound work, it does introduce some overhead.

### Avoid `async` for Synchronous Work

If a method doesn't perform any truly asynchronous operations, don't mark it as `async`. You can return a completed `Task` directly.

**Good:**

```csharp
public Task<int> GetConstantAsync()
{
    // No await needed, so no async state machine is generated.
    return Task.FromResult(42);
}
```

**Avoid:**

```csharp
public async Task<int> GetConstantAsync()
{
    // Unnecessary async/await creates overhead.
    return await Task.FromResult(42);
}
```

### Use `ValueTask<T>` for "Fast Path" Scenarios

If your method is often able to return a result synchronously (e.g., from a cache), but may sometimes need to be asynchronous, use `ValueTask<T>`. This avoids a heap allocation for the `Task` object in the synchronous case.

**This is especially critical for transformer nodes in high-volume pipelines.** Many transforms are synchronous or have a high synchronous fast path (cache hits, simple mappings). Using `Task<T>` for these transforms creates millions of unnecessary heap allocations per second, causing constant GC pressure.

For comprehensive implementation guidance, including critical constraints and real-world examples, see [**Synchronous Fast Paths and ValueTask Optimization**](synchronous-fast-paths.md)—the dedicated deep-dive guide that covers the complete implementation pattern, performance impact quantification, and dangerous constraints you must understand.

## 3. Choose the Right Concurrency Strategy

- **I/O-Bound Work:** For nodes that spend most of their time waiting for network or disk I/O, use the [Parallelism Extension](../extensions/parallelism/index.md) with a relatively high `MaxDegreeOfParallelism`. This ensures that while some tasks are waiting, others are actively being processed.

- **CPU-Bound Work:** For nodes performing intensive calculations, set `MaxDegreeOfParallelism` to a value close to `Environment.ProcessorCount`. Note that this is already the default behavior when no value is specified, so you typically don't need to set it explicitly.

### Advanced Parallel Configuration

The Parallelism Extension provides fine-grained control over execution behavior through the `ParallelOptions` class:

```csharp
using NPipeline.Extensions.Parallelism;

// Configure parallel execution with advanced options
var parallelOptions = new ParallelOptions
{
    MaxDegreeOfParallelism = Environment.ProcessorCount, // Default when null
    MaxQueueLength = 1000, // Controls backpressure by limiting input queue size
    QueuePolicy = BoundedQueuePolicy.Block, // Behavior when queue is full
    OutputBufferCapacity = 500, // Throttles workers by limiting output buffer
    PreserveOrdering = true // Default: maintains input ordering in output
};

// Apply to a specific transform node
builder.WithParallelOptions(transformHandle, parallelOptions);
```

#### Queue Policy Options

When `MaxQueueLength` is set, you can control behavior when the queue becomes full:

- **`Block`** (default): The producer blocks until space is available, providing natural backpressure
- **`DropNewest`**: Incoming items are discarded when the queue is full
- **`DropOldest`**: The oldest items in the queue are discarded to make room for new ones

#### Output Buffer Control

The `OutputBufferCapacity` option creates an additional throttling mechanism:

- When specified, it limits how many processed results can queue ahead of downstream consumption
- This restores end-to-end backpressure when downstream nodes are slow
- When null (default), output buffering is unbounded, which can lead to memory accumulation under sustained load

#### Ordering Considerations

- The `PreserveOrdering` flag (default: true) ensures output maintains input order
- Setting it to false can increase throughput but results in unordered output
- Note: Drop-policy paths (`DropNewest`, `DropOldest`) are inherently unordered regardless of this setting

## 4. Streaming vs. Buffering

NPipeline is designed around a streaming-first philosophy. Nodes should process items as they arrive and yield results immediately. Avoid collecting all items from the input into a list before processing.

**Good (Streaming):**

```csharp
public async IAsyncEnumerable<string> ExecuteAsync(IAsyncEnumerable<string> input, CancellationToken cancellationToken)
{
    // Process items as they come in.
    await foreach (var item in input.WithCancellation(cancellationToken))
    {
        yield return item.ToUpper();
    }
}
```

**Avoid (Buffering):**

```csharp
public async IAsyncEnumerable<string> ExecuteAsync(IAsyncEnumerable<string> input, CancellationToken cancellationToken)
{
    // This buffers the entire input into memory before processing.
    // It can lead to high memory usage and delays the start of processing.
    var allItems = await input.ToListAsync(cancellationToken);

    foreach (var item in allItems)
    {
        yield return item.ToUpper();
    }
}
```

Buffering is only appropriate if your logic requires access to the entire dataset at once (e.g., sorting or calculating a global aggregate).

## 5. Use Benchmarking

The most reliable way to improve performance is to measure it. Use tools like [BenchmarkDotNet](https://benchmarkdotnet.org/) to write micro-benchmarks for your critical nodes. This allows you to test different implementations and configurations to see which performs best.

```csharp
[MemoryDiagnoser] // Track memory allocations
public class MyTransformBenchmarks
{
    private MyTransform _transform;
    private IAsyncEnumerable<string> _data;

    [Params(100, 1000)]
    public int N;

    [GlobalSetup]
    public void Setup()
    {
        _transform = new MyTransform();
        _data = new EnumerableSourceNode<string>(Enumerable.Range(0, N).Select(i => i.ToString())).GetAsyncEnumerator();
    }

    [Benchmark]
    public async Task Transform()
    {
        await foreach(var _ in _transform.ExecuteAsync(_data))
        {
            // Consume results
        }
    }
}
```

## 4. Avoid Context Mutations During Node Execution

### The Context Immutability Guarantee

NPipeline uses `CachedNodeExecutionContext` to optimize per-item context access by caching execution state (retry options, tracing configuration, etc.) at the start of node execution. This provides significant performance benefits, but it requires that context state remains **immutable during node execution**.

### What You Must Not Do

❌ **Don't modify retry options during node execution:**

```csharp
public async Task<TOut> ExecuteAsync(TIn item, PipelineContext context, CancellationToken cancellationToken)
{
    // ❌ WRONG: Context state is cached at node start; this modification may be ignored
    context.Items[PipelineContextKeys.NodeRetryOptions(context.CurrentNodeId)] = newRetryOptions;
    
    // Proceed with transform
    var result = TransformItem(item);
    return await Task.FromResult(result);
}
```

❌ **Don't replace tracer or logger factory during node execution:**

```csharp
public async Task<TOut> ExecuteAsync(TIn item, PipelineContext context, CancellationToken cancellationToken)
{
    // ❌ WRONG: Tracer is cached at node start
    context.Tracer = new MyCustomTracer();
    
    // Rest of execution will use old tracer instance
    return await ProcessAsync(item);
}
```

### What You Should Do Instead

✅ **Configure context before node execution starts:**

```csharp
// Configure everything upfront
var context = new PipelineContext();
context.Items[PipelineContextKeys.GlobalRetryOptions] = new PipelineRetryOptions(3, 2, 5);
context.LoggerFactory = new MyLoggerFactory();
context.Tracer = new MyTracer();

// Now start the pipeline - all configuration is fixed for the duration
var runner = new PipelineRunner();
await runner.RunAsync<MyPipeline>(context);
```

✅ **Modify context between pipeline runs if needed:**

```csharp
// Pipeline 1 with configuration A
await runner.RunAsync<MyPipeline>(contextA);

// Modify context for next run
contextB.Items[PipelineContextKeys.GlobalRetryOptions] = newOptions;

// Pipeline 2 with configuration B
await runner.RunAsync<MyPipeline>(contextB);
```

### Why This Matters

When context is cached at node scope (which execution strategies do automatically), mutations during execution can cause:

- **Inconsistent behavior** - Some items processed with old config, others with new config
- **Unexpected retry behavior** - Items may retry with different policies mid-execution
- **Tracing/logging gaps** - Tracer/logger changes don't apply to all items uniformly

### DEBUG Validation

In DEBUG builds, NPipeline detects mutations and throws a clear exception:

```text
InvalidOperationException: 
  Context immutability violation detected for node 'myNode': 
  Retry options were modified during node execution. 
  When using CachedNodeExecutionContext, context state must remain 
  immutable during node execution.
```

In RELEASE builds, this validation is **compiled out entirely** (zero overhead).

### Best Practices Summary

1. **Set all context configuration before starting pipeline execution**
2. **Treat context as read-only during node execution**
3. **Use separate context instances if you need different configurations**
4. **Trust the automatic caching** - It's optimizing your pipeline for you

For architectural context, see [Execution Flow: Context Immutability During Execution](../architecture/execution-flow.md#context-immutability-during-execution).

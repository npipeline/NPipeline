---
title: Performance Hygiene
description: Best practices for building high-performance, low-allocation data pipelines with NPipeline.
sidebar_position: 1
---

# Performance Hygiene

NPipeline is designed for high performance, but building an efficient pipeline requires careful consideration of how you write your nodes and structure your data flow. "Performance hygiene" refers to the practice of writing code that is mindful of memory allocations, CPU usage, and data transfer overhead.

> :information_source: For specific optimization patterns and techniques, see [Synchronous Fast Paths](synchronous-fast-paths.md).

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

The basic pattern is straightforward:

```csharp
// Synchronous fast path: Check cache first
if (_cache.TryGetValue(key, out var value))
{
    return new ValueTask<string>(value); // No Task allocation
}

// Asynchronous slow path: Fall back to async work
return new ValueTask<string>(FetchAndCacheValueAsync(key));
```

This pattern is commonly applied to **transformer nodes** where cache hits or simple synchronous operations dominate. For comprehensive examples and the full pattern applied to `ITransformNode<TIn, TOut>`, see [**Synchronous Fast Paths and ValueTask Optimization**](synchronous-fast-paths.md)—the dedicated deep-dive guide that covers real-world scenarios, performance impact quantification, and implementation guidelines.

## 3. Choose the Right Concurrency Strategy

- **I/O-Bound Work:** For nodes that spend most of their time waiting for network or disk I/O, use the [Parallelism Extension](../extensions/parallelism.md) with a relatively high `MaxDegreeOfParallelism`. This ensures that while some tasks are waiting, others are actively being processed.

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

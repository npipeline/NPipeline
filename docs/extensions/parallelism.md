---
title: Parallelism
description: Enhance your NPipeline's performance by leveraging parallel processing capabilities.
sidebar_position: 2
---

# Parallelism

NPipeline is designed for high performance, and a key aspect of this is its ability to execute parts of your pipeline in parallel. The [`NPipeline.Extensions.Parallelism`](../../../src/NPipeline.Extensions.Parallelism/NPipeline.Extensions.Parallelism.csproj) package provides tools and extensions to easily introduce parallel processing into your data flows, allowing you to scale out your computations and maximize throughput.

## Understanding Parallelism in NPipeline

Parallelism in NPipeline typically means processing multiple data items concurrently, either within a single node or across multiple independent branches of a pipeline. This is distinct from concurrency, which is about managing multiple tasks that may or may not run simultaneously.

## [`NPipeline.Extensions.Parallelism`](../../../src/NPipeline.Extensions.Parallelism/NPipeline.Extensions.Parallelism.csproj)

This extension package provides the [`ParallelExecutionStrategy`](../../../src/NPipeline.Extensions.Parallelism/ParallelExecutionStrategy.cs) class and builder extensions to enable and manage parallel execution.

### Important: Thread Safety

When using parallel execution, it's critical to understand NPipeline's thread-safety model. Each worker thread processes **independent data items**—the `PipelineContext` itself is not shared across threads. However, if your nodes need to access shared state during parallel execution, you must use thread-safe mechanisms.

**See [Thread Safety Guidelines](../core-concepts/thread-safety.md) for detailed guidance on:**

- Safe patterns for accessing context during parallel execution
- Using `IPipelineStateManager` for shared state
- Node-level synchronization strategies
- When and how to use atomic operations

This is essential reading if your parallel nodes interact with shared state.

### Example: Parallel Transform

Imagine you have a computationally intensive transformation that can be applied independently to each item. You can use parallel execution to process multiple items simultaneously.

```csharp
using NPipeline.Extensions.Parallelism;
using NPipeline.Extensions.Testing;
using NPipeline.Execution;
using NPipeline.Nodes;

public sealed class IntensiveTransform : TransformNode<int, int>
{
    public override async Task<int> ExecuteAsync(
        int item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var logger = context.LoggerFactory.CreateLogger("IntensiveTransform");
        logger.Log(NPipeline.Observability.Logging.LogLevel.Information,
            $"Processing item {item} on Thread {Environment.CurrentManagedThreadId}");
        await Task.Delay(100, cancellationToken); // Simulate intensive work
        return item * 2;
    }
}

public sealed class ParallelPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddInMemorySource<int>();
        var transform = builder.AddTransform<IntensiveTransform, int, int>();
        var sink = builder.AddInMemorySink<int>();

        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        // Configure parallel execution for the transform
        builder.WithParallelOptions(transform,
            new ParallelOptions { MaxDegreeOfParallelism = 4 });

        // Set the execution strategy to ParallelExecutionStrategy
        transform.ExecutionStrategy = new ParallelExecutionStrategy();
    }
}

public static class Program
{
    public static async Task Main(string[] args)
    {
        // Set up test data
        var testData = new[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        var context = new PipelineContext();
        context.SetSourceData(testData);

        Console.WriteLine("Starting parallel pipeline...");
        var runner = PipelineRunner.Create();
        await runner.RunAsync<ParallelPipeline>(context);
        Console.WriteLine("Parallel pipeline finished.");
    }
}
```

In this example, we:

1. Use [`WithParallelOptions(transform, new ParallelOptions { MaxDegreeOfParallelism = 4 })`](../../../src/NPipeline.Extensions.Parallelism/ParallelOptions.cs:47) to configure parallel execution options for the transform
2. Set the [`ExecutionStrategy`](../../../src/NPipeline/Graph/NodeDefinition.cs) property to [`ParallelExecutionStrategy`](../../../src/NPipeline.Extensions.Parallelism/ParallelExecutionStrategy.cs) to enable parallel processing

## Non-Ordered Parallel Execution for Maximum Throughput

By default, NPipeline preserves the order of items even when processing them in parallel. While this ensures predictable output, it can introduce overhead that reduces throughput. When the order of output items is not important for your use case, you can configure parallel execution to not preserve order, which can significantly increase throughput.

### Example: Non-Ordered Parallel Processing

```csharp
using NPipeline.Extensions.Parallelism;
using NPipeline.Extensions.Testing;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;

public sealed class IntensiveTransform : TransformNode<int, int>
{
    public override async Task<int> ExecuteAsync(
        int item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var logger = context.LoggerFactory.CreateLogger("IntensiveTransform");
        logger.Log(NPipeline.Observability.Logging.LogLevel.Information,
            $"Processing item {item} on Thread {Environment.CurrentManagedThreadId}");
        await Task.Delay(new Random().Next(50, 150), cancellationToken); // Simulate variable work
        return item * 2;
    }
}

public sealed class NonOrderedParallelPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddInMemorySource<int>();
        var transform = builder.AddTransform<IntensiveTransform, int, int>();
        var sink = builder.AddInMemorySink<int>();

        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        // Configure parallel execution with non-ordered output for maximum throughput
        builder.WithParallelOptions(transform,
            new ParallelOptions
            {
                MaxDegreeOfParallelism = 4,
                PreserveOrdering = false  // Disable ordering to maximize throughput
            });

        // Set the execution strategy to ParallelExecutionStrategy
        transform.ExecutionStrategy = new ParallelExecutionStrategy();
    }
}

public static class Program
{
    public static async Task Main(string[] args)
    {
        // Set up test data
        var testData = new[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        var context = new PipelineContext();
        context.SetSourceData(testData);

        Console.WriteLine("Starting non-ordered parallel pipeline...");
        var runner = PipelineRunner.Create();
        await runner.RunAsync<NonOrderedParallelPipeline>(context);
        Console.WriteLine("Non-ordered parallel pipeline finished.");
    }
}
```

In this example, we explicitly set `PreserveOrdering = false` in the [`ParallelOptions`](../../../src/NPipeline.Extensions.Parallelism/ParallelOptions.cs:34). This configuration:

1. Allows items to be emitted as soon as they are processed, without waiting for slower items
2. Eliminates the overhead of tracking and reordering items
3. Can significantly increase throughput, especially when processing times vary widely
4. Results in output that may not match the input order

### When to Use Non-Ordered Execution

Consider using `PreserveOrdering = false` when:

- **Order is irrelevant**: Your downstream processing doesn't depend on the input order
- **Maximum throughput is critical**: You need to process as many items as possible per unit of time
- **Processing times vary significantly**: Some items take much longer to process than others
- **You're aggregating results**: You're collecting statistics or aggregating data where order doesn't matter

### Trade-offs

| Aspect | PreserveOrdering: true (Default) | PreserveOrdering: false |
|--------|----------------------------------|-------------------------|
| **Throughput** | Good | Excellent |
| **Output Order** | Matches input order | May be out of order |
| **Memory Usage** | Higher (needs to buffer) | Lower |
| **Latency** | Higher (waits for slow items) | Lower (emits immediately) |
| **Use Case** | Order-sensitive processing | Maximum throughput scenarios |

## Advanced Parallel Options

The [`ParallelOptions`](../../../src/NPipeline.Extensions.Parallelism/ParallelOptions.cs:34) class provides additional configuration options for fine-tuning parallel execution:

```csharp
var options = new ParallelOptions
{
    MaxDegreeOfParallelism = 8,           // Maximum concurrent operations
    MaxQueueLength = 1000,               // Bounded input queue for backpressure
    QueuePolicy = BoundedQueuePolicy.Block, // Behavior when queue is full
    OutputBufferCapacity = 500,           // Bounded output buffer
    PreserveOrdering = true               // Whether to preserve input order
};
```

### Queue Policies

When `MaxQueueLength` is specified, you can control the behavior when the queue reaches its capacity:

- **`BoundedQueuePolicy.Block`**: Wait until space becomes available (default)
- **`BoundedQueuePolicy.DropNewest`**: Drop the incoming item
- **`BoundedQueuePolicy.DropOldest`**: Remove the oldest item to make space

## Thread Safety in Parallel Execution

One of the most important aspects of parallel processing is understanding and managing thread safety correctly. NPipeline's parallel execution model is designed to be safe by default, but requires careful attention when accessing shared state.

### Key Principles

**Independent Item Processing:** Each worker thread processes a different data item. The core processing is inherently thread-safe because workers operate on independent data.

```csharp
// ✅ SAFE: Each thread processes different items independently
public override async ValueTask<TOut> TransformAsync(
    TIn input,                    // Each thread gets a different item
    PipelineContext context,
    CancellationToken ct)
{
    // Safe to process input without synchronization
    return await ProcessItemAsync(input, ct);
}
```

**Shared State is NOT Thread-Safe:** The `PipelineContext` dictionaries (Items, Parameters, Properties) are NOT thread-safe. If multiple worker threads need to access or modify shared state, you must use explicit synchronization.

```csharp
// ❌ UNSAFE: Multiple threads accessing context.Items without synchronization
context.Items["counter"] = (int)context.Items.GetValueOrDefault("counter", 0) + 1;

// ✅ SAFE: Use IPipelineStateManager for thread-safe shared state
var stateManager = context.StateManager;
if (stateManager != null)
{
    await stateManager.IncrementCounterAsync("counter", ct);
}
```

### Three Approaches to Shared State

See [Thread Safety Guidelines](../core-concepts/thread-safety.md) for comprehensive guidance, but here's a quick summary for parallel scenarios:

#### 1. IPipelineStateManager (Recommended)

For complex shared state that needs coordination across parallel workers:

```csharp
public override async ValueTask<TOut> TransformAsync(
    TIn input,
    PipelineContext context,
    CancellationToken ct)
{
    var result = ProcessItem(input);
    
    // Thread-safe state update via state manager
    var stateManager = context.StateManager;
    if (stateManager != null)
    {
        await stateManager.RecordMetricAsync("items_processed", 1, ct);
    }
    
    return result;
}
```

#### 2. Node-Level Synchronization

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

#### 3. Atomic Operations for Simple Counters

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

### ✅ Thread Safety DO's

- ✅ Process independent data items in parallel (inherently safe)
- ✅ Use `IPipelineStateManager` for shared state
- ✅ Use `lock` for simple critical sections
- ✅ Use `Interlocked` for atomic counter operations
- ✅ Keep synchronization scopes small and fast

### ❌ Thread Safety DON'Ts

- ❌ Directly access or modify `context.Items` from multiple threads
- ❌ Share mutable state between nodes without explicit synchronization
- ❌ Assume dictionaries in `PipelineContext` are thread-safe
- ❌ Hold locks across I/O operations (causes contention)
- ❌ Create complex multi-step interlocked sequences (use locks instead)

**For comprehensive guidance, see [Thread Safety Guidelines](../core-concepts/thread-safety.md).**

## Considerations for Parallelism

- **Degree of Parallelism:** Carefully choose the `MaxDegreeOfParallelism`. Too high a value can lead to excessive resource consumption (CPU, memory, threads) and diminish returns due to context switching overhead. Too low a value might underutilize available resources.

- **Thread Safety:** Ensure that any shared state or external resources accessed by your parallel nodes are thread-safe. If your nodes are pure functions (operating only on their input and producing output without side effects), this is less of a concern.

- **Order Preservation:** By default, NPipeline maintains the order of items even when processing them in parallel. If order is not critical and you need maximum throughput, you can configure nodes to not preserve order by setting `PreserveOrdering = false`.

- **Resource Contention:** Be aware of potential bottlenecks when multiple parallel tasks try to access the same limited resource (e.g., a single database connection, a slow API).

- **Debugging:** Debugging parallel code can be more complex. Ensure you have good logging and monitoring in place.

## Best Practices

- **Identify Parallelizable Work:** Apply parallelism to parts of your pipeline where operations are independent and computationally intensive.

- **Start Small:** Begin with a low degree of parallelism and incrementally increase it while monitoring performance metrics (CPU, memory, throughput) to find the optimal balance.

- **Profile:** Use profiling tools to identify bottlenecks and ensure that parallelism is indeed improving performance.

By strategically applying parallelism, you can significantly boost the processing capabilities of your NPipelines for demanding workloads.

## Next Steps

- **[Thread Safety Guidelines](../core-concepts/thread-safety.md)**: Comprehensive guide to thread safety and shared state management
- **[Dependency Injection](dependency-injection.md)**: Learn how to integrate NPipeline with dependency injection frameworks
- **[Testing Pipelines](testing/index.md)**: Understand how to effectively test your parallel pipelines


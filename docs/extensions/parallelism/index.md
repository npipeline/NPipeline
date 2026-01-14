---
title: Parallelism
description: Enhance your NPipeline's performance by leveraging parallel processing capabilities.
sidebar_position: 1
slug: /extensions/parallelism
---

# Parallelism

NPipeline is designed for high performance, and a key aspect of this is its ability to execute parts of your pipeline in parallel. The [`NPipeline.Extensions.Parallelism`](../../../src/NPipeline.Extensions.Parallelism/NPipeline.Extensions.Parallelism.csproj) package provides tools and extensions to easily introduce parallel processing into your data flows, allowing you to scale out your computations and maximize throughput.

## Understanding Parallelism in NPipeline

Parallelism in NPipeline typically means processing multiple data items concurrently, either within a single node or across multiple independent branches of a pipeline. This is distinct from concurrency, which is about managing multiple tasks that may or may not run simultaneously.

## [`NPipeline.Extensions.Parallelism`](../../../src/NPipeline.Extensions.Parallelism/NPipeline.Extensions.Parallelism.csproj)

This extension package provides the [`ParallelExecutionStrategy`](../../../src/NPipeline.Extensions.Parallelism/ParallelExecutionStrategy.cs) class and builder extensions to enable and manage parallel execution.

### Important: Thread Safety

When using parallel execution, it's critical to understand NPipeline's thread-safety model. Each worker thread processes **independent data items**—the `PipelineContext` itself is not shared across threads. However, if your nodes need to access shared state during parallel execution, you must use thread-safe mechanisms.

**See [Thread Safety Guidelines](./thread-safety.md) for detailed guidance on:**

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

        // Set execution strategy to ParallelExecutionStrategy
        transform.NodeDefinition.ExecutionConfig.ExecutionStrategy = new ParallelExecutionStrategy();
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

        // Set execution strategy to ParallelExecutionStrategy
        transform.NodeDefinition.ExecutionConfig.ExecutionStrategy = new ParallelExecutionStrategy();
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
    PreserveOrdering = true,              // Whether to preserve input order
    MetricsInterval = TimeSpan.FromSeconds(1) // Interval for metrics emission
};
```

### Metrics Configuration

The [`MetricsInterval`](../../../src/NPipeline.Extensions.Parallelism/ParallelOptions.cs:45) property controls how frequently parallel execution metrics are emitted:

- **Default value**: 1 second
- **Purpose**: Determines the interval at which performance metrics (throughput, queue depth, worker utilization) are collected and reported
- **When to adjust**:
  - Decrease for more granular monitoring in high-frequency trading or real-time analytics
  - Increase to reduce monitoring overhead in batch processing scenarios
  - Set to longer intervals when using custom metrics collection systems that batch data

```csharp
// Example: Fine-grained monitoring for real-time systems
var realtimeOptions = new ParallelOptions
{
    MaxDegreeOfParallelism = 4,
    MetricsInterval = TimeSpan.FromMilliseconds(500) // Emit metrics every 500ms
};

// Example: Reduced monitoring overhead for batch processing
var batchOptions = new ParallelOptions
{
    MaxDegreeOfParallelism = 8,
    MetricsInterval = TimeSpan.FromSeconds(10) // Emit metrics every 10 seconds
};
```

### Queue Policies

When `MaxQueueLength` is specified, you can control the behavior when the queue reaches its capacity:

- **`BoundedQueuePolicy.Block`**: Wait until space becomes available (default)
- **`BoundedQueuePolicy.DropNewest`**: Drop the incoming item
- **`BoundedQueuePolicy.DropOldest`**: Remove the oldest item to make space

### Default Queue Length

The [`ParallelNodeConfigurationExtensions.DefaultQueueLength`](../../../src/NPipeline.Extensions.Parallelism/ParallelNodeConfigurationExtensions.cs:27) constant defines the default queue length for bounded parallel execution strategies:

- **Default value**: 100 items
- **Applied to**: Drop-oldest and drop-newest parallel strategies when no explicit queue length is provided
- **Rationale**: This value balances memory usage with throughput, providing sufficient buffer for most workloads while preventing excessive memory consumption

```csharp
// Example: Using default queue length (100)
var transform = builder.AddTransform<MyTransform, int, string>()
    .WithDropOldestParallelism(builder, maxDegreeOfParallelism: 4);
    // Uses DefaultQueueLength of 100 automatically

// Example: Custom queue length for high-volume scenarios
var highVolumeTransform = builder.AddTransform<MyTransform, int, string>()
    .WithDropOldestParallelism(
        builder, 
        maxDegreeOfParallelism: 4,
        maxQueueLength: 1000); // Override default with larger queue
```

When to adjust the default queue length:

- **Increase** for high-throughput scenarios with bursty input patterns
- **Decrease** for memory-constrained environments or when processing large items
- **Keep default** for most typical workloads where balanced performance is desired

## Next Steps

- **[Thread Safety Guidelines](./thread-safety.md)**: Comprehensive guide to thread safety and shared state management in parallel execution
- **[Configuration](./configuration.md)**: Learn about different configuration APIs (Preset, Builder, and Manual)
- **[Validation](./validation.md)**: Learn about parallel configuration validation rules
- **[Best Practices](./best-practices.md)**: Guidelines for optimizing parallelism in your pipelines

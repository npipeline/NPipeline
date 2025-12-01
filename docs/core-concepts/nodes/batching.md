---
title: Batching Nodes
description: Learn how to group data items into batches for efficient processing in NPipeline.
sidebar_position: 2
---

# Batching Nodes

Batching nodes represent a deliberate operational shift in how NPipeline processes data. While NPipeline is built on the principle of **strict item-by-item flow control**—where each data item is processed individually and immediately forwarded—batching groups items together for specific practical reasons.

Batching is used when downstream operations need collecting a specified number of input items or items over a certain time period before processing them as a group. This is not an optimization but often a **necessity** for certain workloads: **bulk database inserts, transactional boundaries, and API calls that accept multiple records** require this grouping approach.

> **Architectural Pattern Shared with Aggregation:** Like [aggregation nodes](aggregation.md), batching represents a shift from NPipeline's item-level streaming model to higher-level data grouping. Both require you to step outside the default item-by-item pattern. The key difference: **batching groups by count/time** for operational efficiency, while **aggregation groups by key and event time** for data correctness. See [Aggregation Nodes](aggregation.md) for patterns that handle temporal ordering of events.

NPipeline provides the [`BatchingNode<T>`](src/NPipeline/Nodes/Batching/BatchingNode.cs) transform node and related extensions to simplify batching operations.

## [`BatchingNode<T>`](src/NPipeline/Nodes/Batching/BatchingNode.cs)

The [`BatchingNode<T>`](src/NPipeline/Nodes/Batching/BatchingNode.cs) is a transform that takes individual items of type `T` and outputs `IReadOnlyCollection<T>`, representing a batch of items.

### How It Works: A Practical Operational Choice

The `BatchingNode<T>` relies on the [`BatchingExecutionStrategy`](src/NPipeline/Execution/Strategies/BatchingExecutionStrategy.cs) to handle batching logic. The `ExecuteAsync` method throws a `NotSupportedException` when called directly—this signals that batching requires special handling: control shifts from processing individual items to the execution strategy's management of collected batches.

The batching strategy collects items until either the configured batch size is reached or a timeout expires, then emits the collected batch as `IReadOnlyCollection<T>`.

### Configuration

When you configure batching, you define explicit trade-offs:

* **Batch Size:** Maximum items per batch. Larger sizes increase throughput but increase latency and memory usage—items wait in the accumulator before processing.
* **Batch Timeout:** Maximum time to wait before emitting a partial batch. Shorter timeouts reduce latency; longer timeouts allow more accumulation and better efficiency.

### Example: Basic Batching

Let's create a pipeline that batches individual integers into lists of 3.

```csharp
using NPipeline;
using NPipeline.Nodes;
using NPipeline.Pipeline;

/// <summary>
/// Source node that produces a sequence of integers.
/// Demonstrates basic source pattern with controlled output.
/// </summary>
public sealed class IntSource : SourceNode<int>
{
    public override IDataPipe<int> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        // Create streaming data pipe immediately (synchronous operation)
        return new StreamingDataPipe<int>(GenerateNumbers());

        static async IAsyncEnumerable<int> GenerateNumbers()
        {
            // Produce 7 items with small delays to simulate work
            for (int i = 1; i <= 7; i++)
            {
                if (cancellationToken.IsCancellationRequested) yield break;
                Console.WriteLine($"Source: Producing {i}");
                yield return i;
                await Task.Delay(10, cancellationToken);
            }
        }
    }
}

/// <summary>
/// Sink node that consumes batches of integers.
/// Demonstrates batch processing pattern for grouped data.
/// </summary>
public sealed class BatchConsumerSink : SinkNode<IReadOnlyCollection<int>>
{
    /// <summary>
    /// Processes each batch as it arrives from batching node.
    /// Uses await foreach to efficiently iterate through batch stream.
    /// </summary>
    public async Task ExecuteAsync(
        IDataPipe<IReadOnlyCollection<int>> input, 
        PipelineContext context, 
        CancellationToken cancellationToken)
    {
        await foreach (var batch in input.WithCancellation(cancellationToken))
        {
            // Process entire batch at once
            Console.WriteLine($"Sink: Consumed batch of {batch.Count} items: [{string.Join(", ", batch)}]");
        }
    }
}

/// <summary>
/// Pipeline definition demonstrating basic batching functionality.
/// Shows how to configure batching with size and timeout parameters.
/// </summary>
public sealed class BatchingPipelineDefinition : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Add nodes to pipeline with descriptive names
        var sourceHandle = builder.AddSource<IntSource, int>("int_source");
        var batchHandle = builder.AddTransform<BatchingNode<int>, int, IReadOnlyCollection<int>>("batch_node");
        var sinkHandle = builder.AddSink<BatchConsumerSink, IReadOnlyCollection<int>>("batch_sink");

        // Connect nodes to define data flow
        builder.Connect(sourceHandle, batchHandle);
        builder.Connect(batchHandle, sinkHandle);
    }
}

public static class Program
{
    public static async Task Main(string[] args)
    {
        var context = PipelineContext.Default;
        var runner = PipelineRunner.Create();
        
        Console.WriteLine("Starting batching pipeline...");
        await runner.RunAsync<BatchingPipelineDefinition>(context);
        Console.WriteLine("Batching pipeline finished.");
    }
}
```

**Expected Output:**

```text
Starting batching pipeline...
Source: Producing 1
Source: Producing 2
Source: Producing 3
Sink: Consumed batch of 3 items: [1, 2, 3]
Source: Producing 4
Source: Producing 5
Source: Producing 6
Sink: Consumed batch of 3 items: [4, 5, 6]
Source: Producing 7
Sink: Consumed batch of 1 items: [7]
Batching pipeline finished.
```

Notice that the last batch contains only 1 item because the source finished producing, and the timeout (or end of pipeline) triggered the emission of the partial batch.

## [`BatchingPipelineBuilderExtensions`](src/NPipeline/Pipeline/BatchingPipelineBuilderExtensions.cs)

The [`BatchingPipelineBuilderExtensions`](src/NPipeline/Pipeline/BatchingPipelineBuilderExtensions.cs) provide a convenient fluent API for adding batching functionality to your pipeline. The `Batch()` extension method simplifies the creation and configuration of [`BatchingNode<T>`](src/NPipeline/Nodes/Batching/BatchingNode.cs).

```csharp
using NPipeline;
using NPipeline.Pipeline;

/// <summary>
/// Pipeline definition using batching extension method.
/// Demonstrates fluent API for configuring batching behavior.
/// </summary>
public sealed class BatchExtensionPipelineDefinition : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Add source node
        var sourceHandle = builder.AddSource<MySource, int>("source");
        
        // Add batching with explicit configuration
        // Batch size: 10 items maximum per batch
        // Timeout: 5 seconds maximum wait before emitting partial batch
        var batchHandle = builder.AddBatch<int>(
            batchSize: 10, 
            batchTimeout: TimeSpan.FromSeconds(5)
        );
        
        // Add sink for batch processing
        var sinkHandle = builder.AddSink<MyBatchProcessingSink, IReadOnlyCollection<int>>("sink");

        // Connect nodes to define data flow
        builder.Connect(sourceHandle, batchHandle);
        builder.Connect(batchHandle, sinkHandle);
    }
}

/// <summary>
/// Source node for demonstration purposes.
/// </summary>
public sealed class MySource : SourceNode<int>
{
    public override IDataPipe<int> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        return new StreamingDataPipe<int>(GenerateItems());

        static async IAsyncEnumerable<int> GenerateItems()
        {
            var random = new Random();
            for (int i = 0; i < 25; i++) // Produce 25 items
            {
                if (cancellationToken.IsCancellationRequested) yield break;
                yield return random.Next(1, 100);
                await Task.Delay(100, cancellationToken);
            }
        }
    }
}

/// <summary>
/// Sink node that processes batches with business logic.
/// </summary>
public sealed class MyBatchProcessingSink : SinkNode<IReadOnlyCollection<int>>
{
    public async Task ExecuteAsync(
        IDataPipe<IReadOnlyCollection<int>> input, 
        PipelineContext context, 
        CancellationToken cancellationToken)
    {
        await foreach (var batch in input.WithCancellation(cancellationToken))
        {
            // Process batch with business logic
            ProcessBatch(batch);
        }
    }

    private void ProcessBatch(IReadOnlyCollection<int> batch)
    {
        var sum = batch.Sum();
        var average = batch.Count > 0 ? (double)sum / batch.Count : 0;
        var min = batch.Min();
        var max = batch.Max();
        
        Console.WriteLine($"Batch of {batch.Count} items: Sum={sum}, Avg={average:F2}, Min={min}, Max={max}");
    }
}
```

## Architectural Costs and Considerations

Batching represents a deliberate choice to group items together. Understand these practical trade-offs:

* **Latency vs. Throughput:** Batching increases throughput by deferring item emission, which necessarily increases latency. Individual items wait in the accumulator before processing.

* **State Management:** Unlike streaming, batching requires higher-level state management (the accumulated batch). If an error occurs within a batch, the entire batch's context is affected—you cannot isolate errors to single items.

* **Partial Batches:** Ensure downstream nodes handle partial batches gracefully, especially at pipeline completion or timeout.

* **Error Handling:** Decide whether errors should fail the entire batch or only problematic items—more complex than item-by-item processing.

* **Memory Footprint:** Large batch sizes consume more memory. Balance throughput requirements against available memory.

Use batching when operational necessity (database efficiency, transactional boundaries, API constraints) justifies this architectural cost.

## Next Steps

* **[Join Nodes](join.md)**: Learn how to merge data from multiple input streams.
* **[Lookup Nodes](lookup.md)**: Discover how to enrich data by querying external sources.


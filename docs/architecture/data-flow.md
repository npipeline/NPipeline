---
title: Data Flow Details
description: How data pipes work and lazy evaluation in NPipeline.
sidebar_position: 4
---

# Data Flow Details

Understanding how data flows through NPipeline and the lazy evaluation principles that make it efficient is key to building high-performance pipelines.

## How Data Pipes Work

Data pipes are the channels through which data flows from nodes to the next stage in the pipeline.

**Data Pipe Interface:**

```csharp
public interface IDataPipe<T> : IAsyncEnumerable<T>
{
    // IDataPipe<T> implements IAsyncEnumerable<T> directly
    // Iterate using: await foreach (var item in dataPipe)
}
```

**Basic Data Flow:**

```csharp
// 1. Source produces a pipe
var sourcePipe = await sourceNode.Execute(context, cancellationToken);

// 2. Transform consumes and wraps it
var transformedPipe = new TransformPipe(sourcePipe, transformNode);

// 3. Sink consumes the pipe
await foreach (var item in transformedPipe.WithCancellation(cancellationToken))
{
    // Each item flows through here
}
```

## Lazy Evaluation

The key to NPipeline's efficiency is **lazy evaluation**: data is only processed when explicitly consumed.

### How Lazy Evaluation Works

```text
Step 1: Source creates pipe
        ↓
        Pipe exists, but no data is read yet
        
Step 2: Transform wraps pipe
        ↓
        Transform is ready, but no processing happens yet
        
Step 3: Sink iterates through pipe
        ↓
        NOW data flows:
        - Source reads item
        - Transform processes item
        - Sink consumes item
        - REPEAT for next item
```

**Code Example:**

```csharp
// Step 1: Source creates pipe (but doesn't read data yet)
var pipe = await source.Execute(context, cancellationToken);

// Step 2: Transform wraps pipe (but doesn't process yet)
var wrappedPipe = new TransformPipe(pipe, transform);

// Step 3: Sink actually triggers execution
await foreach (var item in wrappedPipe.WithCancellation(cancellationToken))
{
    // NOW data is read, transformed, consumed
    await sink.ProcessAsync(item);
}
```

### Benefits of Lazy Evaluation

**Early Termination:**

```csharp
// If pipeline is cancelled before consuming all items,
// source never reads remaining data
await foreach (var item in pipe.WithCancellation(cancellationToken))
{
    if (shouldStop)
    {
        cancellationToken.Cancel();
        break; // Source stops reading
    }
}
```

**Memory Efficiency:**

```csharp
// Reading 1 million items from a file:
// - Lazy: Only ~1 item in memory at a time
// - Eager (.ToList()): ~100 MB or more in memory
```

**Streaming Responsiveness:**

```csharp
// Results are available immediately
// Don't wait for entire dataset to load
await foreach (var result in pipeline.WithCancellation(cancellationToken))
{
    // Process each result as it's available
    await WriteToUIAsync(result);
}
```

## Composability of Data Pipes

Each transform creates a new data pipe, allowing for clean composition:

```csharp
var source = await sourceNode.Execute(context, ct);      // IDataPipe<Order>
var validated = new TransformPipe(source, validator);          // IDataPipe<ValidatedOrder>
var enriched = new TransformPipe(validated, enricher);         // IDataPipe<EnrichedOrder>
var processed = new TransformPipe(enriched, processor);        // IDataPipe<ProcessedOrder>

// Only when iterated does the entire chain execute
await foreach (var result in processed.WithCancellation(ct))
{
    // All transforms happen here for each item
}
```

## Memory Patterns

**Good: Streaming Processing**

```csharp
// Only one item in memory at a time
await foreach (var item in pipe.WithCancellation(ct))
{
    var result = await ProcessAsync(item);
    await WriteAsync(result);
    // Item is eligible for GC after this iteration
}
```

**Bad: Materializing Entire Stream**

```csharp
// Loads everything into memory!
var allItems = await pipe.ToListAsync(ct); // ❌ Bad for large datasets
foreach (var item in allItems)
{
    // Process...
}
```

## Next Steps

- **[Dependency Injection Integration](dependency-injection.md)** - Learn how DI works with NPipeline
- **[Performance Characteristics](performance-characteristics.md)** - Understand memory and throughput implications

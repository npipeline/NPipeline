---
title: Design Principles
description: Core principles guiding NPipeline's architecture.
sidebar_position: 10
---

# Design Principles

NPipeline is built on six core principles that guide its design and evolution:

## 1. Separation of Concerns

Each component has a single, well-defined responsibility:

- **Nodes** handle data transformation logic
- **Builders** handle pipeline composition
- **Runners** handle execution
- **Context** handles state management
- **Observability** handles diagnostics

```csharp
// Good: Each node focused on one thing
.AddSourceNode<CustomerSourceNode>()      // Only reads from database
.AddTransformNode<ValidationTransform>()  // Only validates
.AddTransformNode<EnrichmentTransform>()  // Only enriches
.AddSinkNode<AuditLogSink>()              // Only logs
```

This makes nodes testable, reusable, and easy to understand.

## 2. Lazy Evaluation

Data is only processed when explicitly consumed:

```csharp
var pipe = await source.ExecuteAsync(context, cancellationToken);
// No processing happens here - pipe exists but empty

var wrappedPipe = new TransformPipe(pipe, transform);
// Still no processing

await foreach (var item in wrappedPipe) // Processing happens HERE
{
    // Items flow through now
}
```

**Benefits:**

- Memory efficient - only what's needed stays in memory
- Responsive - results available immediately
- Cancellable - can stop at any point without wasting computation

## 3. Streaming First

NPipeline treats all data as streams, not arrays:

```csharp
// ❌ Wrong - breaks streaming model
public async IAsyncEnumerable<Output> ProcessAsync(...)
{
    var allItems = await _source.ToListAsync(); // Materializes!
    foreach (var item in allItems)
    {
        yield return item;
    }
}

// ✅ Right - maintains streaming model
public async IAsyncEnumerable<Output> ProcessAsync(...)
{
    await foreach (var item in _source)
    {
        yield return ProcessItem(item);
    }
}
```

All data flows as `IAsyncEnumerable<T>`, enabling true streaming composition.

## 4. Composability

Complex pipelines are built by composing simple, focused nodes:

```csharp
// Start simple
var pipeline = PipelineBuilder
    .AddSourceNode<SourceNode>()
    .AddTransformNode<TransformNode>()
    .AddSinkNode<SinkNode>()
    .BuildPipeline();

// Extend by adding more nodes
var extendedPipeline = PipelineBuilder
    .AddSourceNode<SourceNode>()
    .AddTransformNode<ValidationNode>()     // New validation step
    .AddTransformNode<EnrichmentNode>()     // New enrichment step
    .AddTransformNode<TransformNode>()      // Original transform
    .AddSinkNode<SinkNode>()
    .BuildPipeline();
```

Each node remains simple; complexity emerges from composition.

## 5. Testability

Nodes are designed to be testable in isolation:

```csharp
// Nodes are testable without a full pipeline
[Fact]
public async Task Transform_ValidInput_ProducesCorrectOutput()
{
    // Arrange
    var transform = new OrderValidationTransform();
    var testInput = new Order { Amount = 100 };

    // Act
    var results = new List<ValidatedOrder>();
    await foreach (var result in transform.ProcessAsync(testInput, CancellationToken.None))
    {
        results.Add(result);
    }

    // Assert
    Assert.Single(results);
    Assert.True(results[0].IsValid);
}
```

No mocking pipelines or runners needed - test the node directly.

## 6. Observability

Built-in diagnostics for understanding and troubleshooting:

```csharp
// Track execution
var context = PipelineContext.Default;
context.StartTracking();

await runner.ExecuteAsync(pipeline, context);

var stats = context.GetExecutionStatistics();
Console.WriteLine($"Items processed: {stats.ItemsProcessed}");
Console.WriteLine($"Processing time: {stats.TotalTime}");
Console.WriteLine($"Items per second: {stats.Throughput}");

// Access lineage for debugging
var lineage = context.Lineage.Items;
foreach (var item in lineage)
{
    Console.WriteLine($"Processed by: {item.NodeName}");
}
```

No external logging frameworks needed for core diagnostics.

## Design Trade-offs

These principles guide decisions when trade-offs arise:

| Trade-off | Principle | Decision | Reason |
|-----------|-----------|----------|--------|
| Memory vs Latency | Streaming First | Stream items immediately | Responsive to user, better memory profile |
| Composition vs Simplicity | Composability | Allow many nodes | Flexibility pays for itself in reuse |
| Strictness vs Flexibility | Separation of Concerns | Strict node contracts | Enables testing and optimization |
| Features vs Performance | Lazy Evaluation | Don't materialize | Supports large datasets |

## Next Steps

- Review **[Core Concepts](core-concepts.md)** to understand fundamentals
- Explore **[Extension Points](extension-points.md)** to build custom components
- Start with **[Getting Started - Quick Start](../getting-started/quick-start.md)**


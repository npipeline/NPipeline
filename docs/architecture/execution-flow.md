---
title: Execution Flow
description: Understanding how NPipeline executes pipelines - sequential and parallel execution patterns.
sidebar_position: 3
---

# Execution Flow

NPipeline supports multiple execution models to handle different requirements: sequential (the default) and parallel execution through extensions.

## Core Design: Synchronous Setup + Asynchronous Execution

NPipeline follows a clear separation of concerns:

**Synchronous Phase:** Pipeline Initialization
- All nodes' `ExecuteAsync` methods return synchronously
- Source nodes immediately return `IDataPipe<T>` objects
- Transform nodes return immediately with their execution strategies
- No waiting for actual data flow

**Asynchronous Phase:** Data Flow
- Data moves through pipes when nodes consume it
- Sinks iterate through pipes with `await foreach`
- Transforms process items as they arrive
- All async work happens during consumption, not setup

This design provides:
- ✅ **Clear execution boundaries:** Setup is fast; data flow is async
- ✅ **Predictable performance:** No hidden delays during initialization
- ✅ **Type safety:** Synchronous returns enable better variance
- ✅ **Memory efficiency:** No unnecessary Task allocations

## Sequential Execution (Default)

**Data Flow Pattern:**

```text
Source produces item 1
    ↓
Transform 1 processes item 1
    ↓
Transform 2 processes item 1
    ↓
Sink consumes item 1
    ↓
Source produces item 2
    ↓
Transform 1 processes item 2
    ↓
... (repeat)
```

**Characteristics:**

- Items flow one-at-a-time through the pipeline
- Order is preserved
- Minimal concurrency overhead
- Predictable memory usage
- Deterministic behavior

**Optimizations:**

The sequential strategy automatically detects and uses `ExecuteValueTaskAsync` when available on transform nodes. This avoids Task allocation overhead for synchronous operations, improving throughput for pipelines dominated by fast, synchronous transforms. See [Synchronous Fast Paths](../advanced-topics/synchronous-fast-paths.md) for details.

**When to Use:**

- Default for most pipelines
- When order preservation is critical
- When external systems require sequential processing
- When debugging is important (deterministic behavior)

**Example:**

```csharp
public class SequentialPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<OrderSource, Order>();
        var validator = builder.AddTransform<OrderValidator, Order, ValidatedOrder>();
        var enricher = builder.AddTransform<OrderEnricher, ValidatedOrder, EnrichedOrder>();
        var sink = builder.AddSink<OrderSink, EnrichedOrder>();

        builder.Connect(source, validator);
        builder.Connect(validator, enricher);
        builder.Connect(enricher, sink);
        
        // Sequential execution - default behavior
        // Each item flows one at a time through the pipeline
    }
}
```

## Parallel Execution

**Data Flow Pattern:**

```text
Source produces items 1, 2, 3, 4
    ↓
[Parallel Processing - Multiple items in flight]
Transform 1 processes items in parallel
    ↓
Transform 2 processes items in parallel
    ↓
Sink consumes items (possibly out of order)
```

**Characteristics:**

- Multiple items processed simultaneously
- Order may not be preserved
- Better CPU utilization on multi-core systems
- Increased memory usage
- Better throughput for CPU-bound work

**When to Use:**

- CPU-bound transforms (calculations, complex logic)
- Multi-core systems available
- Order doesn't matter or can be sorted later
- High throughput is a priority

**Configuration:**

Requires `NPipeline.Extensions.Parallelism`:

```csharp
using NPipeline.Extensions.Parallelism;

public class ParallelPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<DataSource, DataItem>();
        var transform = builder.AddTransform<ComplexTransform, DataItem, ProcessedItem>();
        var sink = builder.AddSink<ResultSink, ProcessedItem>();

        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        // Configure parallel execution
        builder.WithParallelOptions(
            transform,
            new ParallelOptions { MaxDegreeOfParallelism = 4 }
        );
    }
}
```

**Degree of Parallelism:**

- Use `Environment.ProcessorCount` for CPU-bound work
- Use smaller values (2-4) for I/O-bound work
- Start conservative and measure performance

## Hybrid Approaches

**Sequential Source → Parallel Transform → Sequential Sink:**

```csharp
public class HybridPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<MySource, Data>();
        var transform = builder.AddTransform<MyTransform, Data, Result>();
        var sink = builder.AddSink<MySink, Result>();

        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        // Only the transform runs in parallel
        builder.WithParallelism(transform, degree: 4);
    }
}
```

This approach:

- Produces data sequentially
- Processes items in parallel
- Consumes results (possibly buffering to maintain order)

## Performance Considerations

| Execution Model | Throughput | Memory | Complexity | Best For |
|---|---|---|---|---|
| **Sequential** | Low-Medium | Low | Low | Default, debugging, order-critical |
| **Parallel** | High | Medium-High | Medium | CPU-bound, high throughput |
| **Hybrid** | Medium-High | Medium | Medium | Mixed workloads |

## Next Steps

- **[Data Flow Details](data-flow.md)** - Understand how data pipes and lazy evaluation work
- **[Performance Characteristics](performance-characteristics.md)** - Learn about throughput and scalability

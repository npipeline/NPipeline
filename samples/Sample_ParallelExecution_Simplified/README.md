# Sample_ParallelExecution_Simplified

This sample demonstrates NPipeline's **simplified parallel execution API**, which makes it easier for new users to configure parallelism without deep knowledge
of all the underlying options.

## Overview

NPipeline provides three complementary approaches to configure parallel execution for transform nodes:

1. **Manual Configuration API** - Fully explicit control, requires specifying all parameters
2. **Preset API** - Simple, opinionated defaults based on workload type characteristics
3. **Builder API** - Flexible fluent configuration with sensible defaults

## Running the Sample

### Run with Manual Configuration API (default):

```bash
dotnet run
```

### Run with Preset API:

```bash
dotnet run preset
```

### Run with Builder API:

```bash
dotnet run builder
```

## Code Comparison

### Manual configuration API

```csharp
// Verbose - requires understanding all parameters
builder
    .AddTransform<MyTransform, Input, Output>()
    .WithBlockingParallelism(
        builder,
        maxDegreeOfParallelism: Environment.ProcessorCount * 4,
        maxQueueLength: Environment.ProcessorCount * 8,
        outputBufferCapacity: Environment.ProcessorCount * 16)
    .AddSink<MySink>();
```

**Pros:**

- Full control over every parameter
- Explicit about what's happening

**Cons:**

- Verbose and hard for newcomers
- Easy to make suboptimal choices
- Must calculate parallelism manually

### Preset API

```csharp
// Simple - just specify the workload type!
builder
    .AddTransform<MyTransform, Input, Output>()
    .RunParallel(builder, ParallelWorkloadType.IoBound)
    .AddSink<MySink>();
```

**Pros:**

- One line to configure parallelism
- Opinionated defaults based on workload characteristics
- Perfect for the common case

**Cons:**

- Less flexible if you need customization

### Builder API

```csharp
// Flexible - customize sensible defaults
builder
    .AddTransform<MyTransform, Input, Output>()
    .RunParallel(builder, opt => opt
        .MaxDegreeOfParallelism(8)
        .DropOldestOnBackpressure()
        .AllowUnorderedOutput())
    .AddSink<MySink>();
```

**Pros:**

- Fluent, chainable syntax
- Override only what you need
- Easy to read and understand

**Cons:**

- Slightly more verbose than presets

## Workload Types

The simplified API supports four workload type presets:

### `ParallelWorkloadType.General` (default)

- Recommended for: Mixed CPU and I/O workloads
- DOP: ProcessorCount × 2
- Queue Length: ProcessorCount × 4
- Output Buffer: ProcessorCount × 8
- Best for: Most scenarios, safe default choice

### `ParallelWorkloadType.CpuBound`

- Recommended for: CPU-intensive operations
- DOP: ProcessorCount (avoid oversubscription)
- Queue Length: ProcessorCount × 2
- Output Buffer: ProcessorCount × 4
- Best for: Pure computation, DSP, mathematical operations

### `ParallelWorkloadType.IoBound`

- Recommended for: I/O-intensive operations (file, database, local calls)
- DOP: ProcessorCount × 4 (hide I/O latency)
- Queue Length: ProcessorCount × 8
- Output Buffer: ProcessorCount × 16
- Best for: Database operations, file I/O, local service calls

### `ParallelWorkloadType.NetworkBound`

- Recommended for: Network operations (high latency)
- DOP: Min(ProcessorCount × 8, 100) (maximize throughput under high latency)
- Queue Length: 200 (large buffer for network delays)
- Output Buffer: 400
- Best for: HTTP calls, remote service calls, high-latency operations

## ParallelOptionsBuilder

For fine-grained control, use the `ParallelOptionsBuilder`:

```csharp
new ParallelOptionsBuilder()
    .MaxDegreeOfParallelism(8)              // Set max concurrent operations
    .MaxQueueLength(100)                     // Set input queue size
    .DropOldestOnBackpressure()              // Queue behavior when full
    .OutputBufferCapacity(50)                // Buffer after processing
    .AllowUnorderedOutput()                  // Disable ordering for throughput
    .MetricsInterval(TimeSpan.FromSeconds(2))
    .Build()
```

## When to Use Each API

| Scenario                   | Recommended API                        |
|----------------------------|----------------------------------------|
| New to NPipeline           | Preset API with `ParallelWorkloadType` |
| Common workload types      | Preset API                             |
| Need slight customization  | Builder API                            |
| Complex performance tuning | Manual API                             |
| Testing/prototyping        | Preset API                             |

## Performance Characteristics

With this sample's I/O-bound workload (100ms delays × 10 tasks):

- **Sequential execution**: ~1000ms
- **With IoBound preset**: ~500-600ms (8 parallel operations)
- **Speedup**: ~1.7-2x faster

The preset automatically selects `ProcessorCount * 4` parallelism, which is ideal for hiding I/O latency.

## Key Features

✅ **Simplified API** - Less configuration boilerplate
✅ **Type-safe** - Compile-time validation
✅ **Fluent syntax** - Readable and chainable
✅ **Sensible defaults** - Based on workload characteristics
✅ **Backward compatible** - Old APIs still work
✅ **Metrics support** - Built-in performance monitoring

## Related Documentation

- [Core Concepts - Parallelism](../../docs/core-concepts/parallelism.md)
- [Extensions - Parallelism](../../docs/extensions/parallelism.md)
- [Performance Optimization](../../docs/advanced-topics/performance.md)

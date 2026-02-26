---
title: Configuration
description: Learn about different configuration APIs (Preset, Builder, and Manual) for parallel execution.
sidebar_position: 3
---

# Parallel Execution Configuration

NPipeline provides multiple ways to configure parallel execution, each suited to different needs:

- **Preset API**: Best for common workload patterns with automatically optimized defaults
- **Builder API**: Best for flexible customization while starting from sensible defaults
- **Manual Configuration API**: Best for advanced performance tuning and complex scenarios

## Preset API: Using Workload Type Presets

For common workload patterns, use the `RunParallel` extension method with a workload type to automatically select optimal parallelism configuration:

```csharp
using NPipeline.Extensions.Parallelism;
using NPipeline.Pipeline;

public sealed class SimplifiedParallelPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        builder
            .AddTransform<MyTransform, Input, Output>()
            .RunParallel(builder, ParallelWorkloadType.IoBound)
            .AddSink<MySink>();
    }
}
```

That's it! The `RunParallel` method with `ParallelWorkloadType.IoBound` automatically configures:

- Degree of parallelism: `ProcessorCount * 4` (hide I/O latency)
- Queue length: `ProcessorCount * 8`
- Output buffer: `ProcessorCount * 16`
- Queue policy: Block (apply backpressure)

### Workload Type Presets

NPipeline provides four built-in presets optimized for common workload patterns:

#### `ParallelWorkloadType.General` (Default)

**Best for**: Mixed CPU and I/O workloads, when you're unsure

```csharp
builder
    .AddTransform<MyTransform, Input, Output>()
    .RunParallel(builder, ParallelWorkloadType.General)
```

**Configuration**:

- DOP: `ProcessorCount * 2`
- Queue: `ProcessorCount * 4`
- Buffer: `ProcessorCount * 8`

#### `ParallelWorkloadType.CpuBound`

**Best for**: CPU-intensive operations, mathematical computations, DSP

```csharp
builder
    .AddTransform<IntensiveMathTransform, double, double>()
    .RunParallel(builder, ParallelWorkloadType.CpuBound)
```

**Configuration** (avoids oversubscription):

- DOP: `ProcessorCount` (1:1 with CPU cores)
- Queue: `ProcessorCount * 2`
- Buffer: `ProcessorCount * 4`

#### `ParallelWorkloadType.IoBound`

**Best for**: File I/O, database operations, local service calls

```csharp
builder
    .AddTransform<DatabaseTransform, int, Record>()
    .RunParallel(builder, ParallelWorkloadType.IoBound)
```

**Configuration** (high parallelism hides I/O latency):

- DOP: `ProcessorCount * 4`
- Queue: `ProcessorCount * 8`
- Buffer: `ProcessorCount * 16`

#### `ParallelWorkloadType.NetworkBound`

**Best for**: HTTP calls, remote service calls, high-latency network operations

```csharp
builder
    .AddTransform<WebServiceTransform, Request, Response>()
    .RunParallel(builder, ParallelWorkloadType.NetworkBound)
```

**Configuration** (maximum throughput under high latency):

- DOP: `Min(ProcessorCount * 8, 100)` (capped at 100)
- Queue: `200` (large buffer)
- Buffer: `400`

## Builder API: Fine-Grained Control with ParallelOptionsBuilder

When you need to customize beyond the presets, use the fluent builder API for flexible configuration:

```csharp
builder
    .AddTransform<MyTransform, Input, Output>()
    .RunParallel(builder, opt => opt
        .MaxDegreeOfParallelism(8)
        .MaxQueueLength(100)
        .DropOldestOnBackpressure()
        .OutputBufferCapacity(50)
        .AllowUnorderedOutput()
        .MetricsInterval(TimeSpan.FromSeconds(2)))
    .AddSink<MySink>();
```

The `ParallelOptionsBuilder` provides full configuration:

```csharp
public class ParallelOptionsBuilder
{
    // Configure degree of parallelism
    public ParallelOptionsBuilder MaxDegreeOfParallelism(int value)

    // Configure input queue behavior
    public ParallelOptionsBuilder MaxQueueLength(int value)
    public ParallelOptionsBuilder BlockOnBackpressure()
    public ParallelOptionsBuilder DropOldestOnBackpressure()
    public ParallelOptionsBuilder DropNewestOnBackpressure()

    // Configure output buffering
    public ParallelOptionsBuilder OutputBufferCapacity(int value)
    public ParallelOptionsBuilder AllowUnorderedOutput()

    // Configure metrics
    public ParallelOptionsBuilder MetricsInterval(TimeSpan interval)

    // Build the final options
    public ParallelOptions Build()
}
```

## Comparison: Configuration Methods

| Aspect | Manual API | Preset API | Builder API |
|--------|---|---|---|
| **Lines of code** | 5-6 | 1 | 2-3 |
| **Parameters to understand** | All 5+ | 0 | 1-2 (as needed) |
| **Configuration style** | Explicit | Predefined | Fluent |
| **When to use** | Advanced tuning | Common patterns | Custom needs |
| **Learning curve** | Steeper | Gentle | Gradual |

**Manual Configuration**:

```csharp
.WithBlockingParallelism(
    builder,
    maxDegreeOfParallelism: Environment.ProcessorCount * 4,
    maxQueueLength: Environment.ProcessorCount * 8,
    outputBufferCapacity: Environment.ProcessorCount * 16)
```

**Preset API**:

```csharp
.RunParallel(builder, ParallelWorkloadType.IoBound)
```

**Builder API**:

```csharp
.RunParallel(builder, opt => opt
    .MaxDegreeOfParallelism(Environment.ProcessorCount * 4)
    .MaxQueueLength(Environment.ProcessorCount * 8))
```

## When to Use Each Approach

| Scenario | Recommendation |
|----------|---|
| New to NPipeline | Preset API with `ParallelWorkloadType` |
| Typical workloads | Preset API |
| Need slight customization | Builder API |
| Advanced performance tuning | Manual API |
| Prototyping | Preset API |
| Production optimization | Builder or Manual API (after profiling) |

## Example: Full Pipeline with Simplified API

```csharp
using NPipeline.Extensions.Parallelism;
using NPipeline.Pipeline;

public class FileProcessingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        builder
            // Read files (I/O-bound)
            .AddTransform<FileReaderTransform, string, FileContent>()
            .RunParallel(builder, ParallelWorkloadType.IoBound)
            
            // Parse content (CPU-bound)
            .AddTransform<ParserTransform, FileContent, ParsedData>()
            .RunParallel(builder, ParallelWorkloadType.CpuBound)
            
            // Upload results (network-bound)
            .AddTransform<UploaderTransform, ParsedData, UploadResult>()
            .RunParallel(builder, ParallelWorkloadType.NetworkBound)
            
            // Store results
            .AddSink<DatabaseSinkNode<UploadResult>>();
    }
}
```

This example shows how different stages of a pipeline can use different workload types, automatically configured for their specific characteristics.

## Related Topics

- **[Validation](./validation.md)**: Learn about parallel configuration validation rules
- **[Best Practices](./best-practices.md)**: Guidelines for optimizing parallelism in your pipelines
- **[Thread Safety](./thread-safety.md)**: Comprehensive guide to thread safety and shared state management

---
title: Validation
description: Learn about parallel configuration validation rules and how to prevent common mistakes.
sidebar_position: 4
---

# Validation of Parallel Configuration

NPipeline includes a **ParallelConfigurationRule** that validates your parallel execution settings at build time, helping prevent common mistakes that can cause performance issues or resource exhaustion.

## Automatic Validation

When you build a pipeline with parallel nodes, the validation rule automatically checks:

1. **Queue Limits with High Parallelism** - Warns if you have high parallelism (>4) without setting `MaxQueueLength`
   - This prevents unbounded memory growth if downstream processing is slower than upstream production
   - Set `MaxQueueLength` to 2-10x your `MaxDegreeOfParallelism`

2. **Order Preservation Overhead** - Warns if you preserve ordering with high parallelism (>8)
   - Preserving order with many workers requires significant buffering and reordering
   - Causes latency as items wait for slower workers to complete
   - Only preserve ordering if downstream requires it

3. **Drop Policies Without Queue Bounds** - Warns if you use drop policies without bounded queues
   - Drop policies (`DropOldest`, `DropNewest`) only work with bounded queues
   - Without `MaxQueueLength`, the drop policy has no effect

4. **Thread Explosion Detection** - Warns if parallelism exceeds `ProcessorCount * 4`
   - May indicate configuration error or unusual workload
   - Excessive parallelism can cause thread pool starvation

## Validation in Action

```csharp
var builder = new PipelineBuilder();

// ... build pipeline ...

// Validate before running
var result = builder.Validate();

if (!result.IsValid)
{
    Console.WriteLine("Errors:");
    foreach (var error in result.Errors)
        Console.WriteLine($"  ❌ {error}");
}

if (result.Warnings.Count > 0)
{
    Console.WriteLine("Warnings:");
    foreach (var warning in result.Warnings)
        Console.WriteLine($"  ⚠️  {warning}");
}

// Example output:
// ⚠️  Node 'transform' has high parallelism (16) but no queue limit (MaxQueueLength is null).
//     Consider setting MaxQueueLength to prevent unbounded memory growth.
// ⚠️  Node 'transform' preserves ordering with high parallelism (16). This may cause 
//     significant output buffering and latency. If ordering is not critical, consider 
//     .AllowUnorderedOutput() to improve throughput.
```

## Quick Fix Examples

```csharp
// PROBLEM: High parallelism without queue limits
var parallelOptions = new ParallelOptions(
    MaxDegreeOfParallelism: 16,
    MaxQueueLength: null);  // ⚠️ Warning!

// FIX: Set appropriate queue length
var parallelOptions = new ParallelOptions(
    MaxDegreeOfParallelism: 16,
    MaxQueueLength: 100);  // ✅ OK - Bounded to 6x parallelism

// PROBLEM: Preserving order with high parallelism
var options = builder.AddTransform<MyTransform, Input, Output>()
    .RunParallel(builder, opt => opt
        .MaxDegreeOfParallelism(16)
        .PreserveOrdering: true);  // ⚠️ High latency warning!

// FIX: Disable ordering for throughput
var options = builder.AddTransform<MyTransform, Input, Output>()
    .RunParallel(builder, opt => opt
        .MaxDegreeOfParallelism(16)
        .AllowUnorderedOutput());  // ✅ Better throughput
```

## Related Topics

- **[Configuration](./configuration.md)**: Learn about different configuration APIs (Preset, Builder, and Manual)
- **[Best Practices](./best-practices.md)**: Guidelines for optimizing parallelism in your pipelines
- **[Thread Safety](./thread-safety.md)**: Comprehensive guide to thread safety and shared state management
- **[Parallel Configuration Rule Details](../../core-concepts/pipeline-validation.md#parallel-configuration-rule-details)**: Complete validation documentation

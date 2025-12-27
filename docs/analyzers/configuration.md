---
title: Configuration Analyzers
description: Detect configuration issues that can cause performance problems, resource leaks, or silent failures in NPipeline pipelines.
sidebar_position: 6
---

## Configuration Analyzers

Configuration analyzers detect issues with pipeline configuration that can lead to performance problems, resource leaks, or silent failures. These analyzers focus on ensuring that your pipeline configuration is optimal for your workload and doesn't introduce hidden problems.

### NP9501: Unbounded Materialization Configuration

**ID:** `NP9501`  
**Severity:** Error  
**Category:** Configuration  

This analyzer detects when `PipelineRetryOptions.MaxMaterializedItems` is null or missing, which causes unbounded memory growth in `ResilientExecutionStrategy` and silently disables restart functionality. This is a critical configuration error that must be treated as a build-time error to prevent production failures.

#### Why This Matters

Unbounded materialization configuration causes:

1. **Memory Leaks**: Unlimited memory growth as items are materialized for retry scenarios
2. **Silent Failures**: Restart functionality is disabled without any indication
3. **Production Crashes**: OutOfMemoryException in high-throughput scenarios
4. **Resource Exhaustion**: System becomes unstable under load

#### Solution: Set MaxMaterializedItems

```csharp
// CORRECT: Set reasonable MaxMaterializedItems
var retryOptions = new PipelineRetryOptions(
    maxRetryCount: 3,
    baseDelay: TimeSpan.FromSeconds(1),
    maxDelay: TimeSpan.FromMinutes(1),
    maxMaterializedItems: 1000); // Bounded memory usage
```

#### Choosing the Right MaxMaterializedItems Value

| Scenario | Recommended Value | Reason |
|----------|-------------------|--------|
| Small items (< 1KB) | 1000-10000 | Low memory per item |
| Medium items (1KB-10KB) | 100-1000 | Balance memory and performance |
| Large items (> 10KB) | 10-100 | Prevent memory pressure |
| Memory-constrained environments | 10-50 | Conservative approach |
| High-throughput scenarios | 100-1000 | Depends on item size |

#### Enforcement Requirement

This analyzer is configured as an **Error** by default because unbounded materialization is fundamentally incompatible with resilient pipeline operation. It should not be downgraded to a warning, as doing so would allow silently broken resilience configurations to pass build time.

### NP9502: Inappropriate Parallelism Configuration

**ID:** `NP9502`  
**Severity:** Warning  
**Category:** Performance  

This analyzer detects inappropriate parallelism configurations that can cause resource contention, thread pool starvation, or suboptimal resource utilization in NPipeline pipelines.

#### Why This Matters

Inappropriate parallelism configuration causes:

1. **Resource Contention**: Too much parallelism competes for limited resources
2. **Thread Pool Starvation**: Excessive parallelism exhausts available threads
3. **Poor Performance**: Suboptimal resource utilization reduces throughput
4. **System Instability**: Overloaded system becomes unpredictable

#### Parallelism Guidelines

| Workload Type | Recommended Parallelism | PreserveOrdering |
|---------------|------------------------|------------------|
| CPU-bound | Processor count | Only if required |
| I/O-bound | Processor count × 2 | Only if required |
| Mixed | Processor count × 1.5 | Only if required |
| Memory-intensive | Processor count ÷ 2 | Only if required |

### NP9503: Batching Configuration Mismatch

**ID:** `NP9503`  
**Severity:** Warning  
**Category:** Performance  

This analyzer detects batching configurations where batch sizes and timeouts are misaligned, causing either excessive latency from large batches or inefficient processing from small batches.

#### Batching Configuration Guidelines

| Batch Size | Recommended Timeout | Use Case |
|------------|---------------------|-----------|
| 1-10 | 50-500ms | Low-latency scenarios |
| 10-100 | 500ms-2s | General purpose |
| 100-1000 | 1-10s | High-throughput scenarios |
| 1000+ | 5-30s | Batch processing systems |

### NP9504: Timeout Configuration Issues

**ID:** `NP9504`  
**Severity:** Warning  
**Category:** Configuration  

This analyzer detects inappropriate timeout configurations that can cause resource leaks, hanging operations, or inefficient resource utilization in NPipeline pipelines.

#### Timeout Guidelines

| Operation Type | Recommended Timeout | Maximum Timeout |
|----------------|---------------------|-----------------|
| Database I/O | 5-60 seconds | 5 minutes |
| Network I/O | 1-30 seconds | 2 minutes |
| File I/O | 10-120 seconds | 10 minutes |
| CPU-bound | 30 seconds-5 minutes | 30 minutes |
| Retry operations | 1-10 minutes | 30 minutes |

## Configuration Best Practices

1. **Profile Before Optimizing**: Measure actual performance when changing configuration
2. **Start Conservative**: Begin with conservative values and adjust based on metrics
3. **Monitor Resource Usage**: Watch memory, CPU, and thread pool metrics
4. **Test Under Load**: Validate configuration with realistic workloads

## Configuration

Adjust analyzer severity in `.editorconfig`:

```ini
# Treat unbounded materialization as errors
dotnet_diagnostic.NP9501.severity = error

# Treat inappropriate parallelism as warnings
dotnet_diagnostic.NP9502.severity = warning

# Treat batching mismatches as warnings
dotnet_diagnostic.NP9503.severity = warning

# Treat timeout issues as warnings
dotnet_diagnostic.NP9504.severity = warning
```

## See Also

- [Resilience Configuration](../../core-concepts/resilience/error-handling)
- [Performance Characteristics](../../architecture/performance-characteristics)

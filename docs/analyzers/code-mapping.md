---
title: Analyzer Code Reference
description: Complete mapping of NPipeline analyzer codes and their implementation details.
sidebar_position: 7
---

# NPipeline Analyzer Code Mapping

This document provides a comprehensive mapping of all NPipeline analyzer diagnostic codes, organized by category.

## Complete Analyzer Mapping

### Resilience Analyzers (NP90XX)

| Code | Name | Category | Description |
|------|------|----------|-------------|
| NP9001 | ResilientExecutionConfigurationAnalyzer | Resilience | Ensures proper configuration for node restart functionality |

### Async Programming Analyzers (NP91XX)

| Code | Name | Category | Description |
|------|------|----------|-------------|
| NP9101 | BlockingAsyncOperationAnalyzer | Performance | Detects blocking operations in async methods |
| NP9102 | OperationCanceledExceptionAnalyzer | Performance | Prevents swallowing of OperationCanceledException |
| NP9103 | SynchronousOverAsyncAnalyzer | Performance | Identifies sync-over-async anti-patterns |
| NP9104 | CancellationTokenRespectAnalyzer | Performance | Ensures proper cancellation token propagation |

### Performance Analyzers (NP92XX)

| Code | Name | Category | Description |
|------|------|----------|-------------|
| NP9201 | LinqInHotPathsAnalyzer | Performance | Prevents LINQ usage in performance-critical paths |
| NP9202 | InefficientStringOperationsAnalyzer | Performance | Identifies inefficient string operations |
| NP9203 | AnonymousObjectAllocationAnalyzer | Performance | Detects unnecessary anonymous object allocations |
| NP9204 | ValueTaskOptimizationAnalyzer | Performance | Encourages ValueTask optimization |
| NP9205 | SourceNodeStreamingAnalyzer | Performance | Ensures proper streaming patterns in source nodes |

### Reliability Analyzers (NP93XX)

| Code | Name | Category | Description |
|------|------|----------|-------------|
| NP9301 | InefficientExceptionHandlingAnalyzer | Reliability | Identifies inefficient exception handling patterns |
| NP9302 | SinkNodeInputConsumptionAnalyzer | Data Processing | Ensures sink nodes consume all input data |
| NP9303 | PipelineContextAccessAnalyzer | Best Practice | Detects unsafe PipelineContext access patterns |

### Best Practices Analyzers (NP94XX)

| Code | Name | Category | Description |
|------|------|----------|-------------|
| NP9401 | DependencyInjectionAnalyzer | Best Practice | Promotes proper dependency injection patterns |

### Configuration Analyzers (NP95XX)

| Code | Name | Category | Description |
|------|------|----------|-------------|
| NP9501 | UnboundedMaterializationConfigurationAnalyzer | Configuration | Prevents unbounded memory growth in retry options |
| NP9502 | InappropriateParallelismConfigurationAnalyzer | Configuration | Detects inappropriate parallelism settings |
| NP9503 | BatchingConfigurationMismatchAnalyzer | Configuration | Identifies batching configuration mismatches |
| NP9504 | TimeoutConfigurationAnalyzer | Configuration | Detects inappropriate timeout configurations |

## Quick Reference by Code

| Code | Analyzer | Category | Severity |
|------|----------|----------|----------|
| NP9001 | Resilience Configuration | Resilience | Warning |
| NP9101 | Blocking Operations | Performance | Warning |
| NP9102 | Swallowed Cancellation | Performance | Warning |
| NP9103 | Sync-Over-Async | Performance | Warning |
| NP9104 | Cancellation Not Respected | Performance | Warning |
| NP9201 | LINQ in Hot Paths | Performance | Warning |
| NP9202 | Inefficient Strings | Performance | Warning |
| NP9203 | Anonymous Objects | Performance | Warning |
| NP9204 | ValueTask Missing | Performance | Warning |
| NP9205 | Non-Streaming Source | Performance | Warning |
| NP9301 | Inefficient Exceptions | Reliability | Warning |
| NP9302 | Input Not Consumed | Data Processing | Error |
| NP9303 | Unsafe Context Access | Best Practice | Warning |
| NP9401 | Missing DI | Best Practice | Warning |
| NP9501 | Unbounded Materialization | Configuration | Error |
| NP9502 | Bad Parallelism | Configuration | Warning |
| NP9503 | Batching Mismatch | Configuration | Warning |
| NP9504 | Bad Timeouts | Configuration | Warning |

## By Category

### Resilience (NP90XX)
- NP9001: Incomplete resilience configuration detection

### Async Programming (NP91XX)
- NP9101: Blocking async operations
- NP9102: Swallowed cancellation exceptions
- NP9103: Fire-and-forget async patterns
- NP9104: Disrespected cancellation tokens

### Performance (NP92XX)
- NP9201: LINQ in hot paths
- NP9202: Inefficient string operations
- NP9203: Anonymous object allocations
- NP9204: Missing ValueTask optimization
- NP9205: Non-streaming SourceNode patterns

### Reliability (NP93XX)
- NP9301: Inefficient exception handling
- NP9302: Unconsumed sink node input
- NP9303: Unsafe PipelineContext access

### Best Practices (NP94XX)
- NP9401: Missing dependency injection

### Configuration (NP95XX)
- NP9501: Unbounded materialization
- NP9502: Inappropriate parallelism
- NP9503: Batching configuration mismatch
- NP9504: Timeout configuration issues

## Diagnostic Severity Levels

| Level | Meaning | Default Action |
|-------|---------|-----------------|
| Error | Critical issue preventing proper operation | Build fails |
| Warning | Issue that may cause problems | Build succeeds with warning |
| Info | Suggestion for improvement | Build succeeds silently |

## Configuration

All analyzers can be configured in `.editorconfig`:

```ini
# Configure individual analyzers
dotnet_diagnostic.NP9001.severity = error
dotnet_diagnostic.NP9101.severity = error
dotnet_diagnostic.NP9205.severity = error
```

## Learn More

- [Build-Time Analyzers Overview](./index.md)
- [Resilience Analyzers](./resilience.md)
- [Performance Analyzers](./performance.md)
- [Best Practice Analyzers](./best-practices.md)
- [Configuration Analyzers](./configuration.md)

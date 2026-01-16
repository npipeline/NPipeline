---
title: Analyzer Code Reference
description: Complete mapping of NPipeline analyzer codes and their implementation details.
sidebar_position: 7
---

# NPipeline Analyzer Code Mapping

This document provides a comprehensive mapping of all NPipeline analyzer diagnostic codes, organized by category.

## Complete Analyzer Mapping

### Configuration & Setup Analyzers (NP90XX)

| Code | Name | Category | Description |
|------|------|----------|-------------|
| NP9001 | ResilientExecutionConfigurationAnalyzer | Configuration & Setup | Ensures proper configuration for node restart functionality |
| NP9002 | UnboundedMaterializationConfigurationAnalyzer | Configuration & Setup | Prevents unbounded memory growth in retry options |
| NP9003 | InappropriateParallelismConfigurationAnalyzer | Configuration & Setup | Detects inappropriate parallelism settings |
| NP9004 | BatchingConfigurationMismatchAnalyzer | Configuration & Setup | Identifies batching configuration mismatches |
| NP9005 | TimeoutConfigurationAnalyzer | Configuration & Setup | Detects inappropriate timeout configurations |

### Performance & Optimization Analyzers (NP91XX)

| Code | Name | Category | Description |
|------|------|----------|-------------|
| NP9101 | BlockingAsyncOperationAnalyzer | Performance & Optimization | Detects blocking operations in async methods |
| NP9102 | SynchronousOverAsyncAnalyzer | Performance & Optimization | Identifies sync-over-async anti-patterns |
| NP9103 | LinqInHotPathsAnalyzer | Performance & Optimization | Prevents LINQ usage in performance-critical paths |
| NP9104 | InefficientStringOperationsAnalyzer | Performance & Optimization | Identifies inefficient string operations |
| NP9105 | AnonymousObjectAllocationAnalyzer | Performance & Optimization | Detects unnecessary anonymous object allocations |
| NP9106 | ValueTaskOptimizationAnalyzer | Performance & Optimization | Encourages ValueTask optimization |
| NP9107 | SourceNodeStreamingAnalyzer | Performance & Optimization | Ensures proper streaming patterns in source nodes |
| NP9108 | NodeParameterlessConstructorAnalyzer | Performance & Optimization | Suggests parameterless constructors for better performance |

### Reliability & Error Handling Analyzers (NP92XX)

| Code | Name | Category | Description |
|------|------|----------|-------------|
| NP9201 | OperationCanceledExceptionAnalyzer | Reliability & Error Handling | Prevents swallowing of OperationCanceledException |
| NP9202 | InefficientExceptionHandlingAnalyzer | Reliability & Error Handling | Identifies inefficient exception handling patterns |
| NP9203 | CancellationTokenRespectAnalyzer | Reliability & Error Handling | Ensures proper cancellation token propagation |

### Data Integrity & Correctness Analyzers (NP93XX)

| Code | Name | Category | Description |
|------|------|----------|-------------|
| NP9301 | SinkNodeInputConsumptionAnalyzer | Data Integrity & Correctness | Ensures sink nodes consume all input data |
| NP9302 | PipelineContextAccessAnalyzer | Data Integrity & Correctness | Detects unsafe PipelineContext access patterns |

### Design & Architecture Analyzers (NP94XX)

| Code | Name | Category | Description |
|------|------|----------|-------------|
| NP9401 | StreamTransformNodeSuggestionAnalyzer | Design & Architecture | Suggests using StreamTransformNode for streaming data |
| NP9402 | StreamTransformNodeExecutionStrategyAnalyzer | Design & Architecture | Detects StreamTransformNode with incompatible execution strategies |
| NP9403 | NodeParameterlessConstructorAnalyzer | Design & Architecture | Detects nodes without parameterless constructors |
| NP9404 | DependencyInjectionAnalyzer | Design & Architecture | Promotes proper dependency injection patterns |

## Quick Reference by Code

| Code | Analyzer | Category | Severity |
|------|----------|----------|----------|
| NP9001 | Resilience Configuration | Configuration & Setup | Warning |
| NP9002 | Unbounded Materialization | Configuration & Setup | Error |
| NP9003 | Inappropriate Parallelism | Configuration & Setup | Warning |
| NP9004 | Batching Mismatch | Configuration & Setup | Warning |
| NP9005 | Timeout Configuration | Configuration & Setup | Warning |
| NP9101 | Blocking Operations | Performance & Optimization | Warning |
| NP9102 | Sync-Over-Async | Performance & Optimization | Warning |
| NP9103 | LINQ in Hot Paths | Performance & Optimization | Warning |
| NP9104 | Inefficient Strings | Performance & Optimization | Warning |
| NP9105 | Anonymous Objects | Performance & Optimization | Warning |
| NP9106 | ValueTask Missing | Performance & Optimization | Warning |
| NP9107 | Non-Streaming Source | Performance & Optimization | Warning |
| NP9108 | Parameterless Constructor | Performance & Optimization | Info |
| NP9201 | Swallowed Cancellation | Reliability & Error Handling | Warning |
| NP9202 | Inefficient Exceptions | Reliability & Error Handling | Warning |
| NP9203 | Cancellation Not Respected | Reliability & Error Handling | Warning |
| NP9301 | Input Not Consumed | Data Integrity & Correctness | Error |
| NP9302 | Unsafe Context Access | Data Integrity & Correctness | Warning |
| NP9401 | StreamTransformNode Suggestion | Design & Architecture | Info |
| NP9402 | StreamTransformNode Execution Strategy | Design & Architecture | Warning |
| NP9403 | Missing Parameterless Constructor | Design & Architecture | Warning |
| NP9404 | Missing DI | Design & Architecture | Warning |

## By Category

### Configuration & Setup (NP90XX)

- NP9001: Incomplete resilience configuration detection
- NP9002: Unbounded materialization configuration
- NP9003: Inappropriate parallelism configuration
- NP9004: Batching configuration mismatch
- NP9005: Timeout configuration issues

### Performance & Optimization (NP91XX)

- NP9101: Blocking async operations
- NP9102: Sync-over-async patterns
- NP9103: LINQ in hot paths
- NP9104: Inefficient string operations
- NP9105: Anonymous object allocations
- NP9106: Missing ValueTask optimization
- NP9107: Non-streaming SourceNode patterns
- NP9108: Parameterless constructor performance suggestion

### Reliability & Error Handling (NP92XX)

- NP9201: Swallowed cancellation exceptions
- NP9202: Inefficient exception handling
- NP9203: Disrespected cancellation tokens

### Data Integrity & Correctness (NP93XX)

- NP9301: Unconsumed sink node input
- NP9302: Unsafe PipelineContext access

### Design & Architecture (NP94XX)

- NP9401: StreamTransformNode suggestion
- NP9402: StreamTransformNode execution strategy
- NP9403: Missing parameterless constructor
- NP9404: Missing dependency injection

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
dotnet_diagnostic.NP9002.severity = error
dotnet_diagnostic.NP9101.severity = error
dotnet_diagnostic.NP9107.severity = error
dotnet_diagnostic.NP9201.severity = error
dotnet_diagnostic.NP9301.severity = error
dotnet_diagnostic.NP9401.severity = error
```

## Learn More

- [Build-Time Analyzers Overview](./index.md)
- [Resilience Analyzers](./resilience.md)
- [Performance Analyzers](./performance.md)
- [Best Practice Analyzers](./best-practices.md)
- [Configuration Analyzers](./configuration.md)

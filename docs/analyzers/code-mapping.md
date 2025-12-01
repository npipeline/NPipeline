# NPipeline Analyzer Code Mapping Document

## Executive Summary

This document provides a comprehensive mapping of all NPipeline analyzer diagnostic codes, organized by category and implementation status. The analyzer implementation follows a standardized NP9XXX numbering scheme that logically groups related diagnostics by functionality:

- **NP90XX**: Resilience analyzers (error handling, restart configuration)
- **NP91XX**: Async Programming analyzers (blocking operations, cancellation tokens, sync-over-async)
- **NP92XX**: Performance analyzers (LINQ, string operations, allocations, streaming)
- **NP93XX**: Reliability analyzers (exception handling, input consumption, context access)
- **NP94XX**: Best Practices analyzers (dependency injection, code patterns)
- **NP95XX**: Configuration analyzers (retry options, parallelism, batching, timeouts)

This document serves as the authoritative reference for the analyzer code organization.

## Complete Analyzer Mapping

### Resilience Analyzers (NP90XX)

| Code | Analyzer Name | Source File | Documentation | Description |
|------|---------------|-------------|---------------|-------------|
| NP9001 | ResilientExecutionConfigurationAnalyzer | ResilientExecutionConfigurationAnalyzer.cs | docs/analyzers/resilience.md | Ensures proper configuration for node restart functionality |

### Async Programming Analyzers (NP91XX)

| Code | Analyzer Name | Source File | Documentation | Description |
|------|---------------|-------------|---------------|-------------|
| NP9101 | BlockingAsyncOperationAnalyzer | BlockingAsyncOperationAnalyzer.cs | docs/analyzers/performance.md | Detects blocking operations in async methods (.Result, .Wait(), GetAwaiter().GetResult()) |
| NP9102 | OperationCanceledExceptionAnalyzer | OperationCanceledExceptionAnalyzer.cs | docs/analyzers/performance.md | Prevents swallowing of OperationCanceledException |
| NP9103 | SynchronousOverAsyncAnalyzer | SynchronousOverAsyncAnalyzer.cs | docs/analyzers/performance.md | Identifies sync-over-async anti-patterns (fire-and-forget) |
| NP9104 | CancellationTokenRespectAnalyzer | CancellationTokenRespectAnalyzer.cs | docs/analyzers/performance.md | Ensures proper cancellation token propagation |

### Performance Analyzers (NP92XX)

| Code | Analyzer Name | Source File | Documentation | Description |
|------|---------------|-------------|---------------|-------------|
| NP9201 | LinqInHotPathsAnalyzer | LinqInHotPathsAnalyzer.cs | docs/analyzers/performance.md | Prevents LINQ usage in performance-critical paths |
| NP9202 | InefficientStringOperationsAnalyzer | InefficientStringOperationsAnalyzer.cs | docs/analyzers/performance.md | Identifies inefficient string operations |
| NP9203 | AnonymousObjectAllocationAnalyzer | AnonymousObjectAllocationAnalyzer.cs | docs/analyzers/performance.md | Detects unnecessary anonymous object allocations in hot paths |
| NP9204 | ValueTaskOptimizationAnalyzer | ValueTaskOptimizationAnalyzer.cs | docs/analyzers/performance.md | Encourages ValueTask optimization for sync-heavy paths |
| NP9205 | SourceNodeStreamingAnalyzer | SourceNodeStreamingAnalyzer.cs | docs/analyzers/data-processing.md | Ensures proper streaming patterns in source nodes |

### Reliability Analyzers (NP93XX)

| Code | Analyzer Name | Source File | Documentation | Description |
|------|---------------|-------------|---------------|-------------|
| NP9301 | InefficientExceptionHandlingAnalyzer | InefficientExceptionHandlingAnalyzer.cs | docs/analyzers/reliability.md | Identifies inefficient exception handling patterns |
| NP9302 | SinkNodeInputConsumptionAnalyzer | SinkNodeInputConsumptionAnalyzer.cs | docs/analyzers/data-processing.md | Ensures sink nodes consume all input data |
| NP9303 | PipelineContextAccessAnalyzer | PipelineContextAccessAnalyzer.cs | docs/analyzers/best-practices.md | Detects unsafe PipelineContext access patterns |

### Best Practices Analyzers (NP94XX)

| Code | Analyzer Name | Source File | Documentation | Description |
|------|---------------|-------------|---------------|-------------|
| NP9401 | DependencyInjectionAnalyzer | DependencyInjectionAnalyzer.cs | docs/analyzers/best-practices.md | Promotes proper dependency injection patterns |

### Configuration Analyzers (NP95XX)

| Code | Analyzer Name | Source File | Documentation | Description |
|------|---------------|-------------|---------------|-------------|
| NP9501 | UnboundedMaterializationConfigurationAnalyzer | UnboundedMaterializationConfigurationAnalyzer.cs | docs/analyzers/configuration.md | Prevents unbounded memory growth in retry options |
| NP9502 | InappropriateParallelismConfigurationAnalyzer | InappropriateParallelismConfigurationAnalyzer.cs | docs/analyzers/configuration.md | Detects inappropriate parallelism settings |
| NP9503 | BatchingConfigurationMismatchAnalyzer | BatchingConfigurationMismatchAnalyzer.cs | docs/analyzers/configuration.md | Identifies batching configuration mismatches |
| NP9504 | TimeoutConfigurationAnalyzer | TimeoutConfigurationAnalyzer.cs | docs/analyzers/configuration.md | Detects inappropriate timeout configurations |

## Quick Reference by Code

| Code | Analyzer | Category |
|------|----------|----------|
| **NP9001** | ResilientExecutionConfigurationAnalyzer | Resilience |
| **NP9101** | BlockingAsyncOperationAnalyzer | Async Programming |
| **NP9102** | OperationCanceledExceptionAnalyzer | Async Programming |
| **NP9103** | SynchronousOverAsyncAnalyzer | Async Programming |
| **NP9104** | CancellationTokenRespectAnalyzer | Async Programming |
| **NP9201** | LinqInHotPathsAnalyzer | Performance |
| **NP9202** | InefficientStringOperationsAnalyzer | Performance |
| **NP9203** | AnonymousObjectAllocationAnalyzer | Performance |
| **NP9204** | ValueTaskOptimizationAnalyzer | Performance |
| **NP9205** | SourceNodeStreamingAnalyzer | Performance / Data Processing |
| **NP9301** | InefficientExceptionHandlingAnalyzer | Reliability |
| **NP9302** | SinkNodeInputConsumptionAnalyzer | Reliability / Data Processing |
| **NP9303** | PipelineContextAccessAnalyzer | Reliability / Best Practices |
| **NP9401** | DependencyInjectionAnalyzer | Best Practices |
| **NP9501** | UnboundedMaterializationConfigurationAnalyzer | Configuration |
| **NP9502** | InappropriateParallelismConfigurationAnalyzer | Configuration |
| **NP9503** | BatchingConfigurationMismatchAnalyzer | Configuration |
| **NP9504** | TimeoutConfigurationAnalyzer | Configuration |

## Source Files Reference

All analyzer source files are located in `src/NPipeline.Analyzers/`:

- `ResilientExecutionConfigurationAnalyzer.cs` (NP9001)
- `BlockingAsyncOperationAnalyzer.cs` (NP9101)
- `OperationCanceledExceptionAnalyzer.cs` (NP9102)
- `SynchronousOverAsyncAnalyzer.cs` (NP9103)
- `CancellationTokenRespectAnalyzer.cs` (NP9104)
- `LinqInHotPathsAnalyzer.cs` (NP9201)
- `InefficientStringOperationsAnalyzer.cs` (NP9202)
- `AnonymousObjectAllocationAnalyzer.cs` (NP9203)
- `ValueTaskOptimizationAnalyzer.cs` (NP9204)
- `SourceNodeStreamingAnalyzer.cs` (NP9205)
- `InefficientExceptionHandlingAnalyzer.cs` (NP9301)
- `SinkNodeInputConsumptionAnalyzer.cs` (NP9302)
- `PipelineContextAccessAnalyzer.cs` (NP9303)
- `DependencyInjectionAnalyzer.cs` (NP9401)
- `UnboundedMaterializationConfigurationAnalyzer.cs` (NP9501)
- `InappropriateParallelismConfigurationAnalyzer.cs` (NP9502)
- `BatchingConfigurationMismatchAnalyzer.cs` (NP9503)
- `TimeoutConfigurationAnalyzer.cs` (NP9504)

## Documentation Files Reference

- `docs/analyzers/index.md` - Main analyzer overview with quick reference
- `docs/analyzers/resilience.md` - Resilience analyzers (NP9001)
- `docs/analyzers/performance.md` - Async and performance analyzers (NP9101-NP9104, NP9201-NP9204)
- `docs/analyzers/data-processing.md` - Data processing analyzers (NP9205, NP9302)
- `docs/analyzers/reliability.md` - Reliability analyzers (NP9301)
- `docs/analyzers/best-practices.md` - Best practices analyzers (NP9303, NP9401)
- `docs/analyzers/configuration.md` - Configuration analyzers (NP9501-NP9504)
- `docs/analyzers/code-fixes.md` - Code fix providers

## Conclusion

The NPipeline analyzer implementation follows a logical, category-based numbering scheme with 18 analyzers distributed across 6 functional categories. This mapping document serves as the authoritative reference for understanding the analyzer code organization.

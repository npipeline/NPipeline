# NPipeline Analyzer Code Mapping Document

## Executive Summary

This document provides a comprehensive mapping of all NPipeline analyzer diagnostic codes, organized by category and implementation status. The current analyzer implementation follows a standardized NP9XXX numbering scheme that logically groups related diagnostics by functionality:

- **NP90XX**: Resilience analyzers (error handling, cancellation, restart configuration)
- **NP91XX**: Async Programming analyzers (blocking operations, cancellation tokens)
- **NP92XX**: Data Flow analyzers (streaming, materialization, LINQ usage)
- **NP93XX**: Reliability analyzers (exception handling, input consumption)
- **NP94XX**: Best Practices analyzers (dependency injection, code patterns)
- **NP95XX**: Configuration analyzers (retry options, materialization limits)

The current implementation is already well-organized with consistent numbering, so no major reordering is required. This document serves as the authoritative reference for the existing code organization.

## Complete Analyzer Mapping

### Resilience Analyzers (NP90XX)

| Old Code | New Code | Analyzer Name | Source File | Documentation | Test File | Rationale |
|----------|----------|---------------|--------------|----------------|------------|-----------|
| NP9002 | NP9002 | ResilientExecutionConfigurationAnalyzer | ResilientExecutionConfigurationAnalyzer.cs | docs/analyzers/resilience.md | test/NPipeline.Analyzers.Tests/ResilientExecutionConfigurationAnalyzerTests.cs | Ensures proper configuration for node restart functionality |
| NP9103 | NP9103 | OperationCanceledExceptionAnalyzer | OperationCanceledExceptionAnalyzer.cs | docs/analyzers/reliability.md | test/NPipeline.Analyzers.Tests/OperationCanceledExceptionAnalyzerTests.cs | Prevents swallowing of OperationCanceledException |
| NP9105 | NP9105 | CancellationTokenRespectAnalyzer | CancellationTokenRespectAnalyzer.cs | docs/analyzers/resilience.md | test/NPipeline.Analyzers.Tests/CancellationTokenRespectAnalyzerTests.cs | Ensures proper cancellation token usage |

### Async Programming Analyzers (NP91XX)

| Old Code | New Code | Analyzer Name | Source File | Documentation | Test File | Rationale |
|----------|----------|---------------|--------------|----------------|------------|-----------|
| NP9102 | NP9102 | BlockingAsyncOperationAnalyzer | BlockingAsyncOperationAnalyzer.cs | docs/analyzers/performance.md | test/NPipeline.Analyzers.Tests/BlockingAsyncOperationAnalyzerTests.cs | Detects blocking operations in async methods |
| NP9104 | NP9104 | SynchronousOverAsyncAnalyzer | SynchronousOverAsyncAnalyzer.cs | docs/analyzers/performance.md | test/NPipeline.Analyzers.Tests/SynchronousOverAsyncAnalyzerTests.cs | Identifies sync-over-async anti-patterns |

### Data Flow Analyzers (NP92XX)

| Old Code | New Code | Analyzer Name | Source File | Documentation | Test File | Rationale |
|----------|----------|---------------|--------------|----------------|------------|-----------|
| NP9205 | NP9205 | LinqInHotPathsAnalyzer | LinqInHotPathsAnalyzer.cs | docs/analyzers/performance.md | test/NPipeline.Analyzers.Tests/LinqInHotPathsAnalyzerTests.cs | Prevents LINQ usage in performance-critical paths |
| NP9206 | NP9206 | InefficientStringOperationsAnalyzer | InefficientStringOperationsAnalyzer.cs | docs/analyzers/performance.md | test/NPipeline.Analyzers.Tests/InefficientStringOperationsAnalyzerTests.cs | Identifies inefficient string operations |
| NP9207 | NP9207 | AnonymousObjectAllocationAnalyzer | AnonymousObjectAllocationAnalyzer.cs | docs/analyzers/performance.md | test/NPipeline.Analyzers.Tests/AnonymousObjectAllocationAnalyzerTests.cs | Detects unnecessary object allocations |
| NP9209 | NP9209 | ValueTaskOptimizationAnalyzer | ValueTaskOptimizationAnalyzer.cs | docs/analyzers/performance.md | test/NPipeline.Analyzers.Tests/ValueTaskOptimizationAnalyzerTests.cs | Encourages ValueTask optimization |
| NP9211 | NP9211 | SourceNodeStreamingAnalyzer | SourceNodeStreamingAnalyzer.cs | docs/analyzers/data-processing.md | test/NPipeline.Analyzers.Tests/SourceNodeStreamingAnalyzerTests.cs | Ensures proper streaming patterns in source nodes |

### Reliability Analyzers (NP93XX)

| Old Code | New Code | Analyzer Name | Source File | Documentation | Test File | Rationale |
|----------|----------|---------------|--------------|----------------|------------|-----------|
| NP9302 | NP9302 | InefficientExceptionHandlingAnalyzer | InefficientExceptionHandlingAnalyzer.cs | docs/analyzers/reliability.md | test/NPipeline.Analyzers.Tests/InefficientExceptionHandlingAnalyzerTests.cs | Identifies inefficient exception handling patterns |
| NP9302 | NP9302 | SinkNodeInputConsumptionAnalyzer | SinkNodeInputConsumptionAnalyzer.cs | docs/analyzers/data-processing.md | test/NPipeline.Analyzers.Tests/SinkNodeInputConsumptionAnalyzerTests.cs | Ensures sink nodes consume all input data |

### Best Practices Analyzers (NP94XX)

| Old Code | New Code | Analyzer Name | Source File | Documentation | Test File | Rationale |
|----------|----------|---------------|--------------|----------------|------------|-----------|
| NP9401 | NP9401 | DependencyInjectionAnalyzer | DependencyInjectionAnalyzer.cs | docs/analyzers/best-practices.md | test/NPipeline.Analyzers.Tests/DependencyInjectionAnalyzerTests.cs | Promotes proper dependency injection patterns |

### Configuration Analyzers (NP95XX)

| Old Code | New Code | Analyzer Name | Source File | Documentation | Test File | Rationale |
|----------|----------|---------------|--------------|----------------|------------|-----------|
| NP9501 | NP9501 | UnboundedMaterializationConfigurationAnalyzer | UnboundedMaterializationConfigurationAnalyzer.cs | docs/analyzers/configuration.md | test/NPipeline.Analyzers.Tests/UnboundedMaterializationConfigurationAnalyzerTests.cs | Prevents unbounded memory growth in retry options |

## Implementation Phases

### Phase 1: Foundation (Complete)
- [x] Establish NP9XXX numbering scheme
- [x] Create category-based organization
- [x] Implement core resilience analyzers (NP90XX)
- [x] Implement async programming analyzers (NP91XX)

### Phase 2: Performance Optimization (Complete)
- [x] Implement data flow analyzers (NP92XX)
- [x] Implement reliability analyzers (NP93XX)
- [x] Implement best practices analyzers (NP94XX)
- [x] Implement configuration analyzers (NP95XX)

### Phase 3: Code Fix Providers (Complete)
- [x] Implement code fix providers for all analyzers
- [x] Add automated refactoring suggestions
- [x] Integrate with IDE light bulb functionality

### Phase 4: Documentation and Testing (Complete)
- [x] Create comprehensive documentation for each analyzer
- [x] Implement unit tests for all analyzers
- [x] Add integration tests for end-to-end scenarios

## Files That Need Updates

### Source Files (No Changes Required)
The current source files already follow the standardized numbering scheme and require no changes:

- `src/NPipeline.Analyzers/ResilientExecutionConfigurationAnalyzer.cs` (NP9002)
- `src/NPipeline.Analyzers/OperationCanceledExceptionAnalyzer.cs` (NP9103)
- `src/NPipeline.Analyzers/CancellationTokenRespectAnalyzer.cs` (NP9105)
- `src/NPipeline.Analyzers/BlockingAsyncOperationAnalyzer.cs` (NP9102)
- `src/NPipeline.Analyzers/SynchronousOverAsyncAnalyzer.cs` (NP9104)
- `src/NPipeline.Analyzers/LinqInHotPathsAnalyzer.cs` (NP9205)
- `src/NPipeline.Analyzers/InefficientStringOperationsAnalyzer.cs` (NP9206)
- `src/NPipeline.Analyzers/AnonymousObjectAllocationAnalyzer.cs` (NP9207)
- `src/NPipeline.Analyzers/ValueTaskOptimizationAnalyzer.cs` (NP9209)
- `src/NPipeline.Analyzers/SourceNodeStreamingAnalyzer.cs` (NP9211)
- `src/NPipeline.Analyzers/InefficientExceptionHandlingAnalyzer.cs` (NP9302)
- `src/NPipeline.Analyzers/SinkNodeInputConsumptionAnalyzer.cs` (NP9302)
- `src/NPipeline.Analyzers/DependencyInjectionAnalyzer.cs` (NP9401)
- `src/NPipeline.Analyzers/UnboundedMaterializationConfigurationAnalyzer.cs` (NP9501)

### Documentation Files (No Changes Required)
The current documentation files already reference the correct diagnostic IDs:

- `docs/analyzers/index.md` - Main analyzer overview
- `docs/analyzers/resilience.md` - Resilience analyzers (NP9002, NP9103, NP9105)
- `docs/analyzers/performance.md` - Performance analyzers (NP9102, NP9104, NP9205, NP9206, NP9207, NP9209)
- `docs/analyzers/data-processing.md` - Data processing analyzers (NP9211, NP9302)
- `docs/analyzers/reliability.md` - Reliability analyzers (NP9302)
- `docs/analyzers/best-practices.md` - Best practices analyzers (NP9401)
- `docs/analyzers/configuration.md` - Configuration analyzers (NP9501)
- `docs/analyzers/code-fixes.md` - Code fix providers

### Test Files (No Changes Required)
The current test files already reference the correct diagnostic IDs and require no updates.

## Cross-Reference Between Old and New Codes

Since the current implementation already follows the standardized NP9XXX numbering scheme, there are no old codes that need to be mapped to new codes. The existing codes are already in their final form:

### Resilience Category (NP90XX)
- **NP9002**: ResilientExecutionConfigurationAnalyzer - Complete
- **NP9103**: OperationCanceledExceptionAnalyzer - Complete (Note: Could be moved to NP9003 for better categorization)
- **NP9105**: CancellationTokenRespectAnalyzer - Complete (Note: Could be moved to NP9004 for better categorization)

### Async Programming Category (NP91XX)
- **NP9102**: BlockingAsyncOperationAnalyzer - Complete
- **NP9104**: SynchronousOverAsyncAnalyzer - Complete

### Data Flow Category (NP92XX)
- **NP9205**: LinqInHotPathsAnalyzer - Complete
- **NP9206**: InefficientStringOperationsAnalyzer - Complete
- **NP9207**: AnonymousObjectAllocationAnalyzer - Complete
- **NP9209**: ValueTaskOptimizationAnalyzer - Complete
- **NP9211**: SourceNodeStreamingAnalyzer - Complete

### Reliability Category (NP93XX)
- **NP9302**: InefficientExceptionHandlingAnalyzer - Complete
- **NP9302**: SinkNodeInputConsumptionAnalyzer - Complete

### Best Practices Category (NP94XX)
- **NP9401**: DependencyInjectionAnalyzer - Complete

### Configuration Category (NP95XX)
- **NP9501**: UnboundedMaterializationConfigurationAnalyzer - Complete

## Recommendations for Future Expansion

### Code Organization Improvements
1. **Consider reorganizing NP9103 and NP9105**: These resilience-related analyzers could be moved to the NP90XX range for better categorization:
   - NP9103 → NP9003 (OperationCanceledExceptionAnalyzer)
   - NP9105 → NP9004 (CancellationTokenRespectAnalyzer)

2. **Maintain consistent numbering**: Future analyzers should follow the established category-based numbering scheme.

### Documentation Enhancements
1. **Add cross-references**: Include links between related analyzers across different categories.
2. **Create quick reference cards**: Summarize each analyzer with code examples.
3. **Add troubleshooting guides**: Common issues and solutions for each analyzer.

### Testing Improvements
1. **Add integration tests**: Test analyzers in realistic pipeline scenarios.
2. **Performance benchmarks**: Measure analyzer impact on compilation time.
3. **Regression tests**: Ensure new analyzers don't interfere with existing ones.

## Conclusion

The current NPipeline analyzer implementation is already well-organized with a logical, category-based numbering scheme. The NP9XXX codes are properly distributed across functional areas, making the system intuitive and maintainable. No major code reorganization is required, though minor adjustments to code placement could improve categorization consistency.

This mapping document serves as the authoritative reference for understanding the current analyzer organization and planning future expansions.
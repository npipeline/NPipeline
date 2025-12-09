---
title: Build-Time Analyzers
description: NPipeline's comprehensive build-time analyzers enforce the framework's fundamental design contract through automated performance and resilience hygiene checks.
sidebar_position: 1
---

## Build-Time Guardrails: Automated Enforcement of Best Practices

The **NPipeline.Analyzers** package provides a comprehensive suite of build-time Roslyn analyzers that act as **automated guardrails for code quality**. Rather than treating them as just another error reference, these analyzers are the **framework's proactive enforcement mechanism** for ensuring your pipeline implementations follow the design principles that make NPipeline fast, safe, and reliable.

### What Are Build-Time Analyzers?

Build-time analyzers are diagnostic tools that scan your code at compile time—before it ever runs—to detect violations of the framework's fundamental design contract. They catch issues that would otherwise surface as runtime failures, silent data loss, performance bottlenecks, or maintenance headaches.

Think of them as **automated code review** by experts who understand how high-performance streaming systems should work.

## The NP9000 Series: Performance and Resilience Hygiene Toolkit

The NP9000 series (NP9XXX) diagnostics represent a curated set of enforcement rules that protect you from the most common—and most dangerous—mistakes when building streaming data pipelines:

| Code Range | Category | Purpose |
|-----------|----------|---------|
| **NP90XX** | **Resilience** | Enforces proper error handling and recovery configuration |
| **NP91XX** | **Performance** | Catches blocking operations, non-streaming patterns, and async/await anti-patterns |
| **NP92XX** | **Data Flow** | Ensures proper data consumption and processing patterns |
| **NP93XX** | **Reliability** | Detects inefficient exception handling and unsafe access patterns |
| **NP94XX** | **Best Practices** | Validates dependency injection, resource management, and framework contract compliance |
| **NP95XX** | **Configuration** | Detects configuration issues that can cause performance problems or silent failures |

### Why This Matters

Without these analyzers, developers could:

- ✗ Configure error handlers to restart nodes without the required prerequisites, causing silent failures
- ✗ Block on async code, causing deadlocks and thread pool starvation
- ✗ Build non-streaming SourceNodes that allocate gigabytes of memory for large datasets
- ✗ Inject dependencies unsafely, creating tightly coupled code that's hard to test
- ✗ Forget to consume input in SinkNodes, silently dropping data
- ✗ Access PipelineContext unsafely, causing null reference exceptions at runtime

**With these analyzers**, all of these issues are caught at build time, not at 3 AM in production.

## The Problem These Analyzers Solve

### Problematic Code: Silent Failures at Runtime

```csharp
// :x: Looks correct but will fail silently at runtime
public class MyErrorHandler : IPipelineErrorHandler
{
    public async Task<PipelineErrorDecision> HandleNodeFailureAsync(
        string nodeId,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        return error switch
        {
            TimeoutException => PipelineErrorDecision.RestartNode,  // Intent is clear
            _ => PipelineErrorDecision.FailPipeline
        };
    }
}

// But at runtime, restart silently fails because prerequisites are missing!
// The entire pipeline crashes instead of gracefully restarting the node.
```

### Solution: Build-Time Enforcement

```text
CSC : warning NP9001: Error handler can return PipelineErrorDecision.RestartNode
but the node may not have all three mandatory prerequisites configured...
```

You fix it during development, not during a production incident.

## Analyzer Categories

The analyzers are organized into focused sections based on what they protect:

- **[Resilience Analyzers](./resilience.md)** - Detect incomplete resilience configuration that could lead to silent failures
- **[Reliability Analyzers](./reliability.md)** - Identify inefficient exception handling patterns and unsafe access patterns
- **[Performance Analyzers](./performance.md)** - Identify blocking operations, non-streaming patterns, and async/await anti-patterns
- **[Best Practice Analyzers](./best-practices.md)** - Flag dependency injection anti-patterns, unsafe context access, and framework contract violations
- **[Data Processing Analyzers](./data-processing.md)** - Ensure proper input consumption and streaming patterns in pipeline nodes
- **[Configuration Analyzers](./configuration.md)** - Detect configuration issues that can cause performance problems, resource leaks, or silent failures
- **[Code Fix Providers](./code-fixes.md)** - Automated code fixes for common analyzer issues

## Installation

The analyzer is included with the main NPipeline package:

```bash
dotnet add package NPipeline
```

Or install it separately:

```bash
dotnet add package NPipeline.Analyzers
```

## Quick Reference: All Analyzer Codes

| Code | Category | Problem | Fix |
|------|----------|---------|-----|
| **NP9001** | Resilience | Incomplete resilience configuration | Add missing prerequisites |
| **NP9101** | Performance | Blocking operations in async methods | Use await instead of .Result/.Wait() |
| **NP9102** | Performance | Swallowed OperationCanceledException | Re-throw or handle explicitly |
| **NP9103** | Performance | Synchronous over async (fire-and-forget) | Await the async call |
| **NP9104** | Performance | Disrespecting cancellation token | Check token and propagate |
| **NP9201** | Performance | LINQ operations in hot paths | Use imperative alternatives |
| **NP9202** | Performance | Inefficient string operations | Use StringBuilder, interpolation, or spans |
| **NP9203** | Performance | Anonymous object allocation in hot paths | Use named types or value types |
| **NP9204** | Performance | Missing ValueTask optimization | Use ValueTask for sync-heavy paths |
| **NP9205** | Performance | Non-streaming patterns in SourceNode | Use IAsyncEnumerable with yield |
| **NP9210** | Data Flow | ITransformNode returning IAsyncEnumerable | Use IStreamTransformNode instead |
| **NP9211** | Data Flow | IStreamTransformNode with non-stream strategy | Use IStreamExecutionStrategy |
| **NP9301** | Reliability | Inefficient exception handling patterns | Use specific exception handling |
| **NP9302** | Data Processing | SinkNode input not consumed | Iterate the input pipe |
| **NP9303** | Best Practice | Unsafe PipelineContext access | Use null-safe patterns |
| **NP9401** | Best Practice | Direct dependency instantiation | Use constructor injection |
| **NP9501** | Configuration | Unbounded materialization configuration | Set MaxMaterializedItems value |
| **NP9502** | Configuration | Inappropriate parallelism configuration | Match parallelism to workload |
| **NP9503** | Configuration | Batching configuration mismatch | Align batch size and timeout |
| **NP9504** | Configuration | Timeout configuration issues | Set appropriate timeouts |

## Philosophy

These analyzers embody a core principle: **Getting it right before you compile is infinitely better than fixing it after it breaks in production.**

They enforce the framework's fundamental design contract:

- **Resilience**: Error handling must be configured completely or not at all
- **Performance**: Streaming operations must never block or materialize unnecessarily
- **Safety**: Dependencies must be explicit and context access must be protected
- **Correctness**: Data flow must be complete and cancellation must be respected

## Best Practices

1. **Never suppress warnings without understanding why** - The analyzer is protecting you from real problems
2. **Treat warnings as errors** - Consider configuring `.editorconfig` to make violations errors instead of warnings
3. **Use the analyzers as a learning tool** - Each warning teaches you something about building safe, fast pipelines
4. **Apply fixes during development** - It's always cheaper to fix issues at compile time

## Configuration

You can adjust analyzer severity in your `.editorconfig`:

```ini
# Treat all analyzer warnings as errors
dotnet_diagnostic.NP9001.severity = error
dotnet_diagnostic.NP9101.severity = error
dotnet_diagnostic.NP9103.severity = error
dotnet_diagnostic.NP9201.severity = error
dotnet_diagnostic.NP9202.severity = error
dotnet_diagnostic.NP9203.severity = error
dotnet_diagnostic.NP9205.severity = error
dotnet_diagnostic.NP9210.severity = info
dotnet_diagnostic.NP9211.severity = warning
dotnet_diagnostic.NP9301.severity = error
dotnet_diagnostic.NP9302.severity = error
dotnet_diagnostic.NP9303.severity = error
dotnet_diagnostic.NP9401.severity = error
dotnet_diagnostic.NP9501.severity = error
dotnet_diagnostic.NP9502.severity = warning
dotnet_diagnostic.NP9503.severity = warning
dotnet_diagnostic.NP9504.severity = warning
```

## See Also

- [Resilience Analyzers](./resilience.md) - Build resilient error handling
- [Reliability Analyzers](./reliability.md) - Improve exception handling and access patterns
- [Performance Analyzers](./performance.md) - Write fast, non-blocking code
- [Best Practice Analyzers](./best-practices.md) - Follow framework design principles
- [Data Processing Analyzers](./data-processing.md) - Ensure data flows correctly
- [Configuration Analyzers](./configuration.md) - Optimize pipeline configuration for performance and reliability
- [Code Fix Providers](./code-fixes.md) - Automated code fixes for common analyzer issues

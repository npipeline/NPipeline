---
title: Resilience Analyzers
description: Detect incomplete resilience configuration that could lead to silent failures and unexpected behavior.
sidebar_position: 2
---

## Resilience Analyzers

Resilience analyzers provide build-time detection of incomplete error handling configuration. While runtime validation throws clear `InvalidOperationException` exceptions when prerequisites are missing, these analyzers catch issues earlier during development.

### NP9001: Incomplete Resilient Configuration

**ID:** `NP9001`
**Severity:** Warning  
**Category:** Resilience  

This analyzer detects when an error handler can return `PipelineErrorDecision.RestartNode` but is missing one or more of three mandatory prerequisites for node restart functionality.

#### The Problem

Without using this analyzer, developers may miss one of the three mandatory prerequisites for node restart. The system validates these requirements at runtime and throws `InvalidOperationException` with clear messages when prerequisites are missing, but build-time detection helps catch issues earlier.

#### With Analyzer

```text
CSC : warning NP9001: Error handler can return PipelineErrorDecision.RestartNode
but node may not have all three mandatory prerequisites configured...
```

This gets flagged immediately at build time, during development, before runtime execution.

#### Solution: Complete Configuration

**For detailed step-by-step configuration instructions, see [Getting Started with Resilience](../../core-concepts/resilience/getting-started) guide.**

The three mandatory prerequisites are:

1. **ResilientExecutionStrategy** wrapping node
2. **MaxNodeRestartAttempts > 0** in PipelineRetryOptions  
3. **MaxMaterializedItems != null** in PipelineRetryOptions (bounded materialization)

If any prerequisite is missing when `RestartNode` is returned, the system throws `InvalidOperationException` at runtime with a clear message indicating which requirement failed.

#### Best Practices

1. **Always configure all three prerequisites together** - They work as a unit
2. **Use ResilientExecutionStrategy consistently** - Apply it to all nodes that need restart capability
3. **Set realistic MaxNodeRestartAttempts** - Usually 2-3 attempts is sufficient
4. **Configure MaxMaterializedItems appropriately** - Balance memory usage with retry capability

**Configuration Guidance:** Always set `MaxMaterializedItems` to a positive bounded value. Setting it to `null` (unbounded) will cause `InvalidOperationException` at runtime when `RestartNode` is attempted. See the [Getting Started with Resilience](../../core-concepts/resilience/getting-started) guide for detailed explanation of why bounded buffers are required for resilience guarantees.

## See Also

- **[Getting Started with Resilience](../../core-concepts/resilience/getting-started)** - Complete step-by-step configuration guide
- [Resilience Configuration Guide](../../core-concepts/resilience/error-handling)
- [Error Handling Architecture](../../architecture/error-handling-architecture)
- [Cancellation Model](../../architecture/cancellation-model)

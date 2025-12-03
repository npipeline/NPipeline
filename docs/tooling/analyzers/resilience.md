---
title: Resilience Analyzers
description: Detect incomplete resilience configuration that could lead to silent failures and unexpected behavior.
sidebar_position: 2
---

## Resilience Analyzers

Resilience analyzers protect against incomplete error handling configuration that can cause silent failures, where error recovery appears to succeed but silently fails at runtime.

### NP9001: Incomplete Resilient Configuration

**ID:** `NP9001`
**Severity:** Warning  
**Category:** Resilience  

This analyzer detects when an error handler can return `PipelineErrorDecision.RestartNode` but is missing one or more of the three mandatory prerequisites for node restart functionality.

#### The Problem

Without using this analyzer, developers can easily miss one of the three mandatory prerequisites for node restart, leading to silent failures where the entire pipeline would crash instead of recovering the failed node.

#### With the Analyzer

```text
CSC : warning NP9001: Error handler can return PipelineErrorDecision.RestartNode
but the node may not have all three mandatory prerequisites configured...
```

This gets flagged immediately at build time, during development.

#### Solution: Complete Configuration

**For detailed step-by-step configuration instructions, see the [Getting Started with Resilience](../../core-concepts/resilience/getting-started.md) guide.**

The three mandatory prerequisites are:

1. **ResilientExecutionStrategy** wrapping the node
2. **MaxNodeRestartAttempts > 0** in PipelineRetryOptions  
3. **MaxMaterializedItems != null** in PipelineRetryOptions (bounded materialization)

Missing even one of these prerequisites will **silently disable restart**, causing the entire pipeline to fail instead of recovering gracefully.

#### Best Practices

1. **Always configure all three prerequisites together** - They work as a unit
2. **Use ResilientExecutionStrategy consistently** - Apply it to all nodes that need restart capability
3. **Set realistic MaxNodeRestartAttempts** - Usually 2-3 attempts is sufficient
4. **Configure MaxMaterializedItems appropriately** - Balance memory usage with retry capability

**Critical Warning:** Never set `MaxMaterializedItems` to `null` (unbounded). This silently disables restart functionality and can cause OutOfMemoryException. See the [Getting Started with Resilience](../../core-concepts/resilience/getting-started.md) guide for detailed explanation of why unbounded buffers break resilience guarantees.

## See Also

- **[Getting Started with Resilience](../../core-concepts/resilience/getting-started.md)** - Complete step-by-step configuration guide
- [Resilience Configuration Guide](../core-concepts/resilience/error-handling.md)
- [Error Handling Architecture](../../architecture/error-handling-architecture.md)
- [Cancellation Model](../../architecture/cancellation-model.md)

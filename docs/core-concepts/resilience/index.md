---
title: Resilience Overview
description: Understand resilience concepts in NPipeline and how to build fault-tolerant data pipelines that can recover from failures.
sidebar_position: 1
---

# Resilience Overview

Resilience in NPipeline refers to the ability of your data pipelines to detect, handle, and recover from failures without complete system breakdown.
This section provides a comprehensive guide to building robust, fault-tolerant pipelines.

## ⚡ Quick Start

**If you want to enable node restarts and common retry patterns, [start here: Getting Started with Resilience](./getting-started.md)**

Resilience configuration requires understanding three mandatory prerequisites for node restarts. Missing any one causes silent failures. The getting-started guide is the single canonical source of truth for configuring all three prerequisites correctly, plus common retry patterns.

---

## Why Resilience Matters

In production environments, pipelines inevitably encounter failures from various sources:

- **Transient infrastructure issues**: Network timeouts, database connection failures
- **Data quality problems**: Invalid formats, missing values, unexpected data types
- **Resource constraints**: Memory pressure, CPU saturation, I/O bottlenecks
- **External service dependencies**: API rate limits, service outages, authentication failures

Without proper resilience mechanisms, these failures can cascade through your pipeline, causing data loss, system instability, and costly manual intervention.

## Resilience Strategy Comparison

| Strategy | Best For | Memory Requirements | Complexity | Key Benefits |
|----------|----------|-------------------|------------|--------------|
| **Simple Retry** | Transient failures (network timeouts, temporary service issues) | Low | Low | Quick recovery from temporary issues |
| **Node Restart** | Persistent node failures, resource exhaustion | Medium (requires materialization) | Medium | Complete recovery from node-level failures |
| **Circuit Breaker** | Protecting against cascading failures, external service dependencies | Low | Medium | Prevents system overload during outages |
| **Dead-Letter Queues** | Handling problematic items that can't be processed | Low | High | Preserves problematic data for manual review |
| **Combined Approach** | Production systems with multiple failure types | High | High | Comprehensive protection against all failure types |

### Choosing the Right Strategy

- **For simple pipelines with basic needs**: Start with Simple Retry
- **For streaming data processing**: Use Node Restart with materialization
- **For external service dependencies**: Add Circuit Breaker to prevent cascade failures
- **For critical data pipelines**: Implement Dead-Letter Queues to preserve failed items
- **For production systems**: Combine multiple strategies for comprehensive protection

## Core Resilience Components

NPipeline's resilience framework is built around several interconnected components:

| Component | Role | Critical Dependency |
|-----------|------|---------------------|
| **[ResilientExecutionStrategy](execution-with-resilience.md)** | Wrapper that enables recovery capabilities for nodes | Prerequisite for all resilience features |
| **[Materialization & Buffering](materialization.md)** | Buffers input items to enable replay during restarts | Required for `PipelineErrorDecision.RestartNode` |
| **[Error Handling](error-handling.md)** | Determines how to respond to different types of failures | Provides decision logic for recovery actions |
| **[Retry Options](retries.md)** | Configures retry limits and materialization caps | Controls resilience behavior boundaries |

## ⚠️ Critical Prerequisites for Node Restart (RestartNode)

If you intend to use `PipelineErrorDecision.RestartNode` to recover from failures, **[read the Getting Started with Resilience guide](./getting-started.md)** first.

You **must** configure all three of the following mandatory prerequisites. The guide provides detailed step-by-step instructions for each requirement.

> **💡 Pro Tip:** The NPipeline build-time analyzer (NP9002) detects incomplete resilience configurations at compile-time, preventing these silent failures. See [Build-Time Resilience Analyzer](../../analyzers/resilience.md) for details.

### Mandatory Requirements Summary

- **Requirement 1: ResilientExecutionStrategy**
  - The node must be wrapped with `ResilientExecutionStrategy`
  - Without this: Restart decisions are ignored; node cannot recover
  - **See detailed instructions:** [Getting Started with Resilience](./getting-started.md)

- **Requirement 2: MaxNodeRestartAttempts Configuration**
  - Set `MaxNodeRestartAttempts > 0` in `PipelineRetryOptions`
  - This enables the restart capability
  - **See detailed instructions:** [Getting Started with Resilience](./getting-started.md)

- **Requirement 3: MaxMaterializedItems Configuration**
  - Set `MaxMaterializedItems > 0` in `PipelineRetryOptions` (for streaming inputs)
  - This enables the input stream to be buffered/materialized for replay
  - **Critical:** Without this, even if RestartNode is requested, the pipeline will fall back to `FailPipeline`
  - **See detailed instructions:** [Getting Started with Resilience](./getting-started.md)

### What Happens If You Miss These

| Missing Component | What Goes Wrong | Observable Behavior |
|---|---|---|
| ResilientExecutionStrategy | Restart capability disabled | Error handler decisions are ignored; pipeline always fails |
| MaxMaterializedItems | Input stream not buffered | RestartNode falls back to `FailPipeline`; entire pipeline halts unexpectedly |
| Error Handler RestartNode | Restart never triggered | All errors result in pipeline failure, even recoverable ones |

**Example of Silent Failure:**

```csharp
// ❌ WRONG: Missing materialization
var options = new PipelineRetryOptions(
    MaxItemRetries: 3,
    MaxNodeRestartAttempts: 2,
    MaxMaterializedItems: null  // ← This is the problem!
);

// Developer expects RestartNode to work, but...
// When an error occurs and handler returns RestartNode:
// → Pipeline sees MaxMaterializedItems is not set
// → Falls back to FailPipeline
// → Entire pipeline halts (unexpected failure!)
```

**For complete configuration examples and detailed explanations, see the [Getting Started with Resilience](./getting-started.md) guide.**

## The Dependency Chain

Understanding the dependency relationships between resilience components is crucial for proper configuration:

```mermaid
graph TD
    A[ResilientExecutionStrategy] --> B[Materialization via MaxMaterializedItems]
    B --> C[PipelineErrorDecision.RestartNode]
    C --> D[Node Restart Functionality]

    E[IPipelineErrorHandler] --> F[Error Decision Logic]
    F --> C

    G[PipelineRetryOptions] --> H[Retry Limits]
    G --> I[Materialization Caps]
    H --> C
    I --> B

    J[Streaming Input] --> K{Materialization Required?}
    K -->|Yes| B
    K -->|No| L[Direct Processing]

    style A fill:#e1f5fe
    style B fill:#f3e5f5
    style C fill:#ffecb3
    style D fill:#e8f5e9
    style E fill:#e1f5fe
    style F fill:#f3e5f5
    style G fill:#e1f5fe
    style H fill:#f3e5f5
    style I fill:#f3e5f5
    style J fill:#e8f5e9
    style K fill:#ffecb3
    style L fill:#e8f5e9
```

*Figure: The dependency chain showing how resilience components must be configured in the correct sequence.*

### Critical Dependency Rules

1. **ResilientExecutionStrategy is mandatory**: All resilience features require this strategy to be applied to a node
2. **Materialization enables restarts**: `PipelineErrorDecision.RestartNode` only works if the input stream is materialized via `MaxMaterializedItems`
3. **Buffer size matters**: The `MaxMaterializedItems` value determines how many items can be replayed during a restart
4. **Streaming inputs need materialization**: Only streaming inputs require explicit materialization; already-buffered inputs work automatically

## Decision Flow for Choosing Resilience Strategies

Use this flow diagram to determine the appropriate resilience configuration for your use case:

```mermaid
flowchart TD
    A[Start: Node Configuration] --> B{Is fault tolerance required?}
    B -->|No| C[Use standard execution strategy]
    B -->|Yes| D[Apply ResilientExecutionStrategy]

    D --> E{What type of failures to handle?}
    E -->|Individual item failures| F[Configure NodeErrorDecision]
    E -->|Node/stream failures| G[Configure PipelineErrorDecision]
    E -->|Both| H[Configure both error handlers]

    F --> I{Is RestartNode needed?}
    G --> I
    H --> I

    I -->|No| J[Standard resilience configuration]
    I -->|Yes| K{Is input streaming?}

    K -->|No| L[RestartNode will work automatically]
    K -->|Yes| M[Set MaxMaterializedItems > 0]

    M --> N{Memory constraints?}
    N -->|No| O[Set high MaxMaterializedItems]
    N -->|Yes| P[Set limited MaxMaterializedItems]

    L --> Q[Complete resilience configuration]
    O --> Q
    P --> R[Monitor for buffer overflows]
    R --> Q

    C --> S[Standard pipeline setup]
    Q --> S

    style A fill:#e8f5e9
    style D fill:#e1f5fe
    style I fill:#ffecb3
    style K fill:#f3e5f5
    style N fill:#f3e5f5
    style Q fill:#e8f5e9
    style S fill:#e8f5e9
```

## Key Scenarios

### Scenario 1: Simple Retry Logic

For handling transient failures without node restarts:

- Apply `ResilientExecutionStrategy`
- Configure `NodeErrorDecision.Retry` or `NodeErrorDecision.Skip`
- No materialization required

### Scenario 2: Node Restart Capability

For recovering from node-level failures:

- Apply `ResilientExecutionStrategy`
- Configure `PipelineErrorDecision.RestartNode`
- Set `MaxMaterializedItems` to enable replay (for streaming inputs)
- **See detailed configuration:** [Getting Started with Resilience](./getting-started.md)

### Scenario 3: Memory-Constrained Environment

For systems with limited memory:

- Apply `ResilientExecutionStrategy`
- Set `MaxMaterializedItems` to a conservative value
- Monitor for buffer overflow exceptions
- Consider alternative recovery strategies

## Next Steps

- **[Getting Started with Resilience](./getting-started.md)**: Quick-start guides for node restarts and retry delays with common patterns
- **[Error Handling](./error-handling.md)**: Learn how to handle errors at node and pipeline levels
- **[Retries](./retries.md)**: Configure retry behavior for items and node restarts
- **[Circuit Breakers](./circuit-breakers.md)**: Prevent cascading failures with circuit breaker patterns
- **[Materialization & Buffering](./materialization.md)**: Understand how buffering enables replay functionality
- **[Dead-Letter Queues](./dead-letter-queues.md)**: Implement dead-letter queues for problematic items
- **[Troubleshooting](./troubleshooting.md)**: Diagnose and resolve common resilience issues

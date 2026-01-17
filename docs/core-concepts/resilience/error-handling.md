---
title: Error Handling in NPipeline
description: Comprehensive guide to error handling at both node and pipeline levels in NPipeline
sidebar_position: 1
---

# Error Handling

Robust error handling is critical for building reliable data pipelines. NPipeline provides mechanisms to manage errors at two complementary levels.

## The Two Levels

### Node-Level Error Handling

Handles errors for individual items within a node. When an item fails, decide whether to skip it, retry it, redirect it to dead-letter, or fail the pipeline.

**→ [Node-Level Error Handling](node-error-handling.md)**

### Pipeline-Level Error Handling

Handles errors that affect an entire node's stream. When a node fails, decide whether to restart it, continue without it, or fail the pipeline.

**→ [Pipeline-Level Error Handling](pipeline-error-handling.md)**

## Choosing Your Approach

```mermaid
graph TD
    A["Error Occurs<br/>in Pipeline"] --> B{Where did the<br/>error occur?}
    B -->|Single Item Failed| C["Use Node-Level<br/>Error Handling"]
    B -->|Entire Stream Failed| D["Use Pipeline-Level<br/>Error Handling"]
    C --> E{Recovery Action?}
    E -->|Skip & Continue| F["Skip Handling"]
    E -->|Try Again| G["Retry Strategy"]
    E -->|Separate Processing| H["Dead Letter Queue"]
    E -->|Stop Pipeline| I["Fail Handler"]
    D --> J{Recovery Action?}
    J -->|Restart Node| K["Node Restart<br/>with Prerequisites"]
    J -->|Continue| L["Graceful Degradation"]
    J -->|Stop Pipeline| M["Pipeline Failure"]
    
    style A fill:#ffcdd2
    style C fill:#e3f2fd
    style D fill:#e3f2fd
    style F fill:#c8e6c9
    style G fill:#fff9c4
    style H fill:#ffe0b2
    style I fill:#ffcdd2
    style K fill:#c8e6c9
    style L fill:#c8e6c9
    style M fill:#ffcdd2
```

### Decision Guidance

**Use Node-Level Error Handling when:**

- Individual items fail during processing
- You want to handle them without affecting other items
- You need to skip, retry, or redirect problematic items

**Use Pipeline-Level Error Handling when:**

- An entire node's stream fails (e.g., external service goes down)
- You need to decide how the pipeline should recover
- You want to implement circuit breaker patterns

**Use Both when:**

- You need comprehensive error management at all levels

## Related Topics

- [Retries](retries.md) - Configure retry policies and strategies
- [Circuit Breakers](circuit-breakers.md) - Implement circuit breaker patterns
- [Dead Letter Queues](dead-letter-queues.md) - Route problematic items for analysis
- [Getting Started with Resilience](getting-started.md) - Quick guide and prerequisites
- [Troubleshooting](troubleshooting.md) - Common error handling issues and solutions

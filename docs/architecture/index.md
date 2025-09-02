---
title: Architecture Overview
description: How NPipeline works internally.
sidebar_position: 12
---

# Architecture Overview

## Prerequisites

Before understanding NPipeline architecture, you should be familiar with:
- [Core Concepts Overview](../core-concepts/index.md) - Basic NPipeline concepts and terminology
- [Nodes Overview](../core-concepts/nodes/index.md) - Understanding the fundamental building blocks

This section explains **how NPipeline works internally** - design decisions, 
performance optimizations, and engineering principles that make NPipeline fast 
and reliable.

> **For implementation guides**, see [Core Concepts](../core-concepts/index.md).

## Understanding NPipeline Architecture

NPipeline's architecture is designed around several core principles: graph-based data flow, lazy evaluation, streaming-first design, and composability. This section breaks down each major architectural component to help you understand how system works internally.

## Main Architecture Sections

### [Core Concepts](core-concepts.md)

Learn to fundamental architectural building blocks:

* Graph-Based Architecture (DAGs)
* Node Types (Source, Transform, Sink)
* Streaming Data Model with `IAsyncEnumerable<T>`

### [Optimization Principles](optimization-principles.md)

Understand how NPipeline achieves exceptional performance:

* Plan-based execution eliminating per-item decisions
* Zero reflection during steady state
* ICountable for right-sized allocations
* ValueTask optimization (up to 90% GC reduction)
* Streaming-first design with lazy evaluation
* Memory layout and cache efficiency

### [Component Architecture](component-architecture.md)

Explore of major system components and their roles:
- Pipeline Definition (`IPipelineDefinition`)
- Pipeline Builder
- Pipeline Context
- Pipeline Runner
- Node Execution Model

### [Execution Flow](execution-flow.md)

Understand how pipelines execute data:
- Sequential Execution (default)
- Parallel Execution patterns
- Data flow through pipeline graph

### [Data Flow Details](data-flow.md)

Deep dive into how data moves through system:
- How Data Pipes Work
- Lazy Evaluation principles
- Memory efficiency patterns

### [Dependency Injection Integration](dependency-injection.md)

Learn how DI is integrated into NPipeline:
- Automatic Node Resolution
- Injecting Dependencies into Nodes
- Service Provider Integration

### [Error Handling Architecture](error-handling-architecture.md)

Understand error propagation and handling:
- Error Propagation mechanisms
- Error Containment strategies
- Dead-Letter Handling for failed items
- Supporting Components (Lineage, Dead Letter Sinks, State Registry)

### [Cancellation Model](cancellation-model.md)

Learn about graceful shutdown and cancellation:
- Token Propagation through pipeline
- Node cancellation responsibilities
- Graceful termination patterns

### [Performance Characteristics](performance-characteristics.md)

Understand performance implications:
- Memory Usage patterns
- Throughput characteristics
- Scalability strategies (vertical and horizontal)

### [Extension Points](extension-points.md)

Discover how to extend NPipeline:
- Custom Nodes
- Custom Execution Strategies
- Custom Context Data

### [Design Principles](design-principles.md)

Learn to core design philosophy:
- Separation of Concerns
- Lazy Evaluation
- Streaming First
- Composability
- Testability
- Observability

## High-Level Architecture Diagram

```mermaid
graph TD
    subgraph "Pipeline Definition Phase"
        PD[PipelineDefinition] -->|Defines| PB[PipelineBuilder]
        PB -->|Creates| Nodes[Nodes]
        PB -->|Connects| Graph[Execution Graph]
    end

    subgraph "Pipeline Execution Phase"
        PR[PipelineRunner] -->|Instantiates| PD
        PR -->|Creates| PB
        PR -->|Provides| PC[PipelineContext]
        PC -->|Contains| Items[Shared Items]
        PC -->|Provides| SP[ServiceProvider]
        PC -->|Propagates| CT[CancellationToken]

        subgraph "Data Flow"
            SN[SourceNode] -->|Produces| DP1[IDataPipe<T>]
            DP1 -->|Consumes| TN1[TransformNode]
            TN1 -->|Transforms| DP2[IDataPipe<TOut>]
            DP2 -->|Consumes| TN2[TransformNode]
            TN2 -->|Transforms| DP3[IDataPipe<TOut2>]
            DP3 -->|Consumes| SK[SinkNode]
        end

        subgraph "Supporting Components"
            PLSP[IPipelineLineageSinkProvider] -->|Tracks| Lineage[Data Lineage]
            DLS[IDeadLetterSink] -->|Handles| FailedItems[Failed Items]
            SR[StatefulRegistry] -->|Manages| State[Node State]
        end
    end

    Graph -->|Executes| SN
    PC -->|Shared Context| SN
    PC -->|Shared Context| TN1
    PC -->|Shared Context| TN2
    PC -->|Shared Context| SK

    Lineage -->|Observes| DP1
    Lineage -->|Observes| DP2
    Lineage -->|Observes| DP3

    FailedItems -->|Captured by| DLS
    State -->|Tracked by| SR

    style PD fill:#e1f5fe
    style PB fill:#f3e5f5
    style PR fill:#e8f5e9
    style PC fill:#fff3e0
    style SN fill:#e3f2fd
    style TN1 fill:#e3f2fd
    style TN2 fill:#e3f2fd
    style SK fill:#e3f2fd
    style PLSP fill:#fff8e1
    style DLS fill:#ffebee
    style SR fill:#f1f8e9
```

*Figure: NPipeline architecture showing relationship between core components, data flow through graph-based pipeline, and supporting components*

## See Also

- [Core Concepts Overview](../core-concepts/index.md) - For implementation guides and practical usage
- [Optimization Principles](optimization-principles.md) - Deep dive into performance optimizations
- [Component Architecture](component-architecture.md) - Detailed component interactions
- [Execution Flow](execution-flow.md) - How data flows through the pipeline
- [Design Principles](design-principles.md) - Core philosophy behind NPipeline's design
- [Performance Characteristics](performance-characteristics.md) - Understanding performance implications
- [Extension Points](extension-points.md) - How to extend NPipeline functionality

## Next Steps

* Start with **[Core Concepts](core-concepts.md)** to understand fundamental building blocks
* Explore **[Component Architecture](component-architecture.md)** to see how components interact
* Dive into **[Optimization Principles](optimization-principles.md)** to understand why NPipeline is fast
* Review **[Common Patterns](../core-concepts/common-patterns.md)** for real-world examples
* Review **[Best Practices](../core-concepts/best-practices.md)** for design guidelines

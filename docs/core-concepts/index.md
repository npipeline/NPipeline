---
title: Core Concepts
description: Implementation guides for building pipelines with NPipeline.
sidebar_position: 1
slug: /core-concepts
---

# Core Concepts

This section provides practical guides and implementation patterns for building pipelines with NPipeline. **Each guide embeds the rationale directly with the implementation guidance** — you'll understand both the how and the why without constant navigation between pages.

For deeper exploration of architectural decisions and internal mechanics, see [Architecture Overview](../architecture/index.md).

NPipeline is designed with a few key concepts at its heart. Understanding these will help you build powerful and efficient data pipelines.

---

## 1. Foundational Concepts

Start here to understand the core building blocks of NPipeline.

| Concept | Description |
| :--- | :--- |
| **[Pipeline](ipipeline.md)** | The executable instance of a pipeline. It's responsible for running nodes in correct order. |
| **[Pipeline Context](pipeline-context.md)** | An object that flows through pipeline, carrying shared state, cancellation tokens, and other contextual information. |
| **[Data Pipes](data-pipes.md)** | The fundamental mechanism for transferring items between nodes, providing type-safe asynchronous streaming. |

---

## 2. Building Pipelines

Learn how to define and construct your data pipelines.

| Concept | Description |
| :--- | :--- |
| **[Defining Pipelines](defining-pipelines.md)** | Both of fluent `PipelineBuilder` API and class-based `IPipelineDefinition` approaches for constructing pipelines. |
| **[Node Definition Structure](node-definition.md)** | Understanding of nested configuration structure of NodeDefinition for advanced customization. |
| **[Nodes](nodes/index.md)** | The fundamental unit of work in a pipeline. Nodes can be sources, transforms, or sinks. |

---

## 3. Pipeline Execution

Understand how pipelines run and how to control execution behavior.

| Concept | Description |
| :--- | :--- |
| **[Pipeline Execution](pipeline-execution/index.md)** | Overview of how pipelines are executed and controlled. |
| **[Execution Strategies](pipeline-execution/execution-strategies.md)** | Control how nodes process data (sequential, parallel, batched). |
| **[Streaming vs Buffering](streaming-vs-buffering.md)** | Understand memory tradeoffs and choose right approach for your use case. |
| **[Thread Safety](thread-safety.md)** | Design safe concurrent pipelines and understand NPipeline's threading model. |
| **[Pipeline Validation](pipeline-validation.md)** | Validate pipelines before execution to catch errors early. |

---

## 4. Architecture Decisions

Critical design choices that affect correctness and performance.

> **⚠️ Important**: Read this section carefully before implementing any grouping logic. Choosing wrong approach can lead to subtle data corruption bugs or unnecessary complexity.

| Concept | Description |
| :--- | :--- |
| **[Grouping Strategies](grouping-strategies.md)** | Choose between batching (operational efficiency) and aggregation (data correctness with late/out-of-order events). |

---

## 5. Resilience

Comprehensive framework for building fault-tolerant pipelines that can recover from failures.

| Concept | Description |
| :--- | :--- |
| **[Resilience Overview](resilience/index.md)** | Introduction to NPipeline's resilience features. |
| **[Error Handling](resilience/error-handling.md)** | Building fault-tolerant pipelines with error handlers and retries. |
| **[Circuit Breakers](resilience/circuit-breakers.md)** | Preventing cascading failures with circuit breaker patterns. |
| **[Dead Letter Queues](resilience/dead-letter-queues.md)** | Handling failed items with dead letter sinks. |

---

## 6. Implementation Guidance

Practical patterns and best practices for building production-ready pipelines.

| Concept | Description |
| :--- | :--- |
| **[Common Patterns](common-patterns.md)** | Practical recipes for solving real-world scenarios (ETL, validation, branching, batching). |
| **[Best Practices](best-practices.md)** | Design principles and recommendations for robust, maintainable pipelines. |

---

## How They Fit Together

1. You use to **`PipelineBuilder`** to define the structure of your pipeline by adding sources, transforms, and sinks.
2. The `Build()` method on `PipelineBuilder` creates an **`IPipelineDefinition`**.
3. The `IPipelineDefinition` is then used to create an **`IPipeline`** instance.
4. When you run the pipeline, data flows from `ISourceNode`s, through `ITransformNode`s, to `ISinkNode`s.
5. The entire process is managed by the pipeline, and **`PipelineContext`** is available to all nodes.

This modular design allows you to create complex data processing workflows from simple, reusable components.

---

## Performance Considerations

When building production-grade pipelines with NPipeline, it's important to consider performance implications to ensure optimal resource utilization and throughput.

### Memory Management

- **Use streaming for large datasets**: NPipeline is designed to work efficiently with streaming data. Avoid loading entire datasets into memory when possible, especially with source nodes that can process data incrementally.
- **Consider batch processing for high-volume scenarios**: For high-throughput scenarios, implement batching in your transform nodes to balance memory usage with processing efficiency.
- **Monitor memory usage with appropriate metrics**: Use observability tools to track memory consumption patterns in your pipelines, especially when processing variable-sized data.

### Parallelism

- **Leverage NPipeline.Extensions.Parallelism for CPU-bound operations**: For computationally intensive transformations, use parallelism extensions to distribute work across multiple CPU cores.
- **Configure appropriate degree of parallelism**: Balance parallelism with available system resources. Too much parallelism can lead to resource contention and diminished returns.
- **Consider resource contention when designing pipelines**: Be mindful of shared resources like database connections or file handles when implementing parallel processing.

### Throughput Optimization

- **Minimize allocations in hot paths**: Reduce object allocations in frequently executed code paths to lower garbage collection pressure.
- **Use appropriate buffer sizes**: Configure buffer sizes based on your specific use case to balance latency and throughput.
- **Profile your pipeline**: Use profiling tools to identify bottlenecks and optimize critical path components.

---

## Next Steps

- **[Defining Pipelines](./defining-pipelines.md)**: Explore both of fluent API and class-based approaches for defining pipelines
- **[Pipeline](./ipipeline.md)**: Learn about the executable instance of a pipeline
- **[Nodes](./nodes/index.md)**: Understand the fundamental unit of work in a pipeline
- **[Pipeline Context](./pipeline-context.md)**: Discover the object that carries shared state through the pipeline
- **[Grouping Strategies](./grouping-strategies.md)**: Understand when to use batching vs. aggregation (critical architecture decision)

---

For deeper exploration of design decisions and internal architecture, see [Architecture Overview](../architecture/index.md).

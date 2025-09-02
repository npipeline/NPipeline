---
title: Core Concepts
description: Understand the fundamental building blocks of NPipeline.
sidebar_position: 1
slug: /core-concepts
---

# Core Concepts

NPipeline is designed with a few key concepts at its heart. Understanding these will help you build powerful and efficient data pipelines. This section provides a high-level overview of the main components.

## 🚀 Critical Architecture Decision: Grouping Strategies

**Before implementing any grouping logic, read this section carefully.** Choosing the wrong approach can lead to subtle data corruption bugs or unnecessary complexity.

**[→ Read Grouping Strategies: Batching vs. Aggregation](./grouping-strategies.md)**

This document explains:

* When to use **batching** (operational efficiency)
* When to use **aggregation** (data correctness with late/out-of-order events)
* Decision framework to choose the right approach
* Consequences of choosing wrong
* Real-world examples

**TL;DR:**

* **Batching:** External system needs multiple items at once (DB bulk inserts)
* **Aggregation:** Events arrive late/out-of-order and time windows matter (analytics)

---

## The Building Blocks

| Concept | Description |
| :--- | :--- |
| **[IPipeline](ipipeline.md)** | The executable instance of a pipeline. It's responsible for running nodes in the correct order. |
| **[INode](nodes/index.md)** | The fundamental unit of work in a pipeline. Nodes can be sources, transforms, or sinks. |
| **[IPipelineDefinition](pipeline-definition.md)** | A blueprint of a pipeline. It defines the nodes and their connections. |
| **[PipelineBuilder](pipelinebuilder.md)** | A fluent API for creating an `IPipelineDefinition`. |
| **[PipelineContext](pipeline-context.md)** | An object that flows through the pipeline, carrying shared state, cancellation tokens, and other contextual information. |
| **[Resilience](resilience/index.md)** | Comprehensive framework for building fault-tolerant pipelines that can recover from failures. |

---

## Understanding the Core Concepts

NPipeline is designed with a few key concepts at its heart. Understanding these will help you build powerful and efficient data pipelines.

## How They Fit Together

1. You use the **`PipelineBuilder`** to define the structure of your pipeline by adding sources, transforms, and sinks.
2. The `Build()` method on the `PipelineBuilder` creates an **`IPipelineDefinition`**.
3. The `IPipelineDefinition` is then used to create an **`IPipeline`** instance.
4. When you run the pipeline, data flows from `ISourceNode`s, through `ITransformNode`s, to `ISinkNode`s.
5. The entire process is managed by the pipeline, and the **`PipelineContext`** is available to all nodes.

This modular design allows you to create complex data processing workflows from simple, reusable components.

## Performance Considerations

When building production-grade pipelines with NPipeline, it's important to consider performance implications to ensure optimal resource utilization and throughput.

### Memory Management

* **Use streaming for large datasets**: NPipeline is designed to work efficiently with streaming data. Avoid loading entire datasets into memory when possible, especially with source nodes that can process data incrementally.
* **Consider batch processing for high-volume scenarios**: For high-throughput scenarios, implement batching in your transform nodes to balance memory usage with processing efficiency.
* **Monitor memory usage with appropriate metrics**: Use observability tools to track memory consumption patterns in your pipelines, especially when processing variable-sized data.

### Parallelism

* **Leverage NPipeline.Extensions.Parallelism for CPU-bound operations**: For computationally intensive transformations, use the parallelism extensions to distribute work across multiple CPU cores.
* **Configure appropriate degree of parallelism**: Balance parallelism with available system resources. Too much parallelism can lead to resource contention and diminished returns.
* **Consider resource contention when designing pipelines**: Be mindful of shared resources like database connections or file handles when implementing parallel processing.

### Throughput Optimization

* **Minimize allocations in hot paths**: Reduce object allocations in frequently executed code paths to lower garbage collection pressure.
* **Use appropriate buffer sizes**: Configure buffer sizes based on your specific use case to balance latency and throughput.
* **Profile your pipeline**: Use profiling tools to identify bottlenecks and optimize critical path components.

## Next Steps

* **[Grouping Strategies](./grouping-strategies.md)**: Understand when to use batching vs. aggregation (critical architecture decision)
* **[IPipeline](ipipeline.md)**: Learn about the executable instance of a pipeline
* **[INode](nodes/index.md)**: Understand the fundamental unit of work in a pipeline
* **[IPipelineDefinition](pipeline-definition.md)**: Explore the blueprint of a pipeline that defines the nodes and their connections
* **[PipelineBuilder](pipelinebuilder.md)**: Explore the fluent API for creating pipeline definitions
* **[PipelineContext](pipeline-context.md)**: Discover the object that carries shared state through the pipeline

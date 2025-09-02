---
title: What is NPipeline?
description: Discover NPipeline, a high-performance, graph-based streaming data pipeline library for .NET.
sidebar_position: 1
slug: /introduction
---

## What is NPipeline?

NPipeline is a powerful and flexible .NET library designed for building high-performance, graph-based streaming data pipelines. It empowers developers to construct complex data processing workflows with ease, focusing on efficiency, testability, and maintainability.

At its core, NPipeline allows you to define a series of interconnected operations (nodes) that process data as it flows through the system. This graph-based approach provides clear visibility into your data's journey, making it simple to design, debug, and optimize your pipelines.

## Key Features

* **Graph-Based Architecture:** Visually intuitive and easy to understand data flow.
* **High Performance:** Optimized for low memory allocations and efficient data processing.
* **Extensible:** Easily integrate custom logic and connect to various data sources and destinations.
* **Testable:** Designed with testability in mind, enabling robust and reliable pipelines.
* **Modern .NET:** Leverages the latest C# features and asynchronous programming patterns.

## How it Works

NPipeline pipelines are constructed using a builder pattern, where you define nodes that act as sources, transforms, or sinks.

* **Sources:** Initiate the data flow by producing items.
* **Transforms:** Process and transform data items as they pass through the pipeline.
* **Sinks:** Consume data items, typically writing them to a destination or performing a final action.

These nodes are connected to form a directed acyclic graph (DAG), ensuring a clear and predictable data path.

## Benefits for Developers

* **Accelerated Development:** Rapidly build and deploy data pipelines with a clear, concise API.
* **Improved Maintainability:** Graph-based structure and modular components make pipelines easier to understand and modify.
* **Enhanced Reliability:** Robust error handling and testing mechanisms ensure data integrity.
* **Optimal Performance:** Achieve high throughput and low latency for demanding data workloads.

## Next Steps

* **[Why NPipeline?](./why-npipeline.md)**: Understand the benefits, design philosophy, and zero-allocation fast paths
* **[Installation](../getting-started/installation.md)**: Get NPipeline set up in your development environment
* **[Quick Start](../getting-started/quick-start.md)**: Begin building your first pipeline
* **[Core Concepts](../core-concepts/index.md)**: Dive deeper into the fundamental building blocks of NPipeline

---

## Learning Path Recommendation

New to NPipeline? Follow this path:

1. **[Why NPipeline?](./why-npipeline.md)** - Understand the value proposition and design philosophy
2. **[Quick Start](../getting-started/quick-start.md)** - Build your first working pipeline (15 minutes)
3. **[Core Concepts](../core-concepts/index.md)** - Understand the building blocks (sources, transforms, sinks)
4. **[Grouping Strategies](../core-concepts/grouping-strategies.md)** - Learn the critical choice between batching and aggregation
5. **[Core Resilience Concepts](../core-concepts/resilience/index.md)** - Build fault-tolerant pipelines
6. **[Optimization Principles](../architecture/optimization-principles.md)** - Understand how NPipeline achieves high performance
7. **[Performance Hygiene](../advanced-topics/performance-hygiene.md)** - Apply optimization patterns to your code

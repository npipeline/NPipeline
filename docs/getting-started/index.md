---
title: Getting Started
description: Install NPipeline and build your first pipeline.
sidebar_position: 1
slug: /getting-started
---

Welcome to NPipeline! This section will get you up and running with your first data pipeline in minutes.

## What is NPipeline?

NPipeline is a powerful and flexible .NET library designed for building high-performance, graph-based streaming data pipelines. It empowers developers to construct complex data processing workflows with ease, focusing on efficiency, testability, and maintainability.

At its core, NPipeline allows you to define a series of interconnected operations (nodes) that process data as it flows through the system. This graph-based approach provides clear visibility into your data's journey, making it simple to design, debug, and optimize your pipelines.

### Key Features

* **Graph-Based Architecture:** Visually intuitive and easy to understand data flow.
* **High Performance:** Optimized for low memory allocations and efficient data processing.
* **Extensible:** Easily integrate custom logic and connect to various data sources and destinations.
* **Testable:** Designed with testability in mind, enabling robust and reliable pipelines.
* **Modern .NET:** Leverages the latest C# features and asynchronous programming patterns.

### How it Works

NPipeline pipelines are constructed using a builder pattern, where you define nodes that act as sources, transforms, or sinks.

* **Sources:** Initiate the data flow by producing items.
* **Transforms:** Process and transform data items as they pass through the pipeline.
* **Sinks:** Consume data items, typically writing them to a destination or performing a final action.

These nodes are connected to form a directed acyclic graph (DAG), ensuring a clear and predictable data path.

### Benefits for Developers

* **Accelerated Development:** Rapidly build and deploy data pipelines with a clear, concise API.
* **Improved Maintainability:** Graph-based structure and modular components make pipelines easier to understand and modify.
* **Enhanced Reliability:** Robust error handling and testing mechanisms ensure data integrity.
* **Optimal Performance:** Achieve high throughput and low latency for demanding data workloads.

---

## Next Steps

1. **[Why NPipeline?](./why-npipeline.md)** - Understand the benefits, design philosophy, and zero-allocation fast paths
2. **[Installation](./installation.md)** - Get NPipeline set up in your development environment
3. **[Quick Start](./quick-start.md)** - Build your first working pipeline (15 minutes)
4. **[Core Concepts](../core-concepts/index.md)** - Understand the building blocks (sources, transforms, sinks)
5. **[Grouping Strategies](../core-concepts/grouping-strategies.md)** - Learn the critical choice between batching and aggregation
6. **[Core Resilience Concepts](../core-concepts/resilience/index.md)** - Build fault-tolerant pipelines
7. **[Optimization Principles](../architecture/optimization-principles.md)** - Understand how NPipeline achieves high performance
8. **[Performance Hygiene](../advanced-topics/performance-hygiene.md)** - Apply optimization patterns to your code

---

## Learning Path Recommendation

New to NPipeline? Follow this path:

1. **Installation** - Get the packages
2. **Quick Start** - Build your first pipeline in 15 minutes
3. **Why NPipeline?** - Understand the philosophy and benefits
4. **Core Concepts** - Learn how the building blocks fit together
5. **Grouping Strategies** - Make critical architecture decisions
6. **Resilience** - Build fault-tolerant systems
7. **Performance** - Optimize for your workload

---

## Quick Navigation

* **[FAQ](../reference/faq.md)** - Common questions and answers
* **[Troubleshooting](../reference/troubleshooting.md)** - Diagnose common issues
* **[Issue Tracker](https://github.com/NPipeline/NPipeline/issues)** - Report bugs or request features

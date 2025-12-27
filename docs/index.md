---
title: Welcome to NPipeline
description: Explore the official documentation for NPipeline, a high-performance, graph-based streaming data pipeline library for .NET.
slug: /
sidebar_position: 0
---

# Welcome to NPipeline Documentation

NPipeline is a powerful and flexible .NET library for building high-performance, graph-based streaming data pipelines. It empowers developers to construct complex data processing workflows with clear, intuitive graph-based architecture—perfect for low-latency, high-throughput applications that demand both reliability and performance.

**Key Benefits:**
- **Graph-Based Design** - Visually intuitive data flow that's easy to understand and debug
- **High Performance** - Optimized for minimal memory allocations and maximum throughput
- **Production-Ready** - Robust error handling, resilience patterns, and extensibility built-in
- **Modern .NET** - Leverages async/await and latest C# features for clean, maintainable code

## Choose Your Learning Path

| Experience Level | Recommended Path | Time Required |
|------------------|------------------|---------------|
| **Just starting** | Quick Start (below) → Beginner Path (below) | 15 min + 2-3 hours |
| **Building production pipelines** | Intermediate Path (below) | 4-6 hours |
| **Optimizing performance** | Expert Path (below) | 8-12 hours |

---

## Quick Start (15 minutes)

New to NPipeline? Get started in three simple steps:

- [Installation](getting-started/installation.md) - Set up NPipeline (2 minutes)
- [Your First Pipeline](getting-started/quick-start.md) - Build a simple data pipeline (5 minutes)
- [Why NPipeline?](getting-started/why-npipeline.md) - Understand the benefits and philosophy (8 minutes)

---

## Beginner Path (2-3 hours)

<details>
<summary><strong>Perfect for developers new to data pipelines. Click to expand learning modules.</strong></summary>

---

### Module 1: Core Concepts (45 minutes)

- [Defining Pipelines](core-concepts/defining-pipelines.md) - Understanding the pipeline builder pattern and class-based definitions
- [Source Nodes](core-concepts/nodes/source-nodes.md) - Getting data into your pipeline
- [Transform Nodes](core-concepts/nodes/transform-nodes.md) - Processing and transforming data
- [Sink Nodes](core-concepts/nodes/sink-nodes.md) - Outputting processed data

### Module 2: Error Handling (30 minutes)

- [Error Handling Basics](core-concepts/resilience/error-handling.md) - Understanding error handling in NPipeline
- [Basic Retry Patterns](core-concepts/resilience/retries.md) - Implementing retry logic

### Module 3: Testing (30 minutes)

- [Unit Testing Pipelines](extensions/testing/index.md) - Testing your pipeline components
- [Debugging Pipelines](reference/troubleshooting.md) - Common issues and solutions

### Module 4: Thread Safety (20 minutes)

- [Thread Safety Guide](core-concepts/thread-safety.md) - Understanding concurrency and shared state management

</details>

---

## Intermediate Path (4-6 hours)

<details>
<summary><strong>For developers comfortable with basics who want to build robust pipelines. Click to expand learning modules.</strong></summary>

---

### Module 1: Node Types (60 minutes)

- [Node Types](core-concepts/nodes/index.md) - Beyond basic transforms
- [Batching Nodes](core-concepts/nodes/batching.md) - Processing data in batches
- [Aggregation Nodes](core-concepts/nodes/aggregation.md) - Data aggregation patterns
- [Join Nodes](core-concepts/nodes/join.md) - Combining multiple data streams

### Module 2: Resilience Patterns (90 minutes)

- [Getting Started with Resilience](core-concepts/resilience/getting-started.md) - Quick start for resilience features
- [Error Handling](core-concepts/resilience/error-handling.md) - Comprehensive error handling and recovery strategies
- [Materialization & Buffering](core-concepts/resilience/materialization.md) - Understanding replay functionality
- [Circuit Breakers](core-concepts/resilience/circuit-breakers.md) - Preventing cascading failures

### Module 3: Performance Optimization (60 minutes)

- [Execution Strategies](core-concepts/pipeline-execution/execution-strategies.md) - Choosing the right execution strategy
- [Performance Hygiene](advanced-topics/performance-hygiene.md) - Best practices for efficient pipelines
- [Synchronous Fast Paths](advanced-topics/synchronous-fast-paths.md) - Optimizing for high throughput
- [Thread Safety in Parallel Execution](core-concepts/thread-safety.md) - Handling concurrency correctly

### Module 4: Real-World Patterns (60 minutes)

- [Retries & Delays](core-concepts/resilience/retries.md) - Configuring retry strategies
- [Troubleshooting](core-concepts/resilience/troubleshooting.md) - Diagnosing common issues

</details>

---

## Expert Path (8-12 hours)

<details>
<summary><strong>For experienced developers who want to master NPipeline. Click to expand learning modules.</strong></summary>

---

### Module 1: Advanced Architecture (120 minutes)

- [Performance Optimization](advanced-topics/performance-hygiene.md) - Advanced optimization techniques
- [Dynamic Pipelines](core-concepts/nodes/index.md) - Working with complex node configurations
- [Custom Extensions](extensions/index.md) - Building custom extensions

### Module 2: Production Readiness (120 minutes)

- [Connector Patterns](connectors/index.md) - Integrating with external systems
- [Error Handling Architecture](architecture/error-handling-architecture.md) - Deep dive into error handling
- [Security Considerations](architecture/index.md) - Securing your pipelines

### Module 3: Advanced Integration (120 minutes)

- [Dependency Injection](architecture/dependency-injection.md) - Advanced Dependency Injection (DI) patterns
- [Extension Points](architecture/extension-points.md) - Creating custom extensions
- [Performance Characteristics](architecture/performance-characteristics.md) - Understanding performance

### Module 4: Mastery Project (120 minutes)

- [Performance Benchmarking](advanced-topics/performance-hygiene.md) - Measuring and optimizing performance
- [Testing Strategies](extensions/testing/advanced-testing.md) - Advanced testing techniques

</details>

---

## Or Browse by Topic

### [Getting Started](./getting-started/installation.md)

Install NPipeline and build your first pipeline in 15 minutes.

### [Why NPipeline?](./getting-started/why-npipeline.md)

Understand the "why" behind NPipeline and what makes it powerful.

### [Core Concepts](./core-concepts/index.md)

**Start here for implementation.** Learn the building blocks: nodes, pipelines,
execution, and resilience.

### [Architecture](./architecture/index.md)

**For the curious.** Deep dive into how NPipeline works internally and why it's fast.

### [Connectors](./connectors/index.md)

Connect to external systems: CSV files, databases, message queues.

### [Extensions](./extensions/index.md)

Dependency injection, parallelism, testing utilities, and more.

### [Advanced Topics](./advanced-topics/performance-hygiene.md)

Performance optimization, testing strategies, and expert patterns.

### [Build-Time Analyzers](./analyzers/index.md)

Catch errors at compile-time with automated best practice enforcement.

### [Reference](./reference/error-codes.md)

Error codes, API reference, FAQ, and troubleshooting.

---

## Tips for Effective Learning

1. **Follow the order** - Each path builds on concepts from previous modules
2. **Practice as you learn** - Try examples in your own environment
3. **Experiment** - Modify examples to understand how changes affect behavior
4. **Join the community** - Ask questions and share your experiences
5. **Build something real** - Apply your learning to a practical project

## Need Help?

- [FAQ](reference/faq.md) - Common questions and answers
- [Issue Tracker](https://github.com/NPipeline/NPipeline/issues) - Report bugs or request features

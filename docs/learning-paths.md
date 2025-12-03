---
title: Learning Paths
description: Progressive learning guides to help you master NPipeline from beginner to expert.
sidebar_position: 1
---

# Learning Paths

Welcome to NPipeline's progressive learning guides! These paths are designed to take you from basic concepts to advanced mastery through structured, hands-on learning experiences.

## Quick Start (15 minutes)

New to NPipeline? Get started with our quick introduction:

- [Introduction to NPipeline](introduction/index.md) - What is NPipeline and why use it?
- [Your First Pipeline](getting-started/quick-start.md) - Build a simple data pipeline in 5 minutes
- [Basic Concepts](core-concepts/pipelinebuilder.md) - Understand the core building blocks

## Beginner Path (2-3 hours)

Perfect for developers new to data pipelines. Learn the fundamentals:

### Module 1: Core Concepts (45 minutes)
- [Pipeline Builder](core-concepts/pipelinebuilder.md) - Understanding the pipeline builder pattern
- [Source Nodes](core-concepts/nodes/source-nodes.md) - Getting data into your pipeline
- [Transform Nodes](core-concepts/nodes/transform-nodes.md) - Processing and transforming data
- [Sink Nodes](core-concepts/nodes/sink-nodes.md) - Outputting processed data

### Module 2: Error Handling (30 minutes)

- [Error Handling Basics](core-concepts/resilience/error-handling-guide.md) - Understanding error handling in NPipeline
- [Basic Retry Patterns](core-concepts/resilience/retry-configuration.md) - Implementing retry logic

### Module 3: Testing (30 minutes)

- [Unit Testing Pipelines](advanced-topics/testing-pipelines.md) - Testing your pipeline components
- [Debugging Pipelines](introduction/troubleshooting.md) - Common issues and solutions

### Module 4: Thread Safety (20 minutes)

- [Thread Safety Guide](core-concepts/thread-safety.md) - Understanding concurrency and shared state management

## Intermediate Path (4-6 hours)

For developers comfortable with basics who want to build robust pipelines:

### Module 1: Node Types (60 minutes)

- [Node Types](core-concepts/nodes/index.md) - Beyond basic transforms
- [Batching Nodes](core-concepts/nodes/batching.md) - Processing data in batches
- [Aggregation Nodes](core-concepts/nodes/aggregation.md) - Data aggregation patterns
- [Join Nodes](core-concepts/nodes/join.md) - Combining multiple data streams

### Module 2: Resilience Patterns (90 minutes)

- [Execution with Resilience](core-concepts/resilience/execution-with-resilience.md) - Building fault-tolerant pipelines
- [Materialization and Buffering](core-concepts/resilience/materialization-and-buffering.md) - Understanding replay functionality
- [Circuit Breaker Configuration](core-concepts/resilience/circuit-breaker-configuration.md) - Preventing cascading failures
- [Dependency Chains](core-concepts/resilience/dependency-chains.md) - Critical prerequisite relationships

### Module 3: Performance Optimization (60 minutes)

- [Execution Strategies](core-concepts/pipeline-execution/execution-strategies.md) - Choosing the right execution strategy
- [Performance Hygiene](advanced-topics/performance-hygiene.md) - Best practices for efficient pipelines
- [Synchronous Fast Paths](advanced-topics/synchronous-fast-paths.md) - Optimizing for high throughput
- [Thread Safety in Parallel Execution](core-concepts/thread-safety.md) - Handling concurrency correctly

### Module 4: Real-World Patterns (60 minutes)

- [Configuration Guide](core-concepts/resilience/configuration-guide.md) - Step-by-step configuration
- [Troubleshooting](core-concepts/resilience/troubleshooting.md) - Diagnosing common issues

## Expert Path (8-12 hours)

For experienced developers who want to master NPipeline:

### Module 1: Advanced Architecture (120 minutes)

- [Performance Optimization](advanced-topics/performance-hygiene.md) - Advanced optimization techniques
- [Dynamic Pipelines](core-concepts/nodes/index.md) - Working with complex node configurations
- [Custom Extensions](extensions/index.md) - Building custom extensions

### Module 2: Production Readiness (120 minutes)

- [Connector Patterns](connectors/index.md) - Integrating with external systems
- [Error Handling Architecture](architecture/error-handling-architecture.md) - Deep dive into error handling
- [Security Considerations](introduction/index.md) - Securing your pipelines

### Module 3: Advanced Integration (120 minutes)

- [Dependency Injection](architecture/dependency-injection.md) - Advanced DI patterns
- [Extension Points](architecture/extension-points.md) - Creating custom extensions
- [Performance Characteristics](architecture/performance-characteristics.md) - Understanding performance

### Module 4: Mastery Project (120 minutes)

- [Performance Benchmarking](advanced-topics/performance-hygiene.md) - Measuring and optimizing performance
- [Testing Strategies](advanced-topics/testing-pipelines.md) - Advanced testing techniques

---

## Tips for Effective Learning

1. **Follow the order** - Each path builds on concepts from previous modules
2. **Practice as you learn** - Try examples in your own environment
3. **Experiment** - Modify examples to understand how changes affect behavior
4. **Join the community** - Ask questions and share your experiences
5. **Build something real** - Apply your learning to a practical project

## Need Help?

- [FAQ](introduction/faq.md) - Common questions and answers
- [Issue Tracker](https://github.com/NPipeline/NPipeline/issues) - Report bugs or request features

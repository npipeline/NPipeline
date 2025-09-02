---
title: Sample Applications
description: Real-world examples demonstrating NPipeline concepts and patterns for different skill levels
sidebar_position: 8
---

# Sample Applications

This guide provides practical examples of NPipeline implementations, organized by skill level and complexity. Each sample demonstrates specific concepts and patterns you can apply to your own pipelines.

> **Location:** All samples are located in the `/samples/` directory of the repository. Each sample includes complete source code, configuration files, and a README with detailed explanations.

## Basic Samples

### Sample 01: Simple ETL Pipeline
**Concepts demonstrated:**
- Basic source, transform, and sink nodes
- Simple data flow between nodes
- Pipeline definition and execution

**What it does:** Extracts data from a CSV file, transforms it by applying simple calculations, and loads the results into another CSV file. Perfect for understanding the fundamental ETL pattern in NPipeline.

**Key takeaways:** How to structure a basic pipeline and connect nodes together.

---

### Sample 02: High-Performance Transform
**Concepts demonstrated:**
- ValueTask optimization for transforms
- Synchronous fast paths vs. asynchronous paths
- Performance optimization techniques

**What it does:** Implements a transform node that processes data with both fast synchronous paths (cache hits) and slower asynchronous paths (cache misses). Demonstrates how to minimize allocations in high-throughput scenarios.

**Key takeaways:** When and how to use ValueTask for performance-critical transforms.

---

### Sample 03: Fluent Configuration
**Concepts demonstrated:**
- PipelineBuilder fluent API
- Configuration options
- Dependency injection setup

**What it does:** Shows different ways to configure pipelines using the fluent API, including setting execution options, error handling policies, and observability features.

**Key takeaways:** How to leverage the fluent API for clean, readable pipeline configuration.

## Intermediate Samples

### Sample 04: CSV to Database Pipeline
**Concepts demonstrated:**
- Real-world data connectors
- Batch processing
- Transaction handling

**What it does:** Reads data from CSV files, transforms it, and writes to a SQL database with proper transaction handling and batch processing for performance.

**Key takeaways:** Working with external systems and handling data at scale.

---

### Sample 05: Error Handling & Resilience
**Concepts demonstrated:**
- Node-level error handling
- Retry policies
- Circuit breaker patterns
- Dead letter queues

**What it does:** Implements a pipeline with comprehensive error handling, including retries, circuit breakers, and dead letter queues for failed items. Shows how to build resilient production pipelines.

**Key takeaways:** Designing pipelines that gracefully handle failures and maintain data integrity.

---

### Sample 06: Parallel Processing
**Concepts demonstrated:**
- Parallel execution strategies
- Resource management
- Performance tuning

**What it does:** Demonstrates different parallel processing approaches in NPipeline, showing when to use each strategy and how to configure them for optimal performance.

**Key takeaways:** How to leverage parallelism while avoiding common pitfalls.

## Advanced Samples

### Sample 07: Time-Windowed Aggregation
**Concepts demonstrated:**
- Time-based windowing
- Watermarks and late data handling
- Aggregation patterns

**What it does:** Implements a time-windowed aggregation pipeline that processes events in time windows and handles late-arriving data correctly.

**Key takeaways:** Building stream processing applications with correct temporal semantics.

---

### Sample 08: ValueTask Optimization
**Concepts demonstrated:**
- Advanced ValueTask patterns
- Memory allocation optimization
- Performance measurement

**What it does:** Deep dive into ValueTask optimization with benchmarks and performance measurements. Shows how to achieve maximum throughput in memory-constrained scenarios.

**Key takeaways:** Advanced techniques for optimizing pipeline performance.

---

### Sample 09: Custom Node Types
**Concepts demonstrated:**
- Creating custom node implementations
- Extending NPipeline functionality
- Best practices for node design

**What it does:** Shows how to implement custom node types for specialized processing needs, including proper error handling, observability, and lifecycle management.

**Key takeaways:** Extending NPipeline to meet specific application requirements.

---

### Sample 10: Full Production Pipeline
**Concepts demonstrated:**
- Production-ready architecture
- Comprehensive monitoring
- Deployment considerations
- Performance optimization

**What it does:** A complete, production-ready pipeline that combines all advanced concepts: error handling, parallelism, custom nodes, monitoring, and performance optimization.

**Key takeaways:** How to architect and implement robust, scalable production pipelines.

## Getting Started with Samples

1. **Clone the repository** to access all sample code
2. **Start with Basic Samples** if you're new to NPipeline
3. **Progress to Intermediate Samples** once you're comfortable with the basics
4. **Explore Advanced Samples** for production scenarios and optimization techniques

Each sample includes:
- Complete source code with comments explaining key concepts
- Configuration files showing best practices
- README with detailed explanations and setup instructions
- Performance characteristics and tuning guidance

## Related Documentation

- [Quick Start](./getting-started/quick-start.md) - Get started with NPipeline
- [Core Concepts](./core-concepts/index.md) - Understand the building blocks
- [Learning Paths](./learning-paths.md) - Structured learning journey
- [Performance Optimization](./advanced-topics/synchronous-fast-paths.md) - Advanced performance techniques
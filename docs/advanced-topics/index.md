---
title: Advanced Topics
description: Expert-level guides for high-performance optimization, production deployment, and advanced implementation patterns.
sidebar_position: 1
---

# Advanced Topics

This section covers advanced techniques and best practices for building high-performance, production-grade data pipelines with NPipeline.

## Performance Optimization

### [Performance Hygiene](./performance-hygiene.md)

Learn best practices for building efficient, low-allocation data pipelines. Covers:
- Memory allocation minimization strategies
- Buffer reuse patterns
- Data structure optimization
- GC pressure reduction techniques

**Best for:** Developers aiming to optimize pipeline performance and reduce garbage collection overhead.

### [Synchronous Fast Paths and ValueTask Optimization](./synchronous-fast-paths.md)

Master the ValueTask pattern for eliminating allocations in high-throughput scenarios. Covers:
- Understanding ValueTask vs Task tradeoffs
- Implementing fast path/slow path patterns
- Real-world optimization examples
- Achieving up to 90% GC reduction in cache-hit scenarios

**Best for:** High-throughput pipelines where cache hits or synchronous operations are common.

## Related Topics

### Resilience & Retry Strategies

For advanced retry delay patterns and production resilience strategies:
- **[Advanced Retry Delay Strategies](../core-concepts/resilience/retry-delay-advanced.md)** - Dynamic configuration, adaptive strategies, performance optimization, and monitoring

### Production Deployment

For guidance on deploying NPipeline to production environments:
- **[Deployment & Operations](../architecture/performance-characteristics.md)** - Performance characteristics and scalability patterns
- **[Troubleshooting](../reference/troubleshooting.md)** - Common issues and solutions

### Testing

For advanced testing strategies:
- **[Advanced Testing Pipelines](../extensions/testing/advanced-testing.md)** - Mocking, dependencies, error handling, and complex scenarios

## Learning Path

If you're new to these topics, follow this recommended order:

1. **[Performance Hygiene](./performance-hygiene.md)** - Foundation for understanding performance considerations
2. **[Synchronous Fast Paths](./synchronous-fast-paths.md)** - Deep dive into specific optimization techniques
3. **[Advanced Retry Strategies](../core-concepts/resilience/retry-delays.md)** - Production resilience patterns
4. **[Advanced Testing](../extensions/testing/advanced-testing.md)** - Validating your optimized implementations

## Prerequisites

Before diving into these advanced topics, ensure you're familiar with:
- [Core Concepts](../core-concepts/index.md) - Pipeline fundamentals
- [Basic Resilience](../core-concepts/resilience/index.md) - Error handling and recovery
- [Basic Testing](../extensions/testing/index.md) - Testing pipelines

## Key Principles

### Measure Before Optimizing

Always profile your pipeline before optimizing. Use the built-in diagnostics and monitoring capabilities to identify actual bottlenecks rather than guessing.

### Optimize for Your Workload

Different workloads have different characteristics. A pipeline processing cached data has very different optimization needs than one processing streaming data from external sources.

### Balance Complexity and Benefit

Advanced optimizations often add complexity. Ensure the performance gain justifies the added code complexity and maintenance burden.

### Test Your Optimizations

Always validate that optimizations actually improve performance in your specific environment. Microbenchmarks don't always translate to real-world improvements.

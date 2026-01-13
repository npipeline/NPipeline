---
title: Sample Applications
description: Real-world examples demonstrating NPipeline concepts and patterns for different skill levels
sidebar_position: 1
---

# Sample Applications

This guide provides practical examples of NPipeline implementations, organized by skill level and complexity. Each sample demonstrates specific concepts and patterns you can apply to your own pipelines.

> **Location:** All samples are located in the `/samples/` directory of the repository. Each sample includes complete source code, configuration files, and a README with detailed explanations.

## 👋 Recommended Learning Path

**New to NPipeline?** Follow this path to master the framework:

1. **Start Here:** [Sample 01: Basic Pipeline](./basic.md#sample-01-basic-pipeline) (1-2 hours)
   - Learn the fundamentals: sources, transforms, sinks, and how data flows
   - This is your "Hello World" of NPipeline

2. **Build Your Foundation:** [Samples 2-5](./basic.md) (2-3 hours)
   - File processing, error handling, data transformation, and parallelism
   - Strengthen core concepts with hands-on examples

3. **Add Resilience:** [Sample 06: Advanced Error Handling](./intermediate.md#sample-06-advanced-error-handling) (1 hour)
   - Learn production-ready error recovery patterns
   - Understand retries and circuit breakers

4. **Choose Your Focus** (ongoing)
   - **Data Processing** → [Samples 7-9](./intermediate.md) - Enrichment, CSV, analytics
   - **Performance** → [Samples 11-12](./advanced.md) - Custom nodes, optimization
   - **Streaming** → [Samples 15-16](./advanced.md) - Windowing, time-based processing
   - **Complex Joins** → [Samples 17-20](./advanced.md) - Keyed joins, temporal logic
   - **Extensions** → [Extension Samples](./extensions.md) - Lineage, observability, composition, nodes

### Already know what you need?

Jump directly to samples by topic:

- [File Processing](./basic.md#sample-02-file-processing)
- [CSV Processing](./intermediate.md#sample-08-csv-connector)
- [Real-time Analytics](./intermediate.md#sample-09-aggregatenode)
- [Custom Node Implementation](./advanced.md)
- [Extension Samples](./extensions.md)

## What Each Sample Includes

Every sample comes with:

- **Complete source code** with comments explaining key concepts
- **Configuration files** showing best practices
- **README** with detailed explanations and setup instructions
- **Performance characteristics** and tuning guidance
- **Real-world scenario context** and business logic

## Sample Breakdown by Skill Level

### Basic Samples (1-5)

Perfect for learning NPipeline fundamentals:

- **Sample 01: Basic Pipeline** - Hello World of NPipeline
- **Sample 02: File Processing** - Stream processing files efficiently
- **Sample 03: Basic Error Handling** - Essential error handling patterns
- **Sample 04: Simple Data Transformation** - Validation, filtering, enrichment
- **Sample 05: Parallel Processing** - Leveraging multiple CPU cores

**→ [View all Basic Samples](./basic.md)**

### Intermediate Samples (6-10)

Building real-world features:

- **Sample 06: Advanced Error Handling** - Production resilience patterns
- **Sample 07: LookupNode** - Data enrichment from external sources
- **Sample 08: CSV Connector** - CSV processing pipelines
- **Sample 09: AggregateNode** - Real-time analytics with windowing
- **Sample 10: BranchNode** - Parallel data distribution

**→ [View all Intermediate Samples](./intermediate.md)**

### Advanced Samples (11-23)

Production-grade scenarios:

- **11-12:** Foundational concepts (custom nodes, performance optimization)
- **13-14:** Data processing patterns (batching, unbatching)
- **15-16:** Stream processing and windowing (time-based, session-based, dynamic)
- **17-20:** Complex joins and merging (keyed, temporal, custom merge)
- **21-23:** Event-time processing, monitoring, end-to-end scenarios

**→ [View all Advanced Samples](./advanced.md)**

### Extension Samples

Demonstrating NPipeline extension packages:

- **Lineage Extension** - Data lineage tracking and provenance
- **Nodes Extension** - Pre-built data processing nodes
- **Observability Extension** - Metrics collection and monitoring
- **Composition Extension** - Hierarchical pipeline composition

**→ [View all Extension Samples](./extensions.md)**

## Learning Paths

Choose the path that matches your goals:

### Minimum Path (1-2 hours)

Get started quickly with essentials:

- Sample 01: Basic Pipeline
- Sample 03: Basic Error Handling
- Sample 04: Simple Data Transformation

### Foundation Path (4-5 hours)

Build a solid understanding:

- All Basic Samples (1-5)
- Sample 06: Advanced Error Handling
- Sample 09: AggregateNode

### Comprehensive Path (8+ hours)

Master all patterns:

- All Basic Samples (1-5)
- All Intermediate Samples (6-10)
- All Advanced Samples (11-23)
- All Extension Samples

### Extension-Focused Path (3-4 hours)

Master extension packages:

- All Basic Samples (1-5)
- All Extension Samples
- Sample 23: Complex Data Transformations

## Related Documentation

- **[Core Concepts](../core-concepts/index.md)** - Learn fundamental NPipeline concepts
- **[Architecture](../architecture/index.md)** - Understand how NPipeline works internally
- **[Advanced Topics](../advanced-topics/index.md)** - Master performance optimization and production patterns
- **[Extensions](../extensions/index.md)** - Discover available extension packages

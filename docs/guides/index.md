---
title: Guides
description: Practical how-to guides for building and configuring NPipeline data pipelines.
order: 2
---

# Guides

These guides cover every major feature of NPipeline with practical code examples. Each guide is self-contained — read them in any order depending on what you need.

**New to NPipeline?** Start with [Defining Pipelines](defining-pipelines.md), then move to [Lambda Nodes](lambda-nodes.md) or [Custom Nodes](custom-nodes.md) depending on your preference.

## Defining and Running Pipelines

- [Defining Pipelines](defining-pipelines.md) — fluent builder API and class-based definitions
- [Lambda Nodes](lambda-nodes.md) — inline node definitions with delegates
- [Custom Nodes](custom-nodes.md) — implement source, transform, and sink nodes
- [Pipeline Context](pipeline-context.md) — runtime parameters, shared state, and framework services
- [Dependency Injection](dependency-injection.md) — integrate with Microsoft.Extensions.DependencyInjection
- [Pipeline Validation](pipeline-validation.md) — validate graph structure before execution

## Data Flow Patterns

- [Branching and Merging](branching-and-merging.md) — fan-out, taps, and fan-in
- [Batching and Windowing](batching-and-windowing.md) — group items by size, time, or intent
- [Joins and Lookups](joins-and-lookups.md) — keyed joins, time-windowed joins, and lookup enrichment
- [Aggregation](aggregation.md) — reduce streams with aggregate and advanced aggregate nodes
- [Pipeline Composition](pipeline-composition.md) — embed reusable sub-pipelines

## Execution and Performance

- [Parallel Execution](parallel-execution.md) — concurrent transforms with backpressure control
- [Streaming Large Datasets](streaming-large-datasets.md) — memory-efficient lazy data streams

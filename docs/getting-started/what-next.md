---
title: "What Next?"
description: "Find the right documentation for what you want to build with NPipeline."
order: 5
---

# What Next?

You've installed NPipeline, built your first pipeline, and understand the mental model. Where you go from here depends on what you're trying to accomplish.

## I Want To

### Read or write files (CSV, JSON, Parquet, Excel)

Install a connector package and use its pre-built source/sink nodes.

→ [Connectors](../connectors/index.md) — choose the right connector for your file format

### Connect to a database (PostgreSQL, SQL Server, MongoDB, etc.)

Database connectors provide source and sink nodes for reading/writing directly.

→ [Connectors](../connectors/index.md) — database connectors section

### Process messages from Kafka, RabbitMQ, or Azure Service Bus

Message queue connectors give you streaming sources that consume messages in real time.

→ [Connectors](../connectors/index.md) — messaging connectors section

### Handle errors without crashing the pipeline

NPipeline has built-in retry strategies, dead-letter queues, and circuit breakers.

→ [Error Handling](../error-handling/index.md)

### Run nodes in parallel for higher throughput

The parallelism extension lets you process items concurrently with configurable thread safety.

→ [Parallel Execution](../guides/parallel-execution.md)

### Split data into multiple paths (branching or conditional routing)

Use branch/tap for unconditional fan-out, and RouteNode for predicate-based routing.

→ [Branching and Merging](../guides/branching-and-merging.md)
→ [Routing with RouteNode](../guides/routing-with-route-node.md)

### Combine data from multiple sources (joins)

Join nodes merge data from two or more input streams based on keys or time windows.

→ [Joins and Lookups](../guides/joins-and-lookups.md)

### Group or aggregate data

Aggregate nodes and grouping strategies let you compute summaries over streams.

→ [Aggregation](../guides/aggregation.md)

### Process large files without running out of memory

NPipeline streams by default, but understanding the buffering trade-offs helps with very large datasets.

→ [Streaming Large Datasets](../guides/streaming-large-datasets.md)

### Use dependency injection

Integrate NPipeline with `Microsoft.Extensions.DependencyInjection` for automatic node resolution.

→ [Dependency Injection](../guides/dependency-injection.md)

### Test my pipelines

In-memory sources and sinks, plus a test harness, make pipeline testing straightforward.

→ [Testing Pipelines](../testing/testing-pipelines.md)

### Understand what's available as a package

See every extension, connector, and storage provider NPipeline offers.

→ [Extensions](../extensions/index.md)

### Optimize performance

Techniques for reducing allocations, caching execution plans, and choosing the right patterns.

→ [Performance Best Practices](../performance/best-practices.md)

## Next Steps

- [Defining Pipelines](../guides/defining-pipelines.md) — fluent builder vs class-based approaches
- [Lambda Nodes](../guides/lambda-nodes.md) — inline nodes without separate classes
- [Custom Nodes](../guides/custom-nodes.md) — writing your own source, transform, and sink nodes
- [Reference: Glossary](../reference/glossary.md) — definitions for all NPipeline terminology

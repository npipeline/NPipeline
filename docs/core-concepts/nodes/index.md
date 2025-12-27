---
title: Nodes (Source, Transform, Sink, and Specialized)
description: Explore the fundamental building blocks of any NPipeline – Source, Transform, Sink, and specialized nodes for complex data processing patterns.
sidebar_position: 4
---

# Nodes: Source, Transform, Sink, and Specialized

Nodes are the fundamental building blocks of NPipeline pipelines. Each node type serves a specific purpose in the data processing workflow, from generating data sources to consuming final results.

## Node Types

- **[Source Nodes](source-nodes.md)** - Generate or fetch data from external systems
- **[Transform Nodes](transform-nodes.md)** - Process data item by item using `ITransformNode<TIn, TOut>`
- **[Stream Transform Nodes](transform-nodes.md)** - Process entire data streams using `IStreamTransformNode<TIn, TOut>`
- **[Sink Nodes](sink-nodes.md)** - Consume and finalize data at the end of your pipeline

## Specialized Node Types

- **[Aggregation](aggregation.md)** - Combine multiple items into aggregated results
- **[Batching](batching.md)** - Group items into batches for efficient processing
- **[Branch](branch.md)** - Split data flows into multiple paths
- **[Join](join.md)** - Merge data from multiple input streams
- **[Time-Windowed Join](time-windowed-join.md)** - Join data with temporal constraints
- **[Lookup](lookup.md)** - Enrich data by querying external sources
- **[Tap](tap.md)** - Monitor data without modifying it

## Choosing the Right Node Type

Selecting the appropriate node type is crucial for building efficient and maintainable pipelines:

| **Use Case** | **Recommended Node Type** | **Key Benefit** |
|---|---|---|
| Simple data transformation | `TransformNode<TIn, TOut>` | One-to-one mapping with minimal overhead |
| Stream-based operations | `IStreamTransformNode<TIn, TOut>` | Batching, unbatching, windowing, or stream cardinality changes |
| Data enrichment | `TransformNode<TIn, TOut>` | Per-item lookups with async service calls |
| High-throughput scenarios | `ValueTaskTransform<TIn, TOut>` | Synchronous operations with zero allocation |
| Complex workflows | Combination of node types | Each node serves its specific purpose |

**Table 1: Node Type Selection Guide**

## Next Steps

1. **[Node Definition Structure](../node-definition.md)** - Understanding the nested configuration structure of NodeDefinition
2. **[Transform Nodes](transform-nodes.md)** - Learn implementation details and patterns
3. **[Source Nodes](source-nodes.md)** - Discover how to create data sources
4. **[Sink Nodes](sink-nodes.md)** - Understand data consumption patterns

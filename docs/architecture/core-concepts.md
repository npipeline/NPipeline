---
title: Core Concepts
description: Fundamental architectural concepts in NPipeline - graphs, nodes, and streaming data.
sidebar_position: 1
---

# Core Concepts

NPipeline's architecture is built on three fundamental concepts: graph-based data representation, distinct node types, and a streaming data model.

## Graph-Based Architecture

NPipeline represents pipelines as **directed acyclic graphs (DAGs)** where:

- **Nodes** are processing units (sources, transforms, sinks)
- **Edges** are connections between nodes representing data flow
- **Data flows** through the graph from sources to sinks

```text
Source Node
    ↓
Transform Node 1
    ↓
Transform Node 2
    ↓
Sink Node
```

This approach provides several advantages:

- **Clarity**: Visual representation of data flow
- **Flexibility**: Easy to add, remove, or reroute nodes
- **Type Safety**: Compile-time validation of connections
- **Composability**: Nodes can be tested and reused independently

## Node Architecture

From an architectural perspective, nodes are the fundamental processing units that form the vertices of the pipeline graph. NPipeline defines three primary node types:

- **Source Nodes**: Initiate data flow by producing data streams
- **Transform Nodes**: Process and transform data as it flows through the pipeline
- **Sink Nodes**: Consume data at the terminal point of the pipeline

These nodes implement a unified interface hierarchy that enables type-safe connections and consistent execution patterns. The architectural design emphasizes:

- **Separation of Concerns**: Each node type has a distinct responsibility
- **Type Safety**: Compile-time validation of node connections
- **Composability**: Nodes can be combined in various configurations
- **Testability**: Each node can be tested in isolation

For detailed information about node implementations, interfaces, and examples, see the **[Nodes documentation](../core-concepts/nodes/index.md)**.

## Streaming Data Model

NPipeline uses `IAsyncEnumerable<T>` for **lazy, streaming data flow**:

```csharp
public interface IDataPipe<T> : IAsyncEnumerable<T>
{
    // IDataPipe<T> implements IAsyncEnumerable<T> directly
    // Iterate using: await foreach (var item in dataPipe)
}
```

### Key Characteristics

**Lazy Evaluation**
- Data is only produced when consumed
- If pipeline is cancelled, source never reads unused data

**Memory Efficient**
- Only active items are in memory at any time
- No need to load entire datasets upfront

**Responsive**
- Processing begins immediately upon pipeline start
- Results are available as soon as items flow through

**Composable**
- Each transform creates a new data pipe
- Pipes can be layered and composed

### Example: How Streaming Works

```csharp
// Step 1: Source creates pipe (but doesn't read yet)
var sourcePipe = await sourceNode.Execute(context, cancellationToken);

// Step 2: Transform wraps pipe (but doesn't process yet)
var transformPipe = new TransformPipe(sourcePipe, transformNode);

// Step 3: Sink actually triggers execution
await foreach (var item in transformPipe.WithCancellation(cancellationToken))
{
    // Data is produced, transformed, and consumed
    // Only ONE item is in memory at a time
}
```

## Benefits of This Design

:heavy_check_mark: **Memory Efficient** - Only active items in memory
:heavy_check_mark: **Responsive** - Processing starts immediately
:heavy_check_mark: **Cancellable** - Can stop at any time
:heavy_check_mark: **Type Safe** - Compile-time validation of node connections
:heavy_check_mark: **Testable** - Each node can be tested in isolation

## Next Steps

* **[Component Architecture](component-architecture.md)** - Learn about the major system components
* **[Execution Flow](execution-flow.md)** - Understand how data flows through pipelines
* **[Nodes](../core-concepts/nodes/index.md)** - Explore detailed node implementations and examples

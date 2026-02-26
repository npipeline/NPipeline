---
title: Architectural Foundations
description: Fundamental architectural concepts in NPipeline - graphs, nodes, and streaming data.
sidebar_position: 1
---

# Architectural Foundations

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

### Interface Hierarchy

All nodes implement the `INode` interface, with specialized interfaces for each node type:

```csharp
// Base interface - marker interface for all pipeline nodes
public interface INode : IAsyncDisposable
{
}

// Source nodes produce output streams
public interface ISourceNode<out TOutput> : INode
{
    IDataPipe<TOutput> Initialize(PipelineContext context, CancellationToken cancellationToken);
}

// Transform nodes process individual items
public interface ITransformNode<in TInput, out TOutput> : INode
{
    Task<TOutput> ExecuteAsync(TInput item, PipelineContext context, CancellationToken cancellationToken);
}

// Sink nodes consume entire streams
public interface ISinkNode<in TInput> : INode
{
    Task ExecuteAsync(IDataPipe<TInput> input, PipelineContext context, CancellationToken cancellationToken);
}
```

**Type Safety Through Generics:**

- Input types (`TInput`) and output types (`TOutput`) are enforced at compile time
- The `PipelineBuilder` validates that connected nodes have compatible types
- Attempting to connect incompatible nodes results in a build error

**Data Flow Model:**

- Source nodes return `IDataPipe<TOutput>`, which implements `IAsyncEnumerable<TOutput>`
- Transform nodes process items one at a time via `ExecuteAsync`
- Sink nodes consume the entire input stream using `await foreach`

The architectural design emphasizes:

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
// Step 1: Source creates pipe (synchronous - no await needed)
var sourcePipe = sourceNode.Initialize(context, cancellationToken);

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

✓ **Memory Efficient** - Only active items in memory
✓ **Responsive** - Processing starts immediately
✓ **Cancellable** - Can stop at any time
✓ **Type Safe** - Compile-time validation of node connections
✓ **Testable** - Each node can be tested in isolation

## Next Steps

- **[Component Architecture](component-architecture.md)** - Learn about the major system components
- **[Execution Flow](execution-flow.md)** - Understand how data flows through pipelines
- **[Nodes](../core-concepts/nodes/index.md)** - Explore detailed node implementations and examples

---
title: "Data Flow Internals"
description: "How IDataStream, async enumeration, and stream implementations connect nodes."
order: 5
---

# Data Flow Internals

Data moves between nodes through `IDataStream<T>`, NPipeline's abstraction over `IAsyncEnumerable<T>`. This page covers the stream interface hierarchy, concrete implementations, and how streams connect the execution model to node logic.

## Interface Hierarchy

```
IDataStream (non-generic marker)
├── StreamName: string
├── GetDataType(): Type
└── ToAsyncEnumerable(ct): IAsyncEnumerable<object?>

IDataStream<T> : IDataStream, IAsyncEnumerable<T>
    (inherits both; consumed via await foreach)

IForwardOnlyDataStream : IDataStream
    (marker for streams that cannot be replayed)

IForwardOnlyDataStream<T> : IDataStream<T>, IForwardOnlyDataStream
    (typed forward-only stream)
```

The non-generic `IDataStream` exists for framework internals that need to handle streams without knowing `T` at compile time (e.g., graph-level orchestration). User code always works with `IDataStream<T>`.

`GetDataType()` returns the actual runtime item type of the stream — `typeof(T)` for `IDataStream<T>`. This is used by `PipeMergeService` to select the correct typed merge delegate at runtime, and by `NodeExecutor` to validate inputs against the node's `RuntimeNodeStreamContract`. When item-level lineage is enabled, the runtime item type is `LineagePacket<T>`, not `T`; `GetDataType()` correctly reflects this because the stream's actual generic argument is `LineagePacket<T>`.

`IForwardOnlyDataStream` is the marker that tells the `ResilientExecutionStrategy` whether materialization is needed. If the input is forward-only and restart is enabled, the strategy wraps it in `CappedReplayableDataStream<T>`.

## Concrete Implementations

### DataStream\<T>

The simplest streaming implementation. Wraps an `IAsyncEnumerable<T>` with a name:

```csharp
public class DataStream<T>(IAsyncEnumerable<T> source, string name) : IForwardOnlyDataStream<T>
```

Used by source nodes to return their data. Forward-only — once consumed, items are gone.

### InMemoryDataStream\<T>

Buffered collection that supports repeated enumeration:

```csharp
public class InMemoryDataStream<T>(IReadOnlyList<T> items, string name) : IDataStream<T>
```

Not forward-only — can be enumerated multiple times without materialization. Used for testing and for small datasets that fit in memory.

### CappedReplayableDataStream\<T>

Bounded replay buffer created by `ResilientExecutionStrategy` for materialization:

```csharp
internal class CappedReplayableDataStream<T>(
    IDataStream<T> source, int? cap, string name) : IDataStream<T>, IAsyncDisposable
```

Buffers items from the source as they're consumed. On re-enumeration, replays from the buffer. If the cap is exceeded, throws to prevent unbounded memory growth.

### CountingPassthroughDataStream\<T>

Transparent wrapper that counts items flowing through for metrics:

```csharp
internal class CountingPassthroughDataStream<T> : IForwardOnlyDataStream<T>
```

Used by the observability system to track item counts without affecting data flow.

### MulticastDataStream\<T>

Distributes items to multiple consumers (branch/tap patterns):

```csharp
internal class AsyncEnumerableDataStream<T>(
    IAsyncEnumerable<T> source, string name) : IForwardOnlyDataStream<T>
```

When a node fans out to multiple downstream nodes, the runtime creates multicast streams so each downstream gets its own enumeration.

## How Streams Connect Nodes

The execution model creates streams at each stage:

1. **Source → Stream:** `ISourceNode<T>.OpenStream(context, ct)` returns an `IDataStream<TOut>`.
2. **Stream → Transform → Stream:** `IExecutionStrategy.ExecuteAsync(inputStream, node, context, ct)` returns an `IDataStream<TOut>`.
3. **Stream → Sink:** `ISinkNode<T>.ConsumeAsync(inputStream, context, ct)` consumes the stream to completion.

When nodes are connected in the graph (`builder.Connect(source, transform)`), the orchestrator wires the output stream of the upstream node as the input stream of the downstream node at execution time.

### Branching

When a node has multiple downstream connections:

1. The output stream is wrapped in a multicast stream.
2. Each downstream receives its own independent enumeration.

### Merging (Joins)

When a node has multiple upstream connections:

1. `NodeDefinition.MergeStrategy` controls how inputs are combined.
2. For non-join nodes, all inbound streams must share a single runtime item type (enforced by `PipeMergeService` via `GetDataType()`). A mismatch is a hard error with type diagnostics.
3. The merge strategy delegate is cached by `(RuntimeItemType, MergeType)` — so when lineage is enabled and the runtime item type is `LineagePacket<T>`, the cached delegate operates directly on `LineagePacket<T>` streams without any reflective adaptation.
4. Join nodes are exempt from homogeneous-type enforcement because they intentionally receive heterogeneous inputs (both sides of the join).

## Stream Lifecycle and Disposal

Streams implement `IAsyncDisposable`. The `PipelineContext` tracks all created streams via `RegisterForDisposal()`. During cleanup, all streams are disposed in reverse creation order.

> ⚠️ **Warning:** Never consume a stream without respecting the cancellation token. Use `.WithCancellation(ct)` on `IAsyncEnumerable<T>` enumerations or check `ct.ThrowIfCancellationRequested()` between items.

## Writing New Stream Implementations

If you're adding a new stream type:

1. Implement `IDataStream<T>` (or `IForwardOnlyDataStream<T>` if it can't be replayed).
2. Provide a meaningful `StreamName` for diagnostics.
3. Implement `IAsyncDisposable` to clean up resources.
4. Ensure thread safety — multiple consumers may enumerate concurrently in parallel execution scenarios.
5. Respect cancellation tokens in `GetAsyncEnumerator()`.

## Next Steps

- [Node Instantiation](node-instantiation.md) — how nodes are created to produce and consume streams
- [Cancellation](cancellation.md) — cancellation token propagation through streams
- [Execution Model](execution-model.md) — how the orchestrator wires streams between nodes

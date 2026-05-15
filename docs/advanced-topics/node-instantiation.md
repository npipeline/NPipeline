---
title: "Node Instantiation"
description: "How NPipeline creates node instances using compiled expression factories and DI."
order: 6
---

# Node Instantiation

When a pipeline executes, every node in the graph needs an instance. NPipeline uses a two-tier factory system: a fast path with **compiled expression delegates** and a fallback through **dependency injection**.

## The INodeFactory Interface

```csharp
public interface INodeFactory
{
    INode Create(NodeDefinition definition);
}
```

`DefaultNodeFactory` is the built-in implementation. It's called once per node per pipeline run.

## Fast Path: Compiled Expression Delegates

For node types with a parameterless constructor, `DefaultNodeFactory` pre-compiles a factory delegate:

```csharp
// Conceptually:
var constructor = typeof(MyNode).GetConstructor(Type.EmptyTypes);
var expression = Expression.New(constructor);
var factory = Expression.Lambda<Func<INode>>(expression).Compile();
```

The compiled delegate is cached in a `ConcurrentDictionary<Type, Func<INode>?>`. After the first call, creating a node is as fast as calling `new MyNode()` directly — no reflection overhead.

### Why Not Just Use `new()`?

The framework doesn't know the concrete node type at compile time. `PipelineBuilder.AddTransform<TNode, TIn, TOut>()` captures `typeof(TNode)`, but the execution engine works with `NodeDefinition` records that carry the type as `Type NodeType`. Compiled expressions bridge this gap without per-invocation reflection.

## Slow Path: DI and Pre-Configured Instances

When a node type doesn't have a parameterless constructor (because it takes dependencies), two alternatives exist:

### Pre-Configured Instances

The `PipelineGraph` stores pre-configured node instances in `PreconfiguredNodeInstances`:

```csharp
PipelineGraph.PreconfiguredNodeInstances: Dictionary<string, INode>
```

When the DI extension resolves nodes, it creates instances from the container and stores them here. At execution time, the factory returns the pre-configured instance instead of creating a new one.

### Activator Fallback

If no compiled factory is possible and no pre-configured instance exists, the factory falls back to `Activator.CreateInstance()`. This is intentionally slow — it's a safety net, not a recommended path.

## The Parameterless Constructor Requirement

The `NodeParameterlessConstructorAnalyzer` (Roslyn analyzer) warns at build time if a node type lacks a parameterless constructor and isn't registered through DI. This catches the most common instantiation mistake before runtime.

Node types that use DI are exempt because the DI container handles construction.

## NodeDefinition

`NodeDefinition` is the metadata record for a registered node. Created during `PipelineBuilder.AddSource/AddTransform/AddSink()` and stored in the `PipelineGraph`.

```csharp
public sealed record NodeDefinition(
    string Id,
    string Name,
    Type NodeType,
    NodeKind Kind,
    Type? InputType,
    Type? OutputType,
    IExecutionStrategy? ExecutionStrategy = null,
    TransformCardinality? DeclaredCardinality = null,
    MergeType? MergeStrategy = null,
    bool HasCustomMerge = false,
    bool IsJoin = false,
    CustomMergeDelegate? CustomMerge = null,
    LineageAdapterDelegate? LineageAdapter = null,
    Type? LineageMapperType = null,
    SinkLineageUnwrapDelegate? SinkLineageUnwrap = null,
    Type? ChildDefinitionType = null,
    ImmutableDictionary<string, object>? Metadata = null)
```

Key properties for instantiation:

- `NodeType` — the concrete `Type` to instantiate
- `Kind` — determines which interface (`ISourceNode`, `ITransformNode`, `ISinkNode`, etc.) the runtime expects
- `ExecutionStrategy` — attached strategy for transform nodes (null = default sequential)

## Execution Strategy Assignment

Every transform node has an `IExecutionStrategy`. The default is `SequentialExecutionStrategy`. Strategies are assigned in two ways:

1. **Builder methods:** `handle.WithResilience(builder)` wraps the current strategy in `ResilientExecutionStrategy`.
2. **Extension methods:** The parallelism extension adds `.WithParallelExecution()` which replaces the strategy.

The strategy is stored on the `NodeDefinition` and set on the `ITransformNode.ExecutionStrategy` property after instantiation.

## Node Lifecycle

1. **Instantiation** — `INodeFactory.Create(definition)` during orchestration setup.
2. **Strategy assignment** — `node.ExecutionStrategy = definition.ExecutionStrategy` for transform nodes.
3. **Execution** — `OpenStream()`, `TransformAsync()`, or `ConsumeAsync()` called by the executor.
4. **Disposal** — `IAsyncDisposable.DisposeAsync()` called during cleanup.

All nodes implement `INode : IAsyncDisposable`. The base classes (`SourceNode<T>`, `TransformNode<TIn, TOut>`, `SinkNode<T>`) provide default no-op disposal. Override `DisposeAsync()` if your node holds resources (database connections, file handles, etc.).

## Next Steps

- [Cancellation](cancellation.md) — how cancellation tokens reach instantiated nodes
- [Adding a Node Type](adding-a-node-type.md) — step-by-step guide to adding a new node kind
- [Data Flow Internals](data-flow-internals.md) — streams that nodes produce and consume

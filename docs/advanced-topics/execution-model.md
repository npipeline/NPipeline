---
title: "Execution Model"
description: "How PipelineRunner orchestrates node execution from graph to output."
order: 4
---

# Execution Model

This page explains how a pipeline goes from definition to completion. The flow is: **define → build graph → create runner → orchestrate → execute nodes → cleanup**.

## Entry Points

`PipelineRunner` is the public entry point. It has two main creation patterns:

```csharp
// Default runner with all built-in services
var runner = PipelineRunner.Create();

// Custom runner via builder
var runner = new PipelineRunnerBuilder()
    .WithNodeFactory(customFactory)
    .WithObservabilitySurface(customObservability)
    .Build();
```

Execution methods:

```csharp
// Simplest: default context, no cancellation
await runner.RunAsync<MyPipeline>();

// With cancellation
await runner.RunAsync<MyPipeline>(cancellationToken);

// With custom context
await runner.RunAsync<MyPipeline>(context, cancellationToken);

// With pre-built definition instance
await runner.RunAsync(definition, context, cancellationToken);
```

## Runner Dependencies

`PipelineRunner` is a primary constructor class that receives all dependencies:

| Dependency | Purpose |
|-----------|---------|
| `IPipelineFactory` | Creates `Pipeline` from `IPipelineDefinition` |
| `INodeFactory` | Instantiates `INode` from `NodeDefinition` |
| `INodeExecutor` | Executes individual nodes |
| `ITopologyService` | Computes topological execution order |
| `INodeInstantiationService` | Coordinates node creation across the graph |
| `IErrorHandlingService` | Resolves error handlers and resilience policies |
| `IPersistenceService` | Handles execution persistence |
| `IObservabilitySurface` | Receives lifecycle events |
| `ILineage` | Builds lineage adapters and records provenance |
| `IPipelineExecutionPlanCache?` | Caches execution plans across runs |
| `IRuntimePipelineBinder?` | Binds runtime configuration to the pipeline |

## Orchestration Lifecycle

`PipelineExecutionOrchestrator` coordinates the full execution. The lifecycle has four stages:

### 1. Setup

- `IPipelineFactory.Create<T>(context)` calls `IPipelineDefinition.Define(builder, context)`, then `builder.Build()` to produce a `Pipeline` containing the `PipelineGraph`.
- `IRuntimePipelineBinder.BindAsync(graph, context)` runs **before** node instantiation. It applies runtime overrides (lineage enable/disable, option decorators) and performs **runtime contract normalization** - computing a `RuntimeNodeStreamContract` for each node and normalizing any stream-sensitive execution options such as route options.
- `INodeInstantiationService` creates node instances using `INodeFactory`. Nodes with pre-configured instances (DI) skip factory creation.
- `IPipelineExecutionPlanCache` checks for a cached `NodeExecutionPlan`. On cache miss, the orchestrator builds a new plan.

### 2. Node Execution

Nodes execute in **topological order** - sources first, then transforms (respecting edge dependencies), then sinks.

For each node, the executor calls the appropriate method based on `NodeKind`:

| Node Kind | Method Called |
|-----------|-------------|
| Source | `ISourceNode<T>.OpenStream(context, ct)` |
| Transform | `IExecutionStrategy.ExecuteAsync(input, node, context, ct)` |
| StreamTransform | Direct `IStreamTransformNode<TIn,TOut>.TransformAsync()` |
| Sink | `ISinkNode<T>.ConsumeAsync(input, context, ct)` |

Transform nodes are always executed through an `IExecutionStrategy`, not directly. The strategy controls item-by-item processing, batching, parallelism, or resilience wrapping.

### 3. Lineage Recording

If lineage is enabled (`builder.EnableItemLevelLineage()`), the `ILineage` service records provenance events for each item as it passes through nodes. Lineage adapters are built at construction time and invoked during execution.

When lineage is enabled, stream items are wrapped in `LineagePacket<T>` at the source and unwrapped at the sink. Runtime contract normalization (run during binding before node instantiation) ensures every node operates on `IDataStream<LineagePacket<T>>` consistently - route options and merge strategies receive the correct wrapped type without any per-item reflection.

### 4. Cleanup

- All `IDataStream` instances are disposed (they implement `IAsyncDisposable`).
- All `INode` instances are disposed.
- Resources registered via `context.RegisterForDisposal()` are disposed.
- `IObservabilitySurface` receives the pipeline-completed event.

On failure, the error handling service captures the exception and the observability surface receives a failure event.

## Execution Strategies

Every transform node has an `IExecutionStrategy`. The strategy receives the input `IDataStream<TIn>` and returns an output `IDataStream<TOut>`:

```csharp
public interface IExecutionStrategy
{
    Task<IDataStream<TOut>> ExecuteAsync<TIn, TOut>(
        IDataStream<TIn> input,
        ITransformNode<TIn, TOut> node,
        PipelineContext context,
        CancellationToken cancellationToken);
}
```

Built-in strategies:

| Strategy | Behavior |
|----------|----------|
| `SequentialExecutionStrategy` | Processes one item at a time via `node.TransformAsync()`. Default for all transforms. |
| `BatchingExecutionStrategy` | Buffers items into batches before processing. |
| `UnbatchingExecutionStrategy` | Expands batches into individual items. |
| `ResilientExecutionStrategy` | Wraps another strategy with retry, circuit breaker, and materialization. |

The parallelism extension adds strategies for concurrent execution with configurable thread count and queue policies.

## Execution Plan Caching

`NodeExecutionPlan` captures the pre-computed execution metadata for a graph. Plans are cached by `IPipelineExecutionPlanCache` (default: `InMemoryPipelineExecutionPlanCache`) so that repeated runs of the same pipeline definition skip the setup overhead.

The cache key is based on the pipeline definition type. Cache invalidation happens when the runner is reconfigured.

## PipelineRunnerBuilder Configuration

The builder allows replacing any internal service:

```csharp
var runner = new PipelineRunnerBuilder()
    .WithNodeFactory(customFactory)
    .WithNodeExecutor(customExecutor)
    .WithTopologyService(customTopology)
    .WithObservabilitySurface(customObservability)
    .WithLineage(customLineage)
    .WithErrorHandlingService(customErrorService)
    .WithExecutionPlanCache(customCache)
    .Build();
```

Use `WithoutExecutionPlanCache()` to disable caching for testing or debugging.

## Runtime Contract Normalization

`RuntimePipelineBinder` runs at the start of each execution and writes a `RuntimeNodeStreamContract` for every node into the graph's execution annotations. The contract records:

| Field | Description |
|-------|-------------|
| `EffectiveInputItemType` | The actual stream item type flowing into this node at runtime (`LineagePacket<T>` when lineage is enabled, `T` otherwise; `null` for sources). |
| `EffectiveOutputItemType` | The actual stream item type flowing out (`LineagePacket<T>` when lineage is enabled; `null` for sinks). |
| `ItemLevelLineageEnabled` | Whether item-level lineage is active for this run. |

The contract is stored under the annotation key `runtime.stream.contract::{nodeId}` and is consumed by `NodeExecutor` to validate inputs before execution. For non-join nodes, all inbound input streams must match `EffectiveInputItemType` - a mismatch is a hard error with assembly-qualified type diagnostics.

### Route Option Normalization

Route predicates configured on the pipeline builder operate on the payload type `T`. When lineage is enabled, the runtime stream item type is `LineagePacket<T>`. The binder normalizes route options once at bind time:

1. If options already match the effective runtime item type, they are used as-is.
2. If lineage is enabled and options are payload-typed `RouteOptions<T>`, they are lifted to `RouteOptions<LineagePacket<T>>` - predicates are re-wrapped to delegate to `packet.Data`.
3. Any other mismatch is a hard error at bind time with type diagnostics.

This normalization happens once per run. No per-item route adaptation occurs during execution.

## Next Steps

- [Data Flow Internals](data-flow-internals.md) - how streams connect nodes
- [Node Instantiation](node-instantiation.md) - how INodeFactory creates node instances
- [Cancellation](cancellation.md) - how cancellation tokens propagate through execution

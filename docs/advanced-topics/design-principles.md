---
title: "Design Principles"
description: "Core philosophy and design decisions behind NPipeline's architecture."
order: 3
---

# Design Principles

These principles explain *why* NPipeline is built the way it is. Understanding them helps you make changes that are consistent with the existing codebase.

## Streaming-First

Data flows item-by-item through `IAsyncEnumerable<T>`. Nothing is buffered in memory unless the user explicitly opts in (materialization, batching, in-memory streams).

**Why:** Pipelines often process datasets larger than available memory. Streaming lets you process a 10GB CSV file with constant memory usage.

**Implication for contributors:** Never collect an entire stream into a list as a convenience. If a feature requires buffering, make it opt-in with a configurable cap (see `MaxMaterializedItems` and `CappedReplayableDataStream<T>`).

## Lazy Evaluation

Source nodes don't produce items until the downstream consumer requests them. Transforms don't execute until enumerated. This is the natural behavior of `IAsyncEnumerable<T>` — items are pulled, not pushed.

**Why:** Lazy evaluation enables backpressure for free. A slow sink automatically slows down the source without explicit flow control logic.

**Implication for contributors:** Don't eagerly enumerate streams in execution strategies unless required for correctness (e.g., materialization for restart). Use `await foreach` and yield results as `IAsyncEnumerable<T>`.

## Type Safety at the Graph Level

Node connections are validated at build time through typed handles (`SourceNodeHandle<TOut>`, `TransformNodeHandle<TIn, TOut>`, `SinkNodeHandle<TIn>`). The C# compiler prevents connecting nodes with incompatible types.

**Why:** Runtime type mismatches in data pipelines are notoriously hard to debug. Compile-time checking eliminates an entire class of errors.

**Implication for contributors:** When adding new node types, ensure the builder method returns a strongly-typed handle. Use generic constraints to enforce type compatibility in `Connect()` overloads.

## Immutable Configuration

All configuration records (`PipelineRetryOptions`, `PipelineCircuitBreakerOptions`, `LineageOptions`, etc.) are `sealed record` types. They're modified using `with` expressions, never mutation.

**Why:** Immutability prevents configuration changes during execution from creating race conditions. It also enables safe sharing across threads.

**Implication for contributors:** New configuration should be `sealed record` with `init`-only properties or positional parameters. Provide a `Default` static property. Include a `Validate()` method when constraints exist.

## Fail-Fast Defaults

The default resilience policy returns `Fail` for all failure types. No items are silently skipped or retried. The user must explicitly opt into error recovery.

**Why:** Silent data loss is worse than a loud failure. In data pipelines, a dropped item that nobody notices can corrupt downstream reports, analytics, or databases.

**Implication for contributors:** New features should default to the strictest (safest) behavior. Permissive behavior requires explicit opt-in.

## Zero-Allocation Hot Paths

The item processing loop — the code path that runs once per item per node — avoids heap allocations. This means no LINQ in hot paths, no closures that capture variables, no boxing of value types, and `ValueTask<T>` where appropriate.

**Why:** Pipeline throughput is directly limited by GC pressure. At millions of items per second, even one allocation per item causes significant GC overhead.

**Implication for contributors:** Roslyn analyzers (`LinqInHotPathsAnalyzer`, `AnonymousObjectAllocationAnalyzer`, `ValueTaskOptimizationAnalyzer`) enforce this. Profile with BenchmarkDotNet before and after changes to hot paths.

## Compiled Factories Over Reflection

Node instantiation uses compiled expression trees cached in a `ConcurrentDictionary<Type, Func<INode>>`. This eliminates per-invocation reflection overhead after the first call.

**Why:** `Activator.CreateInstance` is slow. Compiled expressions are as fast as direct constructor calls after the initial compilation cost.

**Implication for contributors:** See [Node Instantiation](node-instantiation.md) for details. Node types must have a parameterless constructor (enforced by the `NodeParameterlessConstructorAnalyzer`) unless using DI.

## Extension Points Over Modification

New behavior is added by implementing interfaces (`IExecutionStrategy`, `IResiliencePolicy`, `IDeadLetterSink`, `IObservabilitySurface`, `ILineage`), not by modifying existing classes.

**Why:** Open/closed principle. The core runtime is stable; extensions add capabilities without risking regressions.

**Implication for contributors:** Before modifying an existing class, check if the behavior can be added through an existing extension point. If not, consider adding a new extension point rather than embedding the behavior.

## Separation of Construction and Execution

Pipeline definition (`PipelineBuilder`) is separate from pipeline execution (`PipelineRunner`). The builder produces an immutable `PipelineGraph`; the runner executes it.

**Why:** This separation enables execution plan caching (same graph, different runs), graph validation before execution, and potential future features like graph serialization or visualization.

**Implication for contributors:** Don't mix construction logic with execution logic. The builder should never depend on execution classes, and execution code should only read from the immutable graph.

## Next Steps

- [Execution Model](execution-model.md) — how these principles manifest in the runner
- [Node Instantiation](node-instantiation.md) — compiled factories in detail
- [Coding Conventions](../contributing/coding-conventions.md) — practical rules derived from these principles

---
title: "Common Issues"
description: "Symptom-based troubleshooting guide for frequent NPipeline problems."
order: 2
---

# Common Issues

This page covers frequently encountered issues, organized by symptom.

## Pipeline Won't Build

### "A node with the given name already exists" (NP0105)

Two nodes share the same name. Names must be unique within a pipeline:

```csharp
// Bad - duplicate name
builder.AddTransform<NodeA, In, Out>("process");
builder.AddTransform<NodeB, In, Out>("process"); // NP0105

// Good - unique names
builder.AddTransform<NodeA, In, Out>("process-a");
builder.AddTransform<NodeB, In, Out>("process-b");
```

### "Type mismatch in connection" (NP0201)

The output type of the source node doesn't match the input type of the target node:

```csharp
var source = builder.AddSource<MySource, string>("source");
var transform = builder.AddTransform<MyTransform, int, int>("transform");
builder.Connect(source, transform); // NP0201 - string ≠ int
```

Use `CanConnect()` to check compatibility before connecting.

### "Cyclic dependency detected" (NP0103)

Your pipeline graph contains a cycle. NPipeline pipelines must be directed acyclic graphs (DAGs). Use `PipelineGraphExporter.ToMermaid(graph)` to visualize the graph and find the cycle.

### Node restart requires a resumable execution strategy (NP0425)

A transform sets `NodeRestart.MaxRestarts` above zero, but its execution strategy doesn't implement `IResumableExecutionStrategy`, so it can't resume from a checkpoint after a restart.

**Fix:** Use a resumable strategy, such as `SequentialExecutionStrategy` or a parallel strategy, implement `IResumableExecutionStrategy` in your own strategy, or set `NodeRestart.MaxRestarts` to 0 for that node. For more information, see [Node restart and the replay window](../error-handling/materialization.md).

### Transform-only resilience setting on a non-transform node (NP9204)

`ItemRetry`, `NodeRestart`, and `CircuitBreaker` apply only to transform nodes. Setting them for a source, sink, aggregate, or join fails the build.

**Fix:** Retry reads and writes in the node's connector, or use `NodeRetry` to execute the node again before it reads any input.

## Pipeline Fails at Runtime

### NodeExecutionException

An unhandled exception occurred in a node's `TransformAsync`, `ConsumeAsync`, or `OpenStream` method. Check the `InnerException` for the root cause and the `NodeId` property to identify which node failed.

**Fix:** Decide how the node should recover. Under the `Default` optimization profile, a transform already retries a transient item failure three times with exponential backoff. To retry every failure and then dead-letter the item, register a resilience policy and a dead-letter sink:

```csharp
// Retry any failure up to 3 times, then dead-letter the item
var policy = ResiliencePolicyBuilder
    .ForNode<MyTransform, MyData>()
    .OnAny().Retry(maxRetries: 3)
    .Build();

builder.AddResiliencePolicy(policy);
builder.AddDeadLetterSink(new BoundedInMemoryDeadLetterSink());
```

To restart a transform whose stream fails mid-stream, set `NodeRestart` in its options. You don't need a separate call to enable it:

```csharp
builder.WithResilience(transform, o => o with
{
    NodeRestart = new NodeRestartOptions { MaxRestarts = 3 },
});
```

Item retry, node restart, and the circuit breaker apply only to transform nodes. Setting them for a source, sink, aggregate, or join fails the build (NP9204). For a source or sink, retry reads and writes in the connector instead. For more information, see [The three resilience layers](../error-handling/three-layers.md).

### CircuitBreakerOpenException

A node's circuit breaker is open, so it refused an item attempt. The breaker opened because the node's dependency kept
failing transiently. The exception's `NodeId` and `State` properties say which breaker refused the attempt and in what
state.

**Fix:** Investigate the underlying failure that opened the breaker; the observer's `OnCircuitStateChanged` event and
the breaker's log messages give the reason. If short outages are expected, set `WhenOpen = BreakerOpenBehavior.Pause`
so that attempts wait for the breaker instead of failing. For more information, see
[Circuit Breakers](../error-handling/circuit-breakers.md).

### RetryExhaustedException (NP0311)

All retry attempts failed. The `AttemptCount` property shows how many attempts were made, and the inner exception is the last failure.

**Fix:** Raise the node's retry limit, for example `ItemRetry = o.ItemRetry with { MaxRetries = 5 }`, or route failed items to a dead-letter queue for manual review with `OnItemFailure = ItemFailureAction.DeadLetter` and a dead-letter sink. If a registered policy decides the node's failures, change the policy instead, because the options are only advice to it. For more information, see [Dead-Letter Queues](../error-handling/dead-letter-queues.md).

### Lineage materialization cap exceeded

With item-level lineage enabled, a node whose inputs and outputs aren't 1:1 buffered more than
`LineageOptions.MaterializationCap` items, and `OverflowPolicy` is `LineageOverflowPolicy.Strict`. The node fails with
an `InvalidOperationException` whose message starts with `[NPipeline.Lineage] Materialization cap exceeded`.

**Fix:** Raise the cap, or use the default `LineageOverflowPolicy.Degrade`, which switches to positional mapping
instead of failing:

```csharp
builder.EnableItemLevelLineage(o => o with
{
    MaterializationCap = 50_000,
    OverflowPolicy = LineageOverflowPolicy.Degrade,
});
```

### DeadLetterQueueCapacityExceeded (NP0502)

The dead letter queue is full. Process or drain the dead letter queue, or increase its capacity.

## Performance Issues

### High Memory Usage

- For transforms with node restart, lower `NodeRestartOptions.MaxReplayWindow` to hold fewer items for replay
- Verify you're using `DataStream<T>` streaming rather than materializing entire datasets
- Enable memory metrics to identify the culprit node: `AddNPipelineObservability(new ObservabilityExtensionOptions { EnableMemoryMetrics = true })`

### Slow Throughput

- Check for blocking calls - analyzer NP9101 detects `.Result`, `.Wait()`
- Check for LINQ in hot paths - analyzer NP9103
- Return `ValueTask.FromResult(...)` from `TransformAsync` for synchronous transforms so no `Task` is allocated per item
- Use parallel execution for CPU-bound transforms

## Next Steps

- [Debugging Tips](debugging-tips.md) - visualization and logging
- [Error Handling](../error-handling/index.md) - configure resilience
- [Error Codes](../reference/error-codes.md) - full error code catalog

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

## Pipeline Fails at Runtime

### NodeExecutionException

An unhandled exception occurred in a node's `TransformAsync`, `ConsumeAsync`, or `OpenStream` method. Check the `InnerException` for the root cause and the `NodeId` property to identify which node failed.

**Fix:** Add a resilience policy and enable resilient execution to handle transient errors:

```csharp
// Configure a resilience policy that retries then dead-letters
var policy = ResiliencePolicyBuilder
    .ForNode<MyTransform, MyData>()
    .OnAny().Retry(maxRetries: 3)
    .Build();

builder.AddResiliencePolicy(policy);
builder.AddDeadLetterSink(new BoundedInMemoryDeadLetterSink());
transform.WithResilience(builder);
```

### CircuitBreakerTrippedException (NP0310)

Too many consecutive failures triggered the circuit breaker. Check the `FailureThreshold` property.

**Fix:** Investigate the underlying error causing repeated failures. Increase the threshold or open duration if failures are expected:

```csharp
builder.WithCircuitBreaker(
    failureThreshold: 10,
    openDuration: TimeSpan.FromSeconds(30));
```

### RetryExhaustedException (NP0311)

All retry attempts failed. The `AttemptCount` property shows how many attempts were made.

**Fix:** Either increase `MaxItemRetries` or route failed items to a dead letter queue for manual review.

### MaterializationCapExceeded (NP0503)

The `MaxMaterializedItems` limit was reached. This safety guard prevents unbounded memory growth when replaying items during retry.

**Fix:** Increase the cap or redesign to process smaller batches:

```csharp
new PipelineRetryOptions { MaxMaterializedItems = 50000 }
```

> **Warning:** Analyzer rule NP9002 flags missing `MaxMaterializedItems` as an **error** because unbounded materialization can cause out-of-memory crashes.

### DeadLetterQueueCapacityExceeded (NP0502)

The dead letter queue is full. Process or drain the dead letter queue, or increase its capacity.

## Performance Issues

### High Memory Usage

- Check for unbounded materialization - set `MaxMaterializedItems` on retry options
- Verify you're using `DataStream<T>` streaming rather than materializing entire datasets
- Enable memory metrics to identify the culprit node: `AddNPipelineObservability(new ObservabilityExtensionOptions { EnableMemoryMetrics = true })`

### Slow Throughput

- Check for blocking calls - analyzer NP9101 detects `.Result`, `.Wait()`
- Check for LINQ in hot paths - analyzer NP9103
- Override `ExecuteValueTaskAsync` for synchronous transforms - analyzer NP9106
- Use parallel execution for CPU-bound transforms

## Next Steps

- [Debugging Tips](debugging-tips.md) - visualization and logging
- [Error Handling](../error-handling/index.md) - configure resilience
- [Error Codes](../reference/error-codes.md) - full error code catalog

---
title: Pipeline-Level Error Handling
description: Handle errors that affect entire node streams using IResiliencePolicy
order: 3
---

# Pipeline-Level Error Handling

Pipeline-level error handling manages errors that affect an entire node's execution stream rather than individual items. These are typically infrastructure failures — a database connection dropping, an external service going down — where the entire stream must be restarted or bypassed.

When a stream-level error occurs, `ResilientExecutionStrategy` calls `DecidePipelineFailureAsync` on the configured `IResiliencePolicy` to determine how to react.

## Stream Failure Decisions

`DecidePipelineFailureAsync` returns a `ResilienceDecision`:

| Value | Effect |
|---|---|
| `Fail` | Pipeline terminates (default) |
| `RestartNode` | The node's buffered input stream is replayed from the start |
| `ContinueWithoutNode` | The node is removed from the pipeline; downstream receives an empty stream |

> **`RestartNode` requires three mandatory prerequisites.** See [Getting Started with Resilience](getting-started.md) for the full checklist.

## Implementing Stream Failure Handling

Extend `ResiliencePolicyBase` and override `DecidePipelineFailureAsync`:

```csharp
using NPipeline.Pipeline;
using NPipeline.Resilience;

public sealed class MyStreamPolicy : ResiliencePolicyBase
{
    private readonly ILogger _logger;

    public MyStreamPolicy(ILogger logger) => _logger = logger;

    public override Task<ResilienceDecision> DecidePipelineFailureAsync(
        string nodeId,
        Exception exception,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        _logger.LogError(exception, "Stream failure in node '{NodeId}'", nodeId);

        return Task.FromResult(exception switch
        {
            OutOfMemoryException => ResilienceDecision.Fail,
            HttpRequestException => ResilienceDecision.RestartNode,
            TimeoutException     => ResilienceDecision.RestartNode,
            _                    => ResilienceDecision.Fail
        });
    }
}
```

Register the policy:

```csharp
public sealed class MyPipelineDefinition : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source    = builder.AddSource<DataSource, string>();
        var transform = builder.AddTransform<DataTransform, string, string>();
        var sink      = builder.AddSink<DataSink, string>();

        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        builder.AddResiliencePolicy<MyStreamPolicy>();
        builder.WithResilience(transform);          // wraps with ResilientExecutionStrategy
        builder.WithRetryOptions(o => o.With(
            maxNodeRestartAttempts: 3,
            maxMaterializedItems: 1000));
    }
}
```

## Common Scenarios

### Resource Exhaustion — Fail Fast

```csharp
public override Task<ResilienceDecision> DecidePipelineFailureAsync(
    string nodeId, Exception exception, PipelineContext context, CancellationToken ct)
{
    if (exception is OutOfMemoryException)
    {
        _logger.LogCritical("Resource exhaustion in node {NodeId}", nodeId);
        return Task.FromResult(ResilienceDecision.Fail);
    }

    return Task.FromResult(ResilienceDecision.RestartNode);
}
```

### Graceful Degradation — Retry Once, Then Continue Without Node

```csharp
private readonly HashSet<string> _attemptedRestart = [];

public override Task<ResilienceDecision> DecidePipelineFailureAsync(
    string nodeId, Exception exception, PipelineContext context, CancellationToken ct)
{
    _logger.LogError(exception, "Node {NodeId} stream failed", nodeId);

    if (_attemptedRestart.Add(nodeId))
        return Task.FromResult(ResilienceDecision.RestartNode);

    return Task.FromResult(ResilienceDecision.ContinueWithoutNode);
}
```

### External Service Failures with Circuit-Breaker Logic

```csharp
public override Task<ResilienceDecision> DecidePipelineFailureAsync(
    string nodeId, Exception exception, PipelineContext context, CancellationToken ct)
{
    _failureCounts.TryGetValue(nodeId, out var count);
    _failureCounts[nodeId] = count + 1;

    _metrics.Increment("stream_failures", [("node", nodeId), ("error", exception.GetType().Name)]);
    _logger.LogError(exception, "Node {NodeId} failed (attempt {Attempt})", nodeId, count + 1);

    return Task.FromResult(exception switch
    {
        OutOfMemoryException                  => ResilienceDecision.Fail,
        _ when _failureCounts[nodeId] < 3     => ResilienceDecision.RestartNode,
        _                                      => ResilienceDecision.ContinueWithoutNode
    });
}
```

## Best Practices

1. **Fail fast on critical errors** — `OutOfMemoryException`, `StackOverflowException`, and similar should terminate immediately.
2. **Cap restart attempts** — use `MaxNodeRestartAttempts` in `PipelineRetryOptions`; don't rely solely on policy logic.
3. **Prefer `ContinueWithoutNode` for optional enrichment nodes** — it allows the rest of the pipeline to complete.
4. **Log with node context** — include `nodeId` and failure counts so stream failures are traceable.
5. **Combine with circuit breakers** — NPipeline's built-in circuit breaker support (via `GetCircuitBreaker`) provides automatic open/close behavior without custom tracking.

## Prerequisites for `RestartNode` ⚠️

`RestartNode` requires all three of the following or it silently falls back to failure:

1. Node wrapped with `ResilientExecutionStrategy` (`.WithResilience(handle)`)
2. `MaxNodeRestartAttempts > 0`
3. `MaxMaterializedItems` set to a positive value

See [Getting Started with Resilience](getting-started.md) for the complete checklist.

## Related Documentation

- [Resilience Policy](resilience-policy.md) — Full `IResiliencePolicy` reference
- [Error Handling Overview](error-handling-overview.md) — Understand all error levels
- [Getting Started with Resilience](getting-started.md) — Node restart prerequisites
- [Retries](retries.md) — Configure retry options
- [Circuit Breakers](circuit-breakers.md) — Built-in circuit breaker support

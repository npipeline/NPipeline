---
title: Resilience Policy
description: Configure one resilience entry point for retry, error decisions, dead-letter handling, and circuit breaking.
order: 2
---

# Resilience Policy

NPipeline exposes a single resilience entry point: `IResiliencePolicy`.

`IResiliencePolicy` centralizes:

- node-level failure decisions
- pipeline-level failure decisions (stream restart / continue-without-node)
- item-level failure decisions (retry / skip / dead-letter)
- retry delay resolution
- circuit breaker resolution

The built-in `DefaultResiliencePolicy` fail-fasts on all errors. Custom policies override only the decisions they need.

## Configure a Policy

Use a policy instance:

```csharp
builder.AddResiliencePolicy(new MyResiliencePolicy());
```

Or use a policy type (instantiated at bind time):

```csharp
builder.AddResiliencePolicy<MyResiliencePolicy>();
```

You can also provide a policy directly on context configuration:

```csharp
var context = new PipelineContext(
    PipelineContextConfiguration.WithResilience(new MyResiliencePolicy()));
```

## Implement a Custom Policy

Extend `ResiliencePolicyBase` and override only the methods you need. All defaults return `Fail`, delegate retry delay to the pipeline strategy, and delegate circuit breaking to `DefaultResiliencePolicy`.

```csharp
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Resilience;

// Minimal: only handle item-level failures.
public sealed class RetryTransientPolicy : ResiliencePolicyBase
{
    public override Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
        ITransformNode<TIn, TOut> node,
        TIn failedItem,
        Exception exception,
        PipelineContext context,
        string nodeId,
        int retryAttempt,
        CancellationToken cancellationToken)
    {
        return Task.FromResult(exception is TimeoutException
            ? ResilienceDecision.Retry
            : ResilienceDecision.Fail);
    }
}
```

For pipeline-level stream restarts, override `DecidePipelineFailureAsync`:

```csharp
public sealed class RestartOnStreamFailurePolicy : ResiliencePolicyBase
{
    public override Task<ResilienceDecision> DecidePipelineFailureAsync(
        string nodeId,
        Exception exception,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        return Task.FromResult(ResilienceDecision.RestartNode);
    }
}
```

> **Tip:** You can also implement `IResiliencePolicy` directly for full control over all five methods.

## Node-scoped Policies

Assign a focused policy to a single node for item-level failures:

```csharp
builder.SetNodeResiliencePolicy(transform, new RetryTransientPolicy());
```

The node-scoped policy takes priority over the global policy for that node's item-level decisions.

## Fluent Rule-based Policies

Use `ResiliencePolicyBuilder` for concise rule-based policies without subclassing:

```csharp
var policy = ResiliencePolicyBuilder.ForNode<MyTransformNode, MyItem>()
    .RetryOn<TimeoutException>(maxRetries: 3, exhaustedDecision: ResilienceDecision.Skip)
    .RetryWhen(
        ex => ex.Message.Contains("transient", StringComparison.OrdinalIgnoreCase),
        maxRetries: 2,
        exhaustedDecision: ResilienceDecision.DeadLetter)
    .Otherwise(ResilienceDecision.Fail)
    .Build();

builder.SetNodeResiliencePolicy(transformNode, policy);
```

Pre-built shortcuts:

```csharp
// Retry all item failures (max 3 times, then dead-letter)
var retry = ResiliencePolicyBuilder.RetryAlways<MyNode, string>();

// Skip all item failures
var skip = ResiliencePolicyBuilder.SkipAlways<MyNode, string>();

// Dead-letter all item failures
var dl = ResiliencePolicyBuilder.DeadLetterAlways<MyNode, string>();
```

## Decision Values

| Decision | Meaning |
| --- | --- |
| `Fail` | Stop execution and surface the exception |
| `Retry` | Retry the current item or operation |
| `Skip` | Skip the failing item and continue |
| `DeadLetter` | Route to dead-letter sink and continue |
| `RestartNode` | Restart the failed node/stream (pipeline-level) |
| `ContinueWithoutNode` | Continue pipeline without the failed node |

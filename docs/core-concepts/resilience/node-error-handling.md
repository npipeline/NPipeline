---
title: Node-Level Error Handling
description: Handle errors for individual items within nodes using IResiliencePolicy
order: 2
---

# Node-Level Error Handling

Node-level error handling defines what happens to problematic items without affecting the entire pipeline. When an error occurs while processing an individual item, NPipeline calls `DecideItemFailureAsync` on the configured `IResiliencePolicy` to determine how to proceed.

## Item Failure Decisions

`DecideItemFailureAsync` returns a `ResilienceDecision`:

| Value | Effect |
|---|---|
| `Fail` | Pipeline terminates with the exception (default) |
| `Retry` | Item is retried up to `MaxItemRetries` times |
| `Skip` | Item is discarded; pipeline continues with the next |
| `DeadLetter` | Item is routed to the dead-letter sink; pipeline continues |

## Implementing Item Failure Handling

Extend `ResiliencePolicyBase` and override `DecideItemFailureAsync`. Parameters include the failing node, the item, the exception, the pipeline context, the node ID, and the current retry attempt number (0-based).

```csharp
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Resilience;

public sealed class MyItemPolicy : ResiliencePolicyBase
{
    private readonly ILogger _logger;

    public MyItemPolicy(ILogger logger) => _logger = logger;

    public override Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
        ITransformNode<TIn, TOut> node,
        TIn failedItem,
        Exception exception,
        PipelineContext context,
        string nodeId,
        int retryAttempt,
        CancellationToken cancellationToken)
    {
        _logger.LogError(exception, "Error in node '{NodeId}' on attempt {Attempt}", nodeId, retryAttempt);

        return Task.FromResult(exception switch
        {
            FormatException     => ResilienceDecision.DeadLetter,
            TimeoutException    => ResilienceDecision.Retry,
            ValidationException => ResilienceDecision.Skip,
            _                   => ResilienceDecision.Fail
        });
    }
}
```

Register the policy:

```csharp
// Pipeline-wide
builder.AddResiliencePolicy(new MyItemPolicy(logger));

// Scoped to a single node
builder.SetNodeResiliencePolicy(transformHandle, new MyItemPolicy(logger));
```

Node-scoped policies override the pipeline-wide policy for that node's item decisions.

## Fluent Rule-Based Policies

Use `ResiliencePolicyBuilder` to build rule-based policies without subclassing:

```csharp
using NPipeline.ErrorHandling;

var policy = ResiliencePolicyBuilder.ForNode<MyTransform, string>()
    .On<TimeoutException>().Retry(3)
    .On<ValidationException>().Skip()
    .OnAny().DeadLetter()
    .Build();

builder.SetNodeResiliencePolicy(transformHandle, policy);
```

### Typed Retry Shortcuts

```csharp
var policy = ResiliencePolicyBuilder.ForNode<MyTransform, string>()
    .RetryOn<TimeoutException>(maxRetries: 3, exhaustedDecision: ResilienceDecision.Skip)
    .RetryWhen(
        ex => ex.Message.Contains("transient", StringComparison.OrdinalIgnoreCase),
        maxRetries: 2,
        exhaustedDecision: ResilienceDecision.DeadLetter)
    .Otherwise(ResilienceDecision.Fail)
    .Build();
```

### Pre-built Factories

```csharp
// Retry all errors (max 3 times), then dead-letter
var retry = ResiliencePolicyBuilder.RetryAlways<MyTransform, string>(maxRetries: 3);

// Skip all errors
var skip = ResiliencePolicyBuilder.SkipAlways<MyTransform, string>();

// Dead-letter all errors
var dl = ResiliencePolicyBuilder.DeadLetterAlways<MyTransform, string>();
```

### Exception Matching

Rules evaluate in the order added — place more specific types first:

```csharp
var policy = ResiliencePolicyBuilder.ForNode<MyTransform, string>()
    .On<TimeoutException>().Retry(3)      // most specific
    .On<IOException>().Retry(5)
    .On<ArgumentException>().Skip()
    .OnAny().DeadLetter()                 // catch-all must be last
    .Build();
```

`OnAny()` matches all exception types and **must** be the last rule.

### Custom Predicate Matching

```csharp
var policy = ResiliencePolicyBuilder.ForNode<MyTransform, string>()
    .When(ex => ex.Message.Contains("timeout", StringComparison.OrdinalIgnoreCase)).Retry(3)
    .When(ex => ex.Message.Contains("invalid", StringComparison.OrdinalIgnoreCase)).Skip()
    .OnAny().Fail()
    .Build();
```

### Default Fallback

```csharp
var policy = ResiliencePolicyBuilder.ForNode<MyTransform, string>()
    .On<TimeoutException>().Retry(2)
    .Otherwise(ResilienceDecision.Skip)   // default when no rule matches
    .Build();
```

## Common Scenarios

### Transient Network Errors

```csharp
public sealed class NetworkPolicy : ResiliencePolicyBase
{
    public override Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
        ITransformNode<TIn, TOut> node, TIn failedItem, Exception exception,
        PipelineContext context, string nodeId, int retryAttempt, CancellationToken ct)
    {
        if (exception is HttpRequestException)
            return Task.FromResult(retryAttempt < 3 ? ResilienceDecision.Retry : ResilienceDecision.DeadLetter);

        return Task.FromResult(ResilienceDecision.Skip);
    }
}
```

### Production Policy with Metrics

```csharp
public sealed class ProductionItemPolicy : ResiliencePolicyBase
{
    private readonly ILogger _logger;
    private readonly IMetrics _metrics;

    public ProductionItemPolicy(ILogger logger, IMetrics metrics)
    {
        _logger = logger;
        _metrics = metrics;
    }

    public override Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
        ITransformNode<TIn, TOut> node, TIn failedItem, Exception exception,
        PipelineContext context, string nodeId, int retryAttempt, CancellationToken ct)
    {
        _metrics.Increment("item_errors", [("node", nodeId), ("error", exception.GetType().Name)]);
        _logger.LogError(exception, "Item failure in node {NodeId} (attempt {Attempt})", nodeId, retryAttempt);

        return Task.FromResult(exception switch
        {
            ValidationException  => ResilienceDecision.DeadLetter,
            TimeoutException     => ResilienceDecision.Retry,
            HttpRequestException => ResilienceDecision.Retry,
            _                    => ResilienceDecision.Skip
        });
    }
}
```

## Best Practices

1. **Match on specific exception types** — transient errors warrant retry; data quality errors warrant dead-letter or skip.
2. **Use `retryAttempt` for escalation** — return `DeadLetter` after a threshold rather than retrying indefinitely.
3. **Configure `MaxItemRetries`** — set a bound in `PipelineRetryOptions` to limit total retry cost.
4. **Keep handlers stateless** — if you need per-item state, drive it from `retryAttempt`.
5. **Log with context** — include `nodeId` and `retryAttempt` so failures are traceable in production.

## Related Documentation

- [Resilience Policy](resilience-policy.md) — Full `IResiliencePolicy` reference
- [Error Handling Overview](error-handling-overview.md) — Understand all error levels
- [Retries](retries.md) — Configure retry options and delay strategies
- [Dead Letter Queues](dead-letter-queues.md) — Route failed items for analysis

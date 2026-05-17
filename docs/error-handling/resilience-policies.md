---
title: "Resilience Policies"
description: "Define how NPipeline responds to failures using IResiliencePolicy and the fluent builder."
order: 3
---

# Resilience Policies

A resilience policy tells NPipeline what to do when something goes wrong. It receives the failure context and returns a `ResilienceDecision` - retry, skip, dead-letter, fail, restart the node, or continue without it.

## The IResiliencePolicy Interface

The core contract lives in `NPipeline.Resilience`:

```csharp
public interface IResiliencePolicy
{
    Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
        ITransformNode<TIn, TOut> node,
        TIn failedItem,
        Exception exception,
        PipelineContext context,
        string nodeId,
        int retryAttempt,
        CancellationToken cancellationToken);

    Task<ResilienceDecision> DecideNodeFailureAsync(
        NodeDefinition nodeDefinition,
        INode node,
        Exception exception,
        PipelineContext context,
        CancellationToken cancellationToken);

    Task<ResilienceDecision> DecidePipelineFailureAsync(
        string nodeId,
        Exception exception,
        PipelineContext context,
        CancellationToken cancellationToken);

    ValueTask<TimeSpan> GetRetryDelayAsync(
        PipelineContext context,
        int attemptNumber,
        CancellationToken cancellationToken);

    IResilienceCircuitBreaker? GetCircuitBreaker(
        PipelineContext context,
        string nodeId);
}
```

Most policies only need to customize one or two of these methods. Use `ResiliencePolicyBase` to avoid boilerplate.

## Using ResiliencePolicyBase

`ResiliencePolicyBase` provides fail-fast defaults for every method. Override only what you need:

```csharp
using NPipeline.Pipeline;
using NPipeline.Resilience;

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
        // Retry transient failures up to 3 times, then dead-letter
        if (exception is TimeoutException or HttpRequestException)
        {
            return Task.FromResult(retryAttempt < 3
                ? ResilienceDecision.Retry
                : ResilienceDecision.DeadLetter);
        }

        // All other exceptions: fail immediately
        return Task.FromResult(ResilienceDecision.Fail);
    }
}
```

Register it on the builder:

```csharp
builder.AddResiliencePolicy(new RetryTransientPolicy());
```

Or register by type (resolved via DI):

```csharp
builder.AddResiliencePolicy<RetryTransientPolicy>();
```

## Fluent Resilience Policy Builder

For common patterns, the `ResiliencePolicyBuilder` in `NPipeline.ErrorHandling` provides a concise fluent API:

```csharp
using NPipeline.ErrorHandling;
using NPipeline.Resilience;

// Retry TimeoutException up to 3 times, then dead-letter. Fail on anything else.
var policy = ResiliencePolicyBuilder
    .ForNode<MyTransform, MyData>()
    .On<TimeoutException>().Retry(maxRetries: 3)
    .On<FormatException>().Skip()
    .OnAny().Fail()
    .Build();

builder.AddResiliencePolicy(policy);
```

### Builder Methods

| Method | Effect |
|--------|--------|
| `.On<TException>()` | Match a specific exception type |
| `.OnAny()` | Match any exception (catch-all - must be last) |
| `.When(predicate)` | Match exceptions passing a custom predicate |
| `.Retry(maxRetries)` | Retry up to N times, then dead-letter |
| `.Skip()` | Discard the item and continue |
| `.DeadLetter()` | Route to the dead-letter sink |
| `.Fail()` | Stop the pipeline |
| `.Otherwise(decision)` | Fallback when no rule matches |

### Rule Ordering

Rules are evaluated in order. The first matching rule wins. A catch-all rule (`.OnAny()`) must be last - placing it before other rules throws an `InvalidOperationException` at build time.

```csharp
// ✓ Correct: specific rules first, catch-all last
var policy = ResiliencePolicyBuilder
    .ForNode<MyTransform, string>()
    .On<TimeoutException>().Retry(3)
    .On<FormatException>().Skip()
    .OnAny().DeadLetter()
    .Build();

// ✗ Throws InvalidOperationException: catch-all before specific rules
var bad = ResiliencePolicyBuilder
    .ForNode<MyTransform, string>()
    .OnAny().Retry(3)
    .On<FormatException>().Skip()  // unreachable
    .Build();
```

### Pre-Built Shortcuts

For the most common patterns, `ResiliencePolicyBuilder` provides static factory methods:

```csharp
// Retry all item failures up to 3 times
var retryAll = ResiliencePolicyBuilder.RetryAlways<MyTransform, string>(maxRetries: 3);

// Retry only TimeoutException, dead-letter after exhaustion
var retryTimeout = ResiliencePolicyBuilder
    .RetryOn<MyTransform, string, TimeoutException>(maxRetries: 5);

// Skip all item failures silently
var skipAll = ResiliencePolicyBuilder.SkipAlways<MyTransform, string>();

// Dead-letter all item failures
var dlqAll = ResiliencePolicyBuilder.DeadLetterAlways<MyTransform, string>();
```

### Retry with Bounded Attempts

The fluent `.RetryOn<TException>()` method on the builder accepts a max retry count and an exhausted decision:

```csharp
var policy = ResiliencePolicyBuilder
    .ForNode<MyTransform, Order>()
    .RetryOn<HttpRequestException>(maxRetries: 5, exhaustedDecision: ResilienceDecision.DeadLetter)
    .RetryWhen(ex => ex.Message.Contains("rate limit"), maxRetries: 10)
    .Otherwise(ResilienceDecision.Fail)
    .Build();
```

## Node-Scoped Policies

The fluent builder creates policies scoped to a specific node type and data type. If the failing node doesn't match the configured `TNode` and `TData`, the policy falls back to `Fail`. This lets you compose multiple policies for different nodes.

## Registering the Policy

```csharp
public void Define(PipelineBuilder builder, PipelineContext context)
{
    // Register a resilience policy (one per pipeline)
    builder.AddResiliencePolicy(policy);

    // Enable resilience on specific transform nodes
    var transform = builder.AddTransform<MyTransform, string, string>("transform");
    transform.WithResilience(builder);

    // ... connect nodes ...
}
```

> **Important:** Adding a resilience policy alone is not enough. You must also call `.WithResilience(builder)` on each transform node that should use it. Nodes without `.WithResilience()` use the default sequential execution strategy and will not invoke the policy.

## Next Steps

- [Retry Strategies](retry-strategies.md) - configure the delay between retry attempts
- [Circuit Breakers](circuit-breakers.md) - automatically stop retrying when a node is consistently failing
- [Dead-Letter Queues](dead-letter-queues.md) - inspect items that exhausted their retries

---
title: "Resilience policies"
description: "Decide in code what happens when an item, a stream, or a node fails, with IResiliencePolicy and the fluent builder."
order: 3
---

# Resilience policies

A resilience policy decides what happens when work fails. The runtime passes it a description of the failure, and the
policy returns a `ResilienceDecision`: retry, skip, dead-letter, fail, restart the node, or continue without it.

You don't need a policy to retry, skip, or dead-letter. The node's `PipelineResilienceOptions` already describe that,
and with no policy registered, `DefaultResiliencePolicy` carries them out. Write a policy when the decision depends on
something the options can't express, such as the exception's type, the item's content, or a count you keep yourself.

## How a policy decides

A policy owns the decision, and the runtime carries it out:

- **The policy decides whether to retry.** The node's options reach the policy as advice, through `MaxRetries` and
  `CanRetry` on the failure. The runtime doesn't override the answer. The one exception is a safety ceiling: a policy
  that answers `Retry` or `RestartNode` more than 100 times for the same unit of work fails the node with an
  `InvalidOperationException` that names the policy, rather than looping forever.
- **The layer's options decide how long to wait.** A retry waits for the layer's `RetryBackoff`, such as
  `ItemRetry.Backoff`. A policy has no say in the delay. For more information, see
  [Retry strategies](retry-strategies.md).
- **Running out of retries throws `RetryExhaustedException`.** When a layer stops after at least one retry, the node
  fails with a `RetryExhaustedException` that carries the node ID, the number of attempts, and the last failure as its
  inner exception. A failure that was never retried surfaces as it was thrown.

## The IResiliencePolicy interface

The interface, in the `NPipeline.Reliability` namespace, has one method for each of the
[three layers](three-layers.md):

```csharp
public interface IResiliencePolicy
{
    // L1: an item's transform failed.
    ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken);

    // L2: a transform node's output stream failed.
    ValueTask<ResilienceDecision> DecideRestartAsync(StreamFailure failure, CancellationToken cancellationToken);

    // L3: a node's execution failed.
    ValueTask<ResilienceDecision> DecideNodeFailureAsync(NodeFailure failure, CancellationToken cancellationToken);
}
```

Each method accepts only some decisions. The following table lists them:

| Method | Decisions it accepts |
| --- | --- |
| `DecideItemFailureAsync` | `Retry`, `Skip`, `DeadLetter`, `Fail` |
| `DecideRestartAsync` | `RestartNode`, `ContinueWithoutNode`, `Fail` |
| `DecideNodeFailureAsync` | `Retry`, `Fail` |

### What the policy is told

Each method receives a read-only struct that describes the failure. The following table lists the properties that
are most useful for making a decision:

| Struct | Property | Description |
| --- | --- | --- |
| `ItemFailure<TIn>` | `Item`, `Node`, `NodeId`, `Exception` | The item, the node, and what went wrong. |
| | `Attempt` | The 1-based number of the attempt that failed. |
| | `MaxRetries` | The node's `ItemRetry.MaxRetries`. |
| | `IsTransient` | Whether the node's `ItemRetry.Classifier` judged the failure transient. |
| | `IsBreakerOpen` | Whether an open [circuit breaker](circuit-breakers.md) refused the attempt, instead of the attempt being made and failing. |
| | `CanRetry` | `true` when the failure is transient, the breaker didn't refuse the attempt, and retries remain. |
| `StreamFailure` | `NodeId`, `Exception` | The node and what went wrong. |
| | `Attempt`, `MaxRestarts` | The 1-based number of the run that failed, and the node's `NodeRestart.MaxRestarts`. |
| | `Checkpoint` | The input index a restart resumes from. |
| | `Delivered` | The outputs the node has delivered so far, across all its runs. |
| | `CanRestart` | `true` when restarts remain. |
| `NodeFailure` | `Definition`, `Node`, `NodeId`, `Exception` | The node and what went wrong. |
| | `Attempt`, `MaxRetries`, `IsTransient` | As for `ItemFailure`, from the node's `NodeRetry` options. |
| | `InputConsumed` | Whether the node read input before it failed. A node that has read input isn't executed again, whatever the policy answers. |
| | `CanRetry` | `true` when the failure is transient, retries remain, and the node hasn't read input. |

Each struct also carries the `PipelineContext` as `Context`.

## The default policy

`DefaultResiliencePolicy` is the policy used when you don't register one. It follows the node's options and adds no
rules of its own:

- **Item failure:** `Retry` while `CanRetry` is `true`. Otherwise, the node's `OnItemFailure`: `Fail`, `Skip`, or
  `DeadLetter`. An attempt that an open breaker refused fails instead, even when `OnItemFailure` is `Skip` or
  `DeadLetter`, so that an outage doesn't skip or dead-letter the whole input.
- **Stream failure:** `RestartNode` while `CanRestart` is `true`, otherwise `Fail`.
- **Node failure:** `Retry` while `CanRetry` is `true`, otherwise `Fail`.

## Write a policy with ResiliencePolicyBase

To write a policy, derive from `ResiliencePolicyBase`. Each of its methods defaults to `DefaultResiliencePolicy`, so
you override only the decision you care about, and the rest follow the node's options.

The following policy dead-letters validation failures, and leaves every other failure to the node's options:

```csharp
using NPipeline.Reliability;

public sealed class DeadLetterInvalidOrders : ResiliencePolicyBase
{
    public override ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(
        ItemFailure<TIn> failure, CancellationToken cancellationToken)
    {
        return failure.Exception is ValidationException
            ? ValueTask.FromResult(ResilienceDecision.DeadLetter)
            : base.DecideItemFailureAsync(failure, cancellationToken);
    }
}
```

When a policy returns `Retry`, it should still respect the failure's advice. The following policy retries rate-limit
errors more times than the node's options allow, and otherwise defers to `CanRetry`:

```csharp
public sealed class PatientWithRateLimits : ResiliencePolicyBase
{
    public override ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(
        ItemFailure<TIn> failure, CancellationToken cancellationToken)
    {
        if (failure.Exception is HttpRequestException { StatusCode: HttpStatusCode.TooManyRequests } && failure.Attempt <= 10)
            return ValueTask.FromResult(ResilienceDecision.Retry);

        return base.DecideItemFailureAsync(failure, cancellationToken);
    }
}
```

A policy that returns `Retry` without reading `CanRetry`, `IsTransient`, `Attempt`, `MaxRetries`, or `IsBreakerOpen`
retries permanent failures, and retries against an open breaker. The [NP9205 analyzer](../analyzers/index.md) warns
about this.

### Decide restarts in code

`DecideRestartAsync` can choose `ContinueWithoutNode` for a node the pipeline can do without. The following policy
restarts a node after an I/O failure while restarts remain. For any other failure, it ends the node's stream and lets
the rest of the pipeline carry on, which suits an optional enrichment step:

```csharp
public sealed class RestartOrSkipEnrichment : ResiliencePolicyBase
{
    public override ValueTask<ResilienceDecision> DecideRestartAsync(
        StreamFailure failure, CancellationToken cancellationToken)
    {
        if (failure.Exception is IOException && failure.CanRestart)
            return ValueTask.FromResult(ResilienceDecision.RestartNode);

        return ValueTask.FromResult(ResilienceDecision.ContinueWithoutNode);
    }
}
```

`DecideRestartAsync` is called only for a transform whose `NodeRestart.MaxRestarts` is above zero, because only those
nodes are wrapped for restart. For more information, see [Node restart and the replay window](materialization.md).

## Register a policy

A pipeline has one policy. Register an instance, or a type that is resolved through dependency injection:

```csharp
builder.AddResiliencePolicy(new DeadLetterInvalidOrders());

// Or, resolved from the service provider:
builder.AddResiliencePolicy<DeadLetterInvalidOrders>();
```

To give one node its own policy, pass its handle:

```csharp
builder.AddResiliencePolicy(enrich, new PatientWithRateLimits());
```

The policy applies as soon as it's registered. There's nothing to enable on each node.

You can also supply a policy through the context instead of the builder, with
`PipelineContextConfiguration.WithResilience(policy)`. The full precedence at run time is:

1. The graph's policy instance (`builder.AddResiliencePolicy(policy)`).
2. The graph's policy type (`builder.AddResiliencePolicy<T>()`).
3. The context's policy (`PipelineContextConfiguration.WithResilience(policy)`).
4. `DefaultResiliencePolicy.Instance`.

A graph-level policy therefore takes precedence over a context-level one.

## The fluent policy builder

For rules that depend only on the exception type, `ResiliencePolicyBuilder`, in the `NPipeline.ErrorHandling`
namespace, builds a policy without a class:

```csharp
using NPipeline.ErrorHandling;
using NPipeline.Reliability;

// Retry timeouts up to three times, then dead-letter. Skip malformed records. Fail on anything else.
var policy = ResiliencePolicyBuilder
    .ForNode<ParseOrderTransform, string>()
    .On<TimeoutException>().Retry(maxRetries: 3)
    .On<FormatException>().Skip()
    .OnAny().Fail()
    .Build();

builder.AddResiliencePolicy(policy);
```

The builder's policies make item decisions only. Restart and node decisions follow the node's options.

The policy doesn't consult rules for an attempt the circuit breaker refused. Such an attempt fails the node, as it does with
`DefaultResiliencePolicy`, so `.OnAny().Skip()` cannot drain the input while a dependency is down. To wait out an
outage instead, use the breaker's opt-in `Pause` mode ([Circuit breakers](circuit-breakers.md)).

### Builder methods

The following methods add rules:

| Method | Effect |
| --- | --- |
| `.On<TException>()` | Matches an exception of type `TException`. |
| `.When(predicate)` | Matches an exception that satisfies `predicate`. |
| `.OnAny()` | Matches any exception. It must be the last rule. |
| `.Retry(maxRetries)` | Retries a matching failure up to `maxRetries` times, then dead-letters it. |
| `.Skip()` | Drops the item and continues. |
| `.DeadLetter()` | Sends the item to the dead-letter sink. |
| `.Fail()` | Fails the node. |
| `.RetryOn<TException>(maxRetries, exhaustedDecision)` | Retries `TException` up to `maxRetries` times, then returns `exhaustedDecision`. The default is `DeadLetter`. |
| `.RetryWhen(predicate, maxRetries, exhaustedDecision)` | As `RetryOn`, for exceptions that satisfy `predicate`. |
| `.RetryOnAny(maxRetries, exhaustedDecision)` | As `RetryOn`, for any exception. |
| `.Otherwise(decision)` | The decision when no rule matches. The default is `Fail`. |

A `Retry(n)` rule governs itself: the item is retried `n` times, even if the node's `ItemRetry.MaxRetries` is lower.
The builder's rules don't consult the classifier, so a rule like `.OnAny().Retry(3)` also retries permanent failures.
The delay between retries is still the node's `ItemRetry.Backoff`. With the HighThroughput profile, that's no delay,
so set a backoff when a builder policy retries.

### Rule order

Rules are checked in the order you add them, and the first match wins. A catch-all rule (`.OnAny()`) must be last.
Putting one before other rules makes `Build()` throw an `InvalidOperationException`:

```csharp
// Specific rules first, catch-all last.
var policy = ResiliencePolicyBuilder
    .ForNode<ParseOrderTransform, string>()
    .On<TimeoutException>().Retry(3)
    .On<FormatException>().Skip()
    .OnAny().DeadLetter()
    .Build();

// Throws InvalidOperationException: the catch-all makes the FormatException rule unreachable.
var invalid = ResiliencePolicyBuilder
    .ForNode<ParseOrderTransform, string>()
    .OnAny().Retry(3)
    .On<FormatException>().Skip()
    .Build();
```

### Node scope

A builder policy applies to one node type and one item type. A failure in any other node, or with an item of another
type, follows that node's options, as if no policy were registered.

### Shortcuts

`ResiliencePolicyBuilder` also has factory methods for the most common policies:

```csharp
// Retry every item failure up to three times, then dead-letter.
var retryAll = ResiliencePolicyBuilder.RetryAlways<ParseOrderTransform, string>(maxRetries: 3);

// Retry TimeoutException up to five times, then dead-letter. Fail on anything else.
var retryTimeouts = ResiliencePolicyBuilder.RetryOn<ParseOrderTransform, string, TimeoutException>(maxRetries: 5);

// Skip every item failure.
var skipAll = ResiliencePolicyBuilder.SkipAlways<ParseOrderTransform, string>();

// Dead-letter every item failure.
var deadLetterAll = ResiliencePolicyBuilder.DeadLetterAlways<ParseOrderTransform, string>();
```

## Dead-lettering without a sink

A policy that returns `DeadLetter` needs a dead-letter sink. If the pipeline has none, the node fails with a
`DeadLetterSinkNotConfiguredException` ([NP0424](../reference/error-codes.md)). The item is never dropped silently.
To add a sink, call `AddDeadLetterSink`. For more information, see [Dead-letter queues](dead-letter-queues.md).

## Next steps

- [The three resilience layers](three-layers.md): what each decision method covers.
- [Retry strategies](retry-strategies.md): how long each layer waits, and which failures are transient.
- [Circuit breakers](circuit-breakers.md): decide what happens to an attempt the breaker refused.
- [Dead-letter queues](dead-letter-queues.md): inspect the items a policy dead-lettered.

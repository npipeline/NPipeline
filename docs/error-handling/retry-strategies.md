---
title: "Retry strategies"
description: "Choose how long each resilience layer waits between attempts, and which failures are worth retrying."
order: 4
---

# Retry strategies

A retry strategy has two parts: a *backoff*, which sets how long to wait before each retry, and a *classifier*, which
decides whether a failure is worth retrying at all. Each of the [three resilience layers](three-layers.md) has its own
of each, so a node can retry items quickly and restart slowly.

## Quick start

The following example retries each failed item up to five times, starting at 500 milliseconds and doubling each time,
up to 30 seconds:

```csharp
using NPipeline.Reliability;

builder.WithResilience(options => options with
{
    ItemRetry = options.ItemRetry with
    {
        MaxRetries = 5,
        Backoff = RetryBackoff.Exponential(TimeSpan.FromMilliseconds(500), maxDelay: TimeSpan.FromSeconds(30)),
    },
});
```

With the Default [optimization profile](../guides/optimization-profiles.md), you don't need any configuration to
retry: transient item failures are retried three times, with exponential backoff from 200 milliseconds up to 30
seconds and full jitter. That's `ItemRetryOptions.Default`. The HighThroughput profile retries nothing.

## Where the backoff is set

Each layer's options record has a `Backoff` property, of type `RetryBackoff`:

| Layer | Property | Default |
| --- | --- | --- |
| L1: item retry | `ItemRetry.Backoff` | No delay. `ItemRetryOptions.Default`, used by the Default profile, is exponential from 200 ms up to 30 s, with full jitter |
| L2: node restart | `NodeRestart.Backoff` | Exponential from 1 s up to 30 s, with full jitter |
| L3: node retry | `NodeRetry.Backoff` | Exponential from 1 s up to 30 s, with full jitter |

Node restart and node retry run a whole node again, so their default waits are longer, and `new NodeRestartOptions {
MaxRestarts = 3 }` doesn't restart in a tight loop against a dependency that's down. In tests, set
`Backoff = RetryBackoff.None`, or put a fake clock in `PipelineResilienceOptions.Time`.

Every delay waits on the clock in `PipelineResilienceOptions.Time`, and cancelling the pipeline ends the wait at once.

A backoff belongs to the node's options, and it depends only on the retry number. Nothing is shared between items or
nodes: two items that fail at the same time each wait their own delay.

## Backoff curves

`RetryBackoff` is a value type with a factory method for each curve. The *retry number* is 1-based: retry 1 is the
first retry after the first attempt failed.

| Factory method | Delay before retry *n* | Default jitter |
| --- | --- | --- |
| `RetryBackoff.None` | Zero | None |
| `RetryBackoff.Constant(delay)` | `delay` | None |
| `RetryBackoff.Linear(step, maxDelay)` | `step` × *n* | Equal |
| `RetryBackoff.Exponential(baseDelay, factor, maxDelay)` | `baseDelay` × `factor`^(*n* - 1) | Full |
| `RetryBackoff.Custom(delayForRetry)` | Whatever `delayForRetry(n)` returns | Never jittered |

The exponential factor defaults to 2. `maxDelay` caps the delay, and it defaults to no cap. Each method also takes a
`jitter` argument to override the default.

The following table shows the delays, before jitter, for three curves:

| Retry | `Constant(1s)` | `Linear(1s)` | `Exponential(1s, maxDelay: 5s)` |
| --- | --- | --- | --- |
| 1 | 1 s | 1 s | 1 s |
| 2 | 1 s | 2 s | 2 s |
| 3 | 1 s | 3 s | 4 s |
| 4 | 1 s | 4 s | 5 s |

To compute a delay yourself, call `DelayFor`. For example, `RetryBackoff.Linear(TimeSpan.FromSeconds(1)).DelayFor(3)`
returns a delay between 1.5 and 3 seconds, because linear backoff has equal jitter by default.

### Adjust a curve

`RetryBackoff` is a record struct, so you can derive a variant with a `with` expression:

```csharp
var backoff = RetryBackoff.Exponential(TimeSpan.FromMilliseconds(200), maxDelay: TimeSpan.FromSeconds(30));
var steeper = backoff with { Factor = 3 };
var predictable = backoff with { Jitter = RetryJitter.None };
```

Each property rejects an out-of-range value when it's set, so a bad value throws `ArgumentOutOfRangeException` where
you write it: a negative delay, a factor below 1, or an undefined kind or jitter. This applies to the factory methods
and to `with` expressions alike.

Some combinations are checked only as a whole, for example an exponential kind with no factor, or a custom kind with
no function. Building the pipeline checks every node's options again, and catches these. To check a value yourself,
call `Validate()`.

### Custom curves

For a curve the factory methods don't cover, pass a function of the retry number:

```csharp
// 100 ms, 100 ms, 200 ms, 300 ms, 500 ms, ...: a Fibonacci backoff.
var fibonacci = RetryBackoff.Custom(retry => TimeSpan.FromMilliseconds(100 * Fibonacci(retry)));
```

A custom delay isn't jittered or capped, and a negative result counts as zero.

## Jitter

Without jitter, items that fail at the same moment all retry at the same moment, and hit a recovering service together.
Jitter spreads the retries out. `RetryJitter` has three values:

| Value | Delay | Use it for |
| --- | --- | --- |
| `None` | The computed delay | Tests, and waits that must be predictable |
| `Full` | A random delay between zero and the computed delay | The most spread. The default for exponential backoff |
| `Equal` | Half the computed delay, plus a random delay up to the other half | Spread with a guaranteed minimum wait. The default for linear backoff |

Jitter never makes a delay longer than `MaxDelay`.

## Which failures are retried

A layer retries a failure only when its classifier judges it *transient*: a failure that might not happen again,
such as a timeout. A *permanent* failure, such as a malformed record or a programming error, fails at once, because
retrying it only wastes time.

Each layer that retries has a `Classifier` property, of type `RetryClassifier`: `ItemRetry.Classifier` and
`NodeRetry.Classifier`. Node restart doesn't use a classifier.

### The default classifier

`RetryClassifier.Default` treats the following exceptions as transient:

- `TimeoutException`, `IOException`, and `SocketException`.
- `HttpRequestException` with no status code, or with status 408, 429, or 5xx.
- `DbException` when its `IsTransient` property is `true`.
- `TaskCanceledException` that the pipeline's own cancellation token didn't cause, such as an `HttpClient` timeout.

It treats everything else as permanent, including the following:

- Cancellation of the pipeline's own token. No classifier can make this transient.
- `RetryExhaustedException`, so that one layer doesn't retry another layer's exhausted retries.
- Any exception that [NResilience](three-layers.md#how-connector-and-pipeline-retries-compose) has already retried. A
  connector's retries aren't multiplied by the pipeline's.

Before it judges a failure, the classifier looks inside the exceptions that the pipeline wraps failures in:
`NodeExecutionException`, `PipelineExecutionException`, `TargetInvocationException`, and an `AggregateException` with a
single inner exception.

To retry every failure except the pipeline's own cancellation, use `RetryClassifier.All`.

### Add your own rules

A classifier is immutable. `Transient<TException>()` and `Permanent<TException>()` each return a new classifier with
one more rule. Your rules are checked before the built-in ones, in the order you add them. A rule can take a condition:

```csharp
builder.WithResilience(options => options with
{
    ItemRetry = options.ItemRetry with
    {
        Classifier = RetryClassifier.Default
            .Transient<SqlException>(e => e.Number is 1205 or 40501)                                // Deadlock, throttled
            .Permanent<HttpRequestException>(e => e.StatusCode == HttpStatusCode.NotImplemented),   // A 501 won't change
    },
});
```

A rule matches the exception or any wrapped exception as described in the preceding section.

To check a classifier, for example in a unit test, call `IsTransient`:

```csharp
var classifier = RetryClassifier.Default.Transient<SqlException>(e => e.Number is 1205);
bool retried = classifier.IsTransient(new TimeoutException()); // true
```

Outside a pipeline there's no pipeline token, so you can omit it. A `TaskCanceledException` then counts as a client
timeout, which is transient.

## Choose a strategy

The following table suggests a strategy for common situations:

| Situation | Backoff | Jitter |
| --- | --- | --- |
| Calls to a shared service from many items or pipelines | Exponential | Full (the default) |
| A predictable wait with some spread | Linear | Equal (the default) |
| A known cooldown, such as a lock timeout | Constant | None |
| Node restart or node retry, where each attempt is expensive | Exponential with a base of a second or more | Full |
| Tests | Constant or `None` | None |

For calls to an HTTP API, consider retrying inside the node with NResilience instead, which also honors
`Retry-After`. For more information, see [Call external APIs from a transform](three-layers.md#call-external-apis-from-a-transform).

## Monitor retries

Every retry at every layer raises `IExecutionObserver.OnRetry` with a `NodeRetryEvent`. The event's `Kind` says which
layer retried: `RetryKind.ItemRetry`, `RetryKind.NodeRestart`, or `RetryKind.NodeRetry`. When a layer gives up after
retrying, it raises `OnRetryExhausted` with a `RetryExhaustedEvent` before it throws `RetryExhaustedException`.

The Observability extension counts these events for you. For more information, see
[Observability](../extensions/observability.md).

The following signs suggest that a strategy needs attention:

- **High retry rate:** If more than a few percent of items are retried, failures are likely systematic. Fix the cause or classify as permanent.
- **Frequent exhaustion:** Items using all retries fail for longer than the backoff waits. Consider a [circuit breaker](circuit-breakers.md) with `WhenOpen = Pause`, or dead-letter the items sooner.
- **High latency:** If retry delays dominate latency, lower `MaxDelay` or make fewer retries.

## Test retry behavior

To test retries without waiting for real delays, set `Time` to a `FakeTimeProvider` from the
`Microsoft.Extensions.TimeProvider.Testing` package, and advance it in the test:

```csharp
var time = new FakeTimeProvider();

builder.WithResilience(options => options with
{
    ItemRetry = options.ItemRetry with { MaxRetries = 3, Backoff = RetryBackoff.Constant(TimeSpan.FromSeconds(10)) },
    Time = time,
});

// ... start the run ...

time.Advance(TimeSpan.FromSeconds(10)); // The first retry happens now.
```

To test a backoff curve on its own, turn jitter off and call `DelayFor`:

```csharp
[Fact]
public void Exponential_backoff_doubles_up_to_the_cap()
{
    var backoff = RetryBackoff.Exponential(
        TimeSpan.FromMilliseconds(100), maxDelay: TimeSpan.FromMilliseconds(500), jitter: RetryJitter.None);

    backoff.DelayFor(1).Should().Be(TimeSpan.FromMilliseconds(100));
    backoff.DelayFor(2).Should().Be(TimeSpan.FromMilliseconds(200));
    backoff.DelayFor(3).Should().Be(TimeSpan.FromMilliseconds(400));
    backoff.DelayFor(4).Should().Be(TimeSpan.FromMilliseconds(500));
}
```

For a runnable tour of the backoff curves, see the
[`Sample_RetryDelay`](https://github.com/NPipeline/NPipeline/tree/main/samples/Sample_RetryDelay) sample.

## Next steps

- [The three resilience layers](three-layers.md): what each layer retries.
- [Resilience policies](resilience-policies.md): decide in code whether to retry.
- [Circuit breakers](circuit-breakers.md): stop retrying a dependency that keeps failing.
- [Dead-letter queues](dead-letter-queues.md): capture the items that still fail.

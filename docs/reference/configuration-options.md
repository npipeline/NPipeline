---
title: "Configuration Options"
description: "Complete reference for all NPipeline runtime configuration records and their properties."
order: 3
---

# Configuration Options

This page lists every configuration record in NPipeline with its properties, types, and defaults. Use this as a reference when configuring pipeline behavior.

## PipelineResilienceOptions

Controls the three resilience layers that can repeat work, the circuit breaker, and what happens to an item that isn't
retried. Configure the pipeline's options with `builder.WithResilience(configure)`, and one node's options with
`builder.WithResilience(handle, configure)`. Each call derives new options from the ones passed in, normally with
`with`.

**Namespace:** `NPipeline.Reliability`

```csharp
builder.WithResilience(options => options with
{
    ItemRetry = ItemRetryOptions.Default with { MaxRetries = 5 },
    OnItemFailure = ItemFailureAction.DeadLetter,
});

builder.WithResilience(transform, options => options with
{
    NodeRestart = new NodeRestartOptions { MaxRestarts = 3 },
});
```

The pipeline's options start from the optimization profile's defaults. Under the `Default` profile, `ItemRetry` is
`ItemRetryOptions.Default`, which retries transient failures three times. Under the `HighThroughput` profile, the
options start from `PipelineResilienceOptions.None`, which retries nothing. A node's options start from the pipeline's.

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `ItemRetry` | `ItemRetryOptions` | `ItemRetryOptions.None` | Item retry (L1): how many times one item's transform is retried, and the wait between attempts. Transform nodes only. |
| `NodeRestart` | `NodeRestartOptions` | `NodeRestartOptions.None` | Node restart (L2): how many times a failed transform stream restarts from its checkpoint. Transform nodes only. |
| `NodeRetry` | `NodeRetryOptions` | `NodeRetryOptions.None` | Node retry (L3): how many times a node that failed during setup, before it consumed any input, executes again. Any node. |
| `CircuitBreaker` | `CircuitBreakerOptions?` | `null` | The breaker that guards each item attempt. `null` means no breaker. Transform nodes only. |
| `OnItemFailure` | `ItemFailureAction` | `Fail` | What happens to an item whose failure isn't retried: `Fail` fails the node, `Skip` drops the item, and `DeadLetter` sends it to the dead-letter sink. |
| `Time` | `TimeProvider` | `TimeProvider.System` | The clock that every retry delay waits on. Substitute a fake clock in tests to avoid real delays. |

Static members: `PipelineResilienceOptions.None`, `PipelineResilienceOptions.ForProfile(profile)`.

The defaults in these options describe what the default resilience policy does. A policy that you register with
`AddResiliencePolicy` makes each decision and can depart from them.

The builder reports these configurations as build errors:

- `ItemRetry`, `NodeRestart`, or `CircuitBreaker` set for a node that isn't a transform, such as a source, sink,
  aggregate, or join. Use `NodeRetry` for those nodes instead.
- `NodeRestart.MaxRestarts` above zero on a transform whose execution strategy doesn't implement
  `IResumableExecutionStrategy` (`NP0425`).

If a transform's `OnItemFailure` is `ItemFailureAction.DeadLetter` and the pipeline has no dead-letter sink, the run
fails with a `DeadLetterSinkNotConfiguredException` (`NP0424`) before any node starts. Add a sink with
`AddDeadLetterSink`.

For more information, see [The three resilience layers](../error-handling/three-layers.md).

## ItemRetryOptions

Item retry (L1) retries one item's transform in a transform node. When an item's retries run out, the item's failure
is handled as `OnItemFailure` specifies.

**Namespace:** `NPipeline.Reliability`

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `MaxRetries` | `int` | `0` | Retries after the first attempt. An item is attempted at most `MaxRetries + 1` times. |
| `Backoff` | `RetryBackoff` | `RetryBackoff.None` | The wait before each retry. |
| `Classifier` | `RetryClassifier` | `RetryClassifier.Default` | Which failures are worth retrying. The default classifier retries transient failures only. |

Static members:

- `ItemRetryOptions.None`: no item is retried.
- `ItemRetryOptions.Default`: three retries of transient failures, with exponential backoff from 200 ms up to 30 sec
  and full jitter. The `Default` optimization profile uses these options.

## NodeRestartOptions

Node restart (L2) restarts a transform node's output stream after it fails. The restart resumes at the node's
checkpoint, the first input item whose outcome wasn't delivered, so items before the checkpoint aren't processed
again. Setting `MaxRestarts` above zero is all that's needed; the builder wraps the node for restart when you build
the pipeline.

**Namespace:** `NPipeline.Reliability`

```csharp
builder.WithResilience(transform, options => options with
{
    NodeRestart = new NodeRestartOptions { MaxRestarts = 3, MaxReplayWindow = 50_000 },
});
```

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `MaxRestarts` | `int` | `0` | Restarts after the first run. A stream runs at most `MaxRestarts + 1` times. |
| `Backoff` | `RetryBackoff` | Exponential, 1 sec up to 30 sec, full jitter | The wait before each restart. |
| `MaxReplayWindow` | `int` | `10,000` | The most input items the node holds so that a restart can process them again. When the window is full, the node stops reading its input until the checkpoint advances. This limit is backpressure, never an error, and it doesn't limit the length of the input. |
| `ResetAfterItems` | `int?` | `null` | When set, the restart count starts again after this many outputs are delivered following a restart. `null` means the count never resets. |

Static member: `NodeRestartOptions.None`.

For more information, see [Node restart and the replay window](../error-handling/materialization.md).

## NodeRetryOptions

Node retry (L3) executes a failed node again. It covers setup failures only: a node that fails before it consumed any
input, such as a source that can't open its connection. A node that fails after it consumed input isn't retried,
because executing it again would process that input twice. To recover a transform that fails mid-stream, use node
restart.

**Namespace:** `NPipeline.Reliability`

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `MaxRetries` | `int` | `0` | Retries after the first execution. A node executes at most `MaxRetries + 1` times. |
| `Backoff` | `RetryBackoff` | Exponential, 1 sec up to 30 sec, full jitter | The wait before each retry. |
| `Classifier` | `RetryClassifier` | `RetryClassifier.Default` | Which failures are worth retrying. The default classifier retries transient failures only. |

Static member: `NodeRetryOptions.None`.

When a layer's retries run out, the node fails with a `RetryExhaustedException`.

## RetryBackoff

A value type that computes the wait before each retry from the retry number. Create one with a factory method, and
adjust it with `with`.

**Namespace:** `NPipeline.Reliability`

```csharp
var backoff = RetryBackoff.Exponential(TimeSpan.FromMilliseconds(200), maxDelay: TimeSpan.FromSeconds(30));
var slower = backoff with { Factor = 3 };
```

The following factory members create a backoff:

| Member | Delay before retry *n* | Default jitter |
| --- | --- | --- |
| `RetryBackoff.None` | None | Not applicable |
| `RetryBackoff.Constant(delay, jitter)` | `delay` | `None` |
| `RetryBackoff.Linear(step, maxDelay, jitter)` | `step` × *n* | `Equal` |
| `RetryBackoff.Exponential(baseDelay, factor, maxDelay, jitter)` | `baseDelay` × `factor`^(*n* - 1), with `factor` defaulting to `2` | `Full` |
| `RetryBackoff.Custom(delayForRetry)` | The value that `delayForRetry(n)` returns | Not applied |

| Property | Type | Description |
| --- | --- | --- |
| `Kind` | `RetryBackoffKind` | The shape of the curve: `None`, `Constant`, `Linear`, `Exponential`, or `Custom`. |
| `BaseDelay` | `TimeSpan` | The delay before the first retry, and the step for `Linear`. |
| `Factor` | `double` | The multiplier between consecutive delays for `Exponential`. Must be at least 1. |
| `MaxDelay` | `TimeSpan` | The longest delay, applied after jitter. `TimeSpan.Zero` means no cap. |
| `Jitter` | `RetryJitter` | How the delay is randomized: `None`, `Full` (between zero and the delay), or `Equal` (half the delay plus up to the other half). |
| `CustomDelay` | `Func<int, TimeSpan>?` | Computes the delay from the 1-based retry number, for `Custom`. |

To compute a delay yourself, call `DelayFor(retry)` with the 1-based retry number.

For more information, see [Retry strategies](../error-handling/retry-strategies.md).

## CircuitBreakerOptions

Controls a transform node's circuit breaker, which guards each item attempt. Set it through
`PipelineResilienceOptions.CircuitBreaker`, for the pipeline or for one node.

**Namespace:** `NPipeline.Reliability`

```csharp
builder.WithResilience(transform, options => options with
{
    CircuitBreaker = new CircuitBreakerOptions { ConsecutiveFailures = 5, OpenDuration = TimeSpan.FromMinutes(1) },
});
```

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `ConsecutiveFailures` | `int?` | `5` | Trip after this many transient failures in a row. `null` disables the condition. |
| `FailureRate` | `double?` | `null` | Trip when this fraction of attempts in `Window` failed transiently (greater than 0, at most 1). |
| `MinimumCalls` | `int` | `20` | The fewest attempts in `Window` before `FailureRate` is considered. |
| `Window` | `TimeSpan` | 30 sec | The period `FailureRate` is measured over. |
| `OpenDuration` | `TimeSpan` | 30 sec | How long the breaker stays open before it lets a probe through. |
| `HalfOpenProbes` | `int` | `1` | The most probes in flight at once while half-open. |
| `ProbeSuccesses` | `int` | `1` | The successful probes needed to close the breaker. |
| `WhenOpen` | `BreakerOpenBehavior` | `Fail` | `Fail` (the default) fails a refused attempt. `Pause` (opt-in) waits for the breaker, up to `MaxPause`. |
| `MaxPause` | `TimeSpan` | 5 min | With `Pause`, the longest one attempt waits before it fails. |

Static member: `CircuitBreakerOptions.Default`. At least one of `ConsecutiveFailures` and `FailureRate` must be set.

Breakers live as long as the `PipelineFactory` that built the pipeline, so their state carries over between runs.

See [Circuit Breakers](../error-handling/circuit-breakers.md) for usage guidance.

## ErrorHandlingConfiguration

Aggregates all error handling settings. Typically configured indirectly through `PipelineBuilder` methods rather than instantiated directly.

**Namespace:** `NPipeline.Configuration`

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ResiliencePolicy` | `IResiliencePolicy?` | `null` | Resilience policy instance. Set via `builder.AddResiliencePolicy()`. |
| `ResiliencePolicyType` | `Type?` | `null` | Resilience policy type for DI resolution. Set via `builder.AddResiliencePolicy<T>()`. |
| `DeadLetterSink` | `IDeadLetterSink?` | `null` | Dead-letter sink instance. Set via `builder.AddDeadLetterSink()`. |
| `DeadLetterSinkType` | `Type?` | `null` | Dead-letter sink type for DI resolution. Set via `builder.AddDeadLetterSink<T>()`. |
| `Resilience` | `PipelineResilienceOptions?` | `null` | The pipeline's resilience options, with the optimization profile's defaults applied. Set via `builder.WithResilience(configure)`. |
| `NodeResilience` | `ImmutableDictionary<string, PipelineResilienceOptions>?` | `null` | Per-node resilience options keyed by node ID, each derived from `Resilience`. Set via `builder.WithResilience(handle, configure)`. |

## LineageOptions

Controls data lineage tracking behavior. Configure via `builder.EnableItemLevelLineage()`.

**Namespace:** `NPipeline.Configuration`

```csharp
builder.EnableItemLevelLineage(options => options with
{
    SampleEvery = 50,
    CaptureHopTimestamps = true,
    RedactData = true
});
```

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `Strict` | `bool` | `false` | Throw on lineage mismatch (vs. log a warning). |
| `WarnOnMismatch` | `bool` | `true` | Log a warning when lineage data is inconsistent. |
| `OnMismatch` | `Action<LineageMismatchContext>?` | `null` | Custom callback for mismatch events. |
| `MaterializationCap` | `int?` | `null` | Max items to materialize for lineage tracking. |
| `OverflowPolicy` | `OverflowPolicy` | `Degrade` | What to do when materialization cap is exceeded. |
| `CaptureHopTimestamps` | `bool` | `true` | Record per-hop enter/exit timestamps. |
| `CaptureDecisions` | `bool` | `true` | Record decision outcomes (Emitted, FilteredOut, Joined, etc.). |
| `CaptureObservedCardinality` | `bool` | `true` | Record Zero/One/Many cardinality and counts. |
| `CaptureAncestryMapping` | `bool` | `false` | Record full ancestry mapping when materialization allows. |
| `CaptureHopSnapshots` | `bool` | `false` | Record per-hop input/output snapshots. |
| `SampleEvery` | `int` | `100` | Deterministic sampling rate (1 in N items). |
| `DeterministicSampling` | `bool` | `true` | Use CorrelationId hashing for consistent sampling. |
| `RedactData` | `bool` | `true` | Omit actual item payload data in lineage events. |
| `MaxHopRecordsPerItem` | `int` | `256` | Maximum hop records retained per item. |
| `EnsurePerInputTerminalRecord` | `bool` | `true` | Ensure a terminal lineage record exists for every input item. |
| `EmitBackpressureDropRecords` | `bool` | `true` | Emit lineage records when items are dropped due to backpressure. |
| `IncludeContributorCorrelationIds` | `bool` | `true` | Include contributor correlation IDs in many-to-one scenarios. |
| `EmitIntermediateNodeRecords` | `bool` | `true` | Emit records for non-terminal (intermediate) nodes. |

Static members: `LineageOptions.CompleteLineage`

## AggregateNodeConfiguration\<TIn>

Controls windowed aggregation behavior. Passed when configuring aggregate nodes.

**Namespace:** `NPipeline.Configuration`

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `WindowAssigner` | `WindowAssigner` | *(required)* | Window strategy (tumbling, sliding). Window sizes and slides must be positive. |
| `TimestampExtractor` | `TimestampExtractor<TIn>?` | `null` | Extracts event time from items. `null` = use arrival time for items that are not `ITimestamped`. |
| `MaxOutOfOrderness` | `TimeSpan?` | 5 min | Grace period for late-arriving events. |
| `WatermarkInterval` | `TimeSpan?` | 30 sec | How often watermarks advance. |

## PipelineContextConfiguration

Configures the initial state of `PipelineContext` before pipeline execution.

**Namespace:** `NPipeline.Configuration`

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `Parameters` | `IDictionary<string, object>?` | `null` | Read-only parameters available to all nodes. |
| `Items` | `IDictionary<string, object>?` | `null` | Mutable shared state available to all nodes. |
| `Properties` | `IDictionary<string, object>?` | `null` | Additional properties. |
| `ErrorHandlerFactory` | `IErrorHandlerFactory?` | `null` | Factory for error handling services. |
| `ResiliencePolicy` | `IResiliencePolicy?` | `null` | Resilience policy. |
| `DeadLetterSink` | `IDeadLetterSink?` | `null` | Dead-letter sink. |
| `LoggerFactory` | `ILoggerFactory?` | `null` | Logger factory for structured logging. |
| `Tracer` | `IPipelineTracer?` | `null` | Tracer for OpenTelemetry integration. |
| `ObservabilityFactory` | `IObservabilityFactory?` | `null` | Factory for observability surfaces. |
| `LineageFactory` | `ILineageFactory?` | `null` | Factory for lineage tracking. |
| `OptimizationProfile` | `PipelineOptimizationProfile` | `Default` | The optimization profile for the run. |
| `CancellationToken` | `CancellationToken` | `None` | Cancellation token for the execution. |

Static members: `PipelineContextConfiguration.Default`, `WithParameters(parameters)`, `WithCancellation(token)`, `WithLogging(loggerFactory)`, `WithObservability(loggerFactory, tracer)`, `WithErrorHandling(deadLetterSink)`, `WithResilience(policy)`, `WithFactories(...)`

Resilience options aren't part of this record. Set them on the pipeline builder with `WithResilience`.

The system uses the `ResiliencePolicy` set through `PipelineContextConfiguration.WithResilience(policy)` when the graph doesn't
configure one. The full precedence at run time is: the graph's policy instance, then the graph's policy type, then the
context policy, then `DefaultResiliencePolicy.Instance`.

## Next Steps

- [Error Handling](../error-handling/index.md) - how retry, circuit breaker, and dead-letter options work together
- [The three resilience layers](../error-handling/three-layers.md) - item retry, node restart, and node retry
- [Retry Strategies](../error-handling/retry-strategies.md) - configuring backoff, jitter, and the retry classifier
- [Glossary](glossary.md) - definitions for terms used in configuration

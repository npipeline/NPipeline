---
title: "Configuration Options"
description: "Complete reference for all NPipeline runtime configuration records and their properties."
order: 3
---

# Configuration Options

This page lists every configuration record in NPipeline with its properties, types, and defaults. Use this as a reference when configuring pipeline behavior.

## PipelineRetryOptions

Controls per-item retries, node restarts, and retry delay strategies. Configure via `builder.WithRetryOptions()`.

**Namespace:** `NPipeline.Configuration`

```csharp
builder.WithRetryOptions(options => options with
{
    MaxItemRetries = 3,
    MaxMaterializedItems = 1000,
    MaxNodeRestartAttempts = 2
});
```

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `MaxItemRetries` | `int` | `0` | Maximum retries per item before dead-lettering. 0 means no retry. |
| `MaxMaterializedItems` | `int?` | `null` | Buffer cap for materialization. `null` = unbounded (no cap). Required for node restart on streaming inputs. |
| `DelayStrategyConfiguration` | `RetryDelayStrategyConfiguration?` | `null` | Backoff + jitter configuration. `null` = no delay between retries. |
| `MaxNodeRestartAttempts` | `int` | `3` | Maximum node restart attempts after failure. |
| `MaxSequentialNodeAttempts` | `int` | `5` | Maximum sequential node execution attempts before giving up. |

Static members: `PipelineRetryOptions.Default`

## RetryDelayStrategyConfiguration

Combines a backoff algorithm with an optional jitter strategy. Set via `PipelineRetryOptions.DelayStrategyConfiguration` or the convenience extension methods.

**Namespace:** `NPipeline.Configuration.RetryDelay`

```csharp
new RetryDelayStrategyConfiguration(
    BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1)),
    JitterStrategies.FullJitter())
```

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `BackoffStrategy` | `BackoffStrategy` | *(required)* | Delegate that calculates base delay from attempt number. |
| `JitterStrategy` | `JitterStrategy?` | `null` | Delegate that randomizes the base delay. `null` = no jitter. |

See [Retry Strategies](../error-handling/retry-strategies.md) for all built-in backoff and jitter options.

## PipelineCircuitBreakerOptions

Controls the circuit breaker that prevents cascading failures. Configure via `builder.WithCircuitBreaker()`.

**Namespace:** `NPipeline.Configuration`

```csharp
builder.WithCircuitBreaker(
    failureThreshold: 5,
    openDuration: TimeSpan.FromMinutes(1),
    samplingWindow: TimeSpan.FromMinutes(5));
```

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `FailureThreshold` | `int` | `5` | Failures before tripping the breaker (must be ≥ 1). |
| `OpenDuration` | `TimeSpan` | 1 min | How long the breaker stays open before transitioning to half-open. |
| `SamplingWindow` | `TimeSpan` | 5 min | Rolling window for failure tracking. |
| `Enabled` | `bool` | `true` | Whether the circuit breaker is active. |
| `ThresholdType` | `CircuitBreakerThresholdType` | `ConsecutiveFailures` | How failures are counted: `ConsecutiveFailures`, `RollingWindowCount`, or `RollingWindowRate`. |
| `FailureRateThreshold` | `double` | `0.5` | Failure rate (0.0–1.0) for rate-based threshold types. |
| `HalfOpenSuccessThreshold` | `int` | `1` | Consecutive successes needed in half-open to close. |
| `HalfOpenMaxAttempts` | `int` | `5` | Maximum attempts allowed in half-open state. |
| `TrackOperationsInWindow` | `bool` | `true` | Whether to track operations in the rolling window for statistics. |

Static members: `PipelineCircuitBreakerOptions.Default`, `PipelineCircuitBreakerOptions.Disabled`

See [Circuit Breakers](../error-handling/circuit-breakers.md) for usage guidance.

## CircuitBreakerMemoryManagementOptions

Controls automatic cleanup of per-node circuit breaker instances. Configure via `builder.ConfigureCircuitBreakerMemoryManagement()`.

**Namespace:** `NPipeline.Configuration`

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `CleanupInterval` | `TimeSpan` | 5 min | How often the cleanup process runs. |
| `InactivityThreshold` | `TimeSpan` | 30 min | How long a breaker must be inactive before removal. |
| `EnableAutomaticCleanup` | `bool` | `true` | Whether automatic cleanup is enabled. |
| `MaxTrackedCircuitBreakers` | `int` | `1000` | Maximum number of tracked breakers. |
| `CleanupTimeout` | `TimeSpan` | 30 sec | Timeout for each cleanup operation. |

Static members: `CircuitBreakerMemoryManagementOptions.Default`, `CircuitBreakerMemoryManagementOptions.Disabled`

## ErrorHandlingConfiguration

Aggregates all error handling settings. Typically configured indirectly through `PipelineBuilder` methods rather than instantiated directly.

**Namespace:** `NPipeline.Configuration`

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ResiliencePolicy` | `IResiliencePolicy?` | `null` | Resilience policy instance. Set via `builder.AddResiliencePolicy()`. |
| `ResiliencePolicyType` | `Type?` | `null` | Resilience policy type for DI resolution. Set via `builder.AddResiliencePolicy<T>()`. |
| `DeadLetterSink` | `IDeadLetterSink?` | `null` | Dead-letter sink instance. Set via `builder.AddDeadLetterSink()`. |
| `DeadLetterSinkType` | `Type?` | `null` | Dead-letter sink type for DI resolution. Set via `builder.AddDeadLetterSink<T>()`. |
| `RetryOptions` | `PipelineRetryOptions?` | `null` | Global retry options. |
| `NodeRetryOverrides` | `ImmutableDictionary<string, PipelineRetryOptions>?` | `null` | Per-node retry option overrides keyed by node ID. |
| `CircuitBreakerOptions` | `PipelineCircuitBreakerOptions?` | `null` | Circuit breaker configuration. |
| `CircuitBreakerMemoryOptions` | `CircuitBreakerMemoryManagementOptions?` | `null` | Circuit breaker memory management. |

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
| `WindowAssigner` | `WindowAssigner` | *(required)* | Window strategy (tumbling, sliding, session). |
| `TimestampExtractor` | `TimestampExtractor<TIn>?` | `null` | Extracts event time from items. `null` = use system arrival time. |
| `MaxOutOfOrderness` | `TimeSpan?` | 5 min | Grace period for late-arriving events. |
| `WatermarkInterval` | `TimeSpan?` | 30 sec | How often watermarks advance. |
| `UseThreadSafeAccumulator` | `bool` | `true` | Use `ConcurrentDictionary` vs. `Dictionary` for accumulation. |

## PipelineContextConfiguration

Configures the initial state of `PipelineContext` before pipeline execution.

**Namespace:** `NPipeline.Configuration`

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `Parameters` | `Dictionary<string, object>?` | `null` | Read-only parameters available to all nodes. |
| `Items` | `Dictionary<string, object>?` | `null` | Mutable shared state available to all nodes. |
| `Properties` | `Dictionary<string, object>?` | `null` | Additional properties. |
| `RetryOptions` | `PipelineRetryOptions?` | `null` | Retry options (typically set via builder). |
| `ErrorHandlerFactory` | `IErrorHandlerFactory?` | `null` | Factory for error handling services. |
| `ResiliencePolicy` | `IResiliencePolicy?` | `null` | Resilience policy. |
| `DeadLetterSink` | `IDeadLetterSink?` | `null` | Dead-letter sink. |
| `LoggerFactory` | `ILoggerFactory?` | `null` | Logger factory for structured logging. |
| `Tracer` | `IPipelineTracer?` | `null` | Tracer for OpenTelemetry integration. |
| `ObservabilityFactory` | `IObservabilityFactory?` | `null` | Factory for observability surfaces. |
| `LineageFactory` | `ILineageFactory?` | `null` | Factory for lineage tracking. |
| `CancellationToken` | `CancellationToken` | `None` | Cancellation token for the execution. |

Static members: `PipelineContextConfiguration.Default`, `PipelineContextConfiguration.WithCancellation(token)`

## Next Steps

- [Error Handling](../error-handling/index.md) — how retry, circuit breaker, and dead-letter options work together
- [Retry Strategies](../error-handling/retry-strategies.md) — configuring backoff and jitter
- [Glossary](glossary.md) — definitions for terms used in configuration

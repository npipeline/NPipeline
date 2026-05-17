---
title: "Retry Strategies"
description: "Configure backoff algorithms and jitter to control timing between retry attempts."
order: 4
---

# Retry Strategies

When a resilience policy returns `ResilienceDecision.Retry`, NPipeline waits before retrying. The **retry delay strategy** controls how long to wait. It combines a **backoff algorithm** (how delay grows) with an optional **jitter strategy** (randomization to prevent thundering herds).

## Quick Start

Configure retry delays on `PipelineRetryOptions` using the fluent extension methods:

```csharp
using NPipeline.Configuration;
using NPipeline.Configuration.RetryDelay;

builder.WithRetryOptions(options => options
    .WithExponentialBackoffAndFullJitter());
```

That single line gives you exponential backoff (1s base, 2x multiplier, 1min cap) with full jitter - a sensible default for most production workloads.

## Backoff Algorithms

NPipeline provides three built-in backoff strategies via `BackoffStrategies` in `NPipeline.Execution.RetryDelay`:

### Exponential Backoff

Delay doubles (or multiplies) with each attempt. Best for transient failures where you want to back off quickly.

```
Attempt 0: 1s
Attempt 1: 2s
Attempt 2: 4s
Attempt 3: 8s  (capped at maxDelay)
```

```csharp
BackoffStrategies.ExponentialBackoff(
    baseDelay: TimeSpan.FromSeconds(1),
    multiplier: 2.0,
    maxDelay: TimeSpan.FromMinutes(1));
```

**Parameters:**

- `baseDelay` - delay for the first retry (must be positive)
- `multiplier` - growth factor per attempt (default: 2.0, must be ≥ 1.0)
- `maxDelay` - ceiling to prevent excessive waits (default: 1 minute)

### Linear Backoff

Delay grows by a fixed increment each attempt. More predictable than exponential.

```
Attempt 0: 1s
Attempt 1: 2s
Attempt 2: 3s
Attempt 3: 4s
```

```csharp
BackoffStrategies.LinearBackoff(
    baseDelay: TimeSpan.FromSeconds(1),
    increment: TimeSpan.FromSeconds(1),
    maxDelay: TimeSpan.FromSeconds(30));
```

**Parameters:**

- `baseDelay` - delay for the first retry (must be positive)
- `increment` - added per attempt (default: 1 second)
- `maxDelay` - ceiling (default: 1 minute)

### Fixed Delay

Same delay every time. Simple and deterministic - useful for testing or rate-limited APIs with known cooldowns.

```
Attempt 0: 5s
Attempt 1: 5s
Attempt 2: 5s
```

```csharp
BackoffStrategies.FixedDelay(
    delay: TimeSpan.FromSeconds(5));
```

## Jitter Strategies

Without jitter, clients that fail simultaneously will all retry at the same instant (thundering herd). Jitter randomizes the delay to spread retries across time.

NPipeline provides four jitter strategies via `JitterStrategies`:

| Strategy | Formula | Best For |
|----------|---------|----------|
| **Full Jitter** | `random(0, baseDelay)` | Maximum spread in distributed systems |
| **Equal Jitter** | `baseDelay/2 + random(0, baseDelay/2)` | Balance between predictability and spread |
| **Decorrelated Jitter** | `random(baseDelay, previousDelay × multiplier)` | Long retry sequences where prior delay matters |
| **No Jitter** | `baseDelay` (unchanged) | Testing or when deterministic behavior is required |

### Usage

```csharp
JitterStrategies.FullJitter()
JitterStrategies.EqualJitter()
JitterStrategies.DecorrelatedJitter(maxDelay: TimeSpan.FromMinutes(1), multiplier: 3.0)
JitterStrategies.NoJitter()
```

## Convenience Extension Methods

The most common combinations have dedicated extension methods on `PipelineRetryOptions`:

```csharp
// Exponential backoff + full jitter (recommended default)
options.WithExponentialBackoffAndFullJitter(
    baseDelay: TimeSpan.FromSeconds(1),
    multiplier: 2.0,
    maxDelay: TimeSpan.FromMinutes(1))

// Linear backoff + equal jitter
options.WithLinearBackoffAndEqualJitter(
    baseDelay: TimeSpan.FromSeconds(1),
    increment: TimeSpan.FromSeconds(1),
    maxDelay: TimeSpan.FromSeconds(30))

// Fixed delay + no jitter
options.WithFixedDelayNoJitter(
    delay: TimeSpan.FromSeconds(1))

// Exponential backoff + decorrelated jitter
options.WithExponentialBackoffAndDecorrelatedJitter(
    baseDelay: TimeSpan.FromSeconds(1),
    multiplier: 2.0,
    maxDelay: TimeSpan.FromMinutes(1),
    jitterMaxDelay: TimeSpan.FromMinutes(1),
    jitterMultiplier: 3.0)
```

## Custom Strategy Composition

For full control, compose your own backoff + jitter combination:

```csharp
using NPipeline.Execution.RetryDelay;

var backoff = BackoffStrategies.ExponentialBackoff(
    TimeSpan.FromMilliseconds(500), multiplier: 1.5, maxDelay: TimeSpan.FromSeconds(30));

var jitter = JitterStrategies.EqualJitter();

builder.WithRetryOptions(options => options.WithCustomStrategy(backoff, jitter));
```

## How It Works Internally

1. Your resilience policy returns `ResilienceDecision.Retry`.
2. The runtime calls `IResiliencePolicy.GetRetryDelayAsync(context, attemptNumber)`.
3. The default implementation (`ResiliencePolicyBase`) delegates to `context.GetRetryDelayStrategy()`.
4. The strategy is a `CompositeRetryDelayStrategy` that applies the backoff delegate, then the jitter delegate.
5. The runtime waits the resulting `TimeSpan`, then retries.

The `attemptNumber` is 0-based: attempt 0 is the first retry after the initial failure.

## Choosing a Strategy

| Scenario | Recommended Strategy |
|----------|---------------------|
| Distributed services with transient failures | Exponential + Full Jitter |
| Predictable recovery with some spread | Linear + Equal Jitter |
| Rate-limited APIs with known cooldown | Fixed + No Jitter |
| Long retry sequences (10+ attempts) | Exponential + Decorrelated Jitter |
| Unit tests | Fixed + No Jitter (deterministic) |

## Configuration Reference

`PipelineRetryOptions` controls how many retries are allowed:

| Property | Default | Description |
|----------|---------|-------------|
| `MaxItemRetries` | 0 | Maximum retries per item (0 = no retry) |
| `MaxNodeRestartAttempts` | 3 | Maximum node restart attempts |
| `MaxSequentialNodeAttempts` | 5 | Maximum sequential node execution attempts |
| `MaxMaterializedItems` | null | Item buffer cap for node restart (see [Materialization](materialization.md)) |
| `DelayStrategyConfiguration` | null | Backoff + jitter configuration (null = no delay) |

```csharp
builder.WithRetryOptions(options => options with
{
    MaxItemRetries = 5,
    MaxNodeRestartAttempts = 3,
    DelayStrategyConfiguration = new RetryDelayStrategyConfiguration(
        BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1)),
        JitterStrategies.FullJitter())
});
```

## Monitoring Retry Behavior

In production, monitor retry metrics to detect emerging issues and tune your strategy.

### Key Metrics

| Metric | Healthy Range | Action If Exceeded |
|--------|--------------|-------------------|
| Retry rate (% of operations retried) | 0–5% | Investigate upstream failures |
| Average retry attempts per failure | 1–2 | Lower max retries or fix root cause |
| Retry exhaustion rate (% that hit max) | < 5% | Increase max retries or dead-letter sooner |
| Average delay per retry | Matches configured strategy | Check for clock skew or strategy misconfiguration |

### Structured Logging

Log retry events with context so you can correlate failures across nodes:

```csharp
public sealed class LoggingResiliencePolicy : ResiliencePolicyBase
{
    private readonly ILogger _logger;

    public LoggingResiliencePolicy(ILogger logger) => _logger = logger;

    public override Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
        ITransformNode<TIn, TOut> node, TIn failedItem, Exception exception,
        PipelineContext context, string nodeId, int retryAttempt,
        CancellationToken cancellationToken)
    {
        _logger.LogWarning(
            "Node {NodeId} retry attempt {Attempt}: {ErrorType} - {Message}",
            nodeId, retryAttempt, exception.GetType().Name, exception.Message);

        return Task.FromResult(retryAttempt < 3
            ? ResilienceDecision.Retry
            : ResilienceDecision.DeadLetter);
    }
}
```

### Alerting Thresholds

Set alerts on these conditions:

- **Retry rate > 10%** - systematic failures, not transient
- **Average delay > 5 seconds** - delays are dominating pipeline latency
- **Exhaustion rate > 5%** - too many items hitting the retry ceiling
- **Retry rate trending up > 20%** over the last hour - degrading dependency

### Using Observability Extension

With `NPipeline.Extensions.Observability`, retry events are emitted as part of the node lifecycle. See [Observability](../extensions/observability.md) for integration with your metrics platform.

## Testing Retry Strategies

### Principle: Use Fixed Delays

Always use `FixedDelay` with `NoJitter` in tests. Jitter adds randomness that makes timing assertions unreliable.

```csharp
// Deterministic for testing
builder.WithRetryOptions(options => options
    .WithFixedDelayNoJitter(delay: TimeSpan.FromMilliseconds(10)));
```

### Testing That Retries Occur

Verify that a transient failure triggers the expected number of retries:

```csharp
[Fact]
public async Task Retry_TransientFailure_RetriesThreeTimes()
{
    var attemptCount = 0;
    var policy = ResiliencePolicyBuilder
        .ForNode<FailingTransform, string>()
        .OnAny().Retry(maxRetries: 3)
        .Build();

    // FailingTransform increments attemptCount and throws on first 2 calls
    // ... run pipeline with policy ...

    attemptCount.Should().Be(3); // 1 initial + 2 retries before success
}
```

### Testing Backoff Sequences

Validate that your strategy produces the expected delay progression:

```csharp
[Fact]
public void ExponentialBackoff_ProducesExpectedDelays()
{
    var backoff = BackoffStrategies.ExponentialBackoff(
        baseDelay: TimeSpan.FromMilliseconds(100),
        multiplier: 2.0,
        maxDelay: TimeSpan.FromMilliseconds(500));

    // Attempt 0: 100ms, 1: 200ms, 2: 400ms, 3: 500ms (capped)
    backoff(0).Should().Be(TimeSpan.FromMilliseconds(100));
    backoff(1).Should().Be(TimeSpan.FromMilliseconds(200));
    backoff(2).Should().Be(TimeSpan.FromMilliseconds(400));
    backoff(3).Should().Be(TimeSpan.FromMilliseconds(500));
}
```

### Testing Retry Exhaustion

Confirm that items are dead-lettered after max retries:

```csharp
[Fact]
public async Task RetryExhausted_ItemIsDeadLettered()
{
    var deadLetterSink = new BoundedInMemoryDeadLetterSink();
    builder.AddDeadLetterSink(deadLetterSink);

    var policy = ResiliencePolicyBuilder
        .ForNode<AlwaysFailsTransform, Order>()
        .OnAny().Retry(maxRetries: 3)  // exhausted → DeadLetter
        .Build();

    builder.AddResiliencePolicy(policy);
    // ... run pipeline ...

    deadLetterSink.Items.Should().HaveCount(1);
    deadLetterSink.Items.First().Attribution.RetryCount.Should().Be(3);
}
```

### Testing Jitter Distribution

Use a seeded `Random` to verify jitter behavior deterministically:

```csharp
[Fact]
public void FullJitter_DelaysAreWithinExpectedRange()
{
    var jitter = JitterStrategies.FullJitter();
    var delays = Enumerable.Range(0, 100)
        .Select(_ => jitter(TimeSpan.FromSeconds(1)))
        .ToList();

    delays.Should().OnlyContain(d =>
        d >= TimeSpan.Zero && d <= TimeSpan.FromSeconds(1));

    // Jitter should produce variety, not constant values
    delays.Distinct().Count().Should().BeGreaterThan(1);
}
```

## Next Steps

- [Circuit Breakers](circuit-breakers.md) - stop retrying when a node is consistently failing
- [Materialization](materialization.md) - buffer items to support node restart
- [Dead-Letter Queues](dead-letter-queues.md) - capture items that exhaust all retries

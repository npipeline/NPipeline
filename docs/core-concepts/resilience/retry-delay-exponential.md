---
title: Exponential Backoff
description: Configure exponential backoff retry delays for external APIs and services in NPipeline
sidebar_position: 8.1
---

# Exponential Backoff

Exponential backoff is the most commonly used retry delay strategy, particularly for external API calls and distributed services.

## When to Use

Use exponential backoff when:
- Making external API calls
- Communicating with distributed services
- Dealing with rate-limited endpoints
- You want automatic throttling as retries increase

## Why This Matters

**Without exponential backoff**, if 1,000 clients retry simultaneously after a service recovers, they create a "thundering herd" that can overwhelm the recovering service, causing it to fail again. This creates a cascading failure pattern that propagates throughout distributed systems.

**With exponential backoff**, retries are naturally throttled—early retries happen quickly, but later retries space out, giving the service time to recover gradually without being overwhelmed.

**Real-world impact:**
- Service down 5 seconds → recovers in 30 seconds with exponential backoff
- Service down 5 seconds → fails again at 35 seconds without exponential backoff (cascading failure)
- Rate-limited API: Exponential backoff respects rate limits; naive retries cause account suspension

## Basic Configuration

```csharp
using NPipeline;
using NPipeline.Pipeline;

var context = PipelineContext.Default;
context.UseExponentialBackoffDelay(
    baseDelay: TimeSpan.FromSeconds(1),
    multiplier: 2.0,  // Optional - defaults to 2.0
    maxDelay: TimeSpan.FromMinutes(1));
```

**Parameters:**
- **baseDelay**: Initial delay for the first retry (e.g., 1 second) - **Required**
- **multiplier**: Factor by which delay increases per attempt - **Optional** (default: 2.0)
- **maxDelay**: Maximum delay cap (prevents indefinitely long waits) - **Optional** (default: 5 minutes)

## Delay Progression

With base delay of 1s and multiplier of 2.0:

| Attempt | Delay |
|---------|-------|
| 1 | 1 second |
| 2 | 2 seconds |
| 3 | 4 seconds |
| 4 | 8 seconds |
| 5 | 16 seconds |
| 6+ | 1 minute (capped) |

## Common Configurations

### Web API Calls (Recommended)

```csharp
context.UseExponentialBackoffDelay(
    baseDelay: TimeSpan.FromSeconds(1),
    multiplier: 2.0,
    maxDelay: TimeSpan.FromMinutes(1));
```

**Rationale:**
- 1 second base gives services a brief moment to recover
- 2.0 multiplier provides aggressive backoff
- 1 minute cap prevents excessively long waits

### Aggressive API Throttling

```csharp
context.UseExponentialBackoffDelay(
    baseDelay: TimeSpan.FromMilliseconds(500),
    multiplier: 3.0,
    maxDelay: TimeSpan.FromMinutes(5));
```

**Use when:**
- API has strict rate limits
- Need to back off more aggressively
- Pipeline can tolerate longer recovery times

### Rapid Retry (Service Degradation)

```csharp
context.UseExponentialBackoffDelay(
    baseDelay: TimeSpan.FromMilliseconds(100),
    multiplier: 1.5,
    maxDelay: TimeSpan.FromSeconds(10));
```

**Use when:**
- Expected quick recovery
- Dealing with temporary connection issues
- Need responsive behavior

## With Jitter

Adding jitter prevents the "thundering herd" problem where multiple retries happen simultaneously:

```csharp
var context = PipelineContext.Default;
context.UseExponentialBackoffDelay(
    baseDelay: TimeSpan.FromSeconds(1),
    multiplier: 2.0,
    maxDelay: TimeSpan.FromMinutes(1),
    jitterStrategy: JitterStrategies.FullJitter()); // Randomizes delay
```

**Jitter options:**
- **NoJitter**: Exact delays (use for testing only)
- **FullJitter**: Random delay between 0 and calculated delay
- **EqualJitter**: Random delay between half and full calculated delay
- **DecorrelatedJitter**: Adapts to system load

### Example with Jitter

Base delays: 1s, 2s, 4s, 8s

With full jitter, actual delays might be:
- Retry 1: 0-1s (randomized)
- Retry 2: 0-2s (randomized)
- Retry 3: 0-4s (randomized)
- Retry 4: 0-8s (randomized)

This prevents all retrying clients from retry at the same moment.

## Advanced Configuration

### Decorrelated Jitter for Adaptive Backoff

```csharp
context.UseExponentialBackoffDelay(
    baseDelay: TimeSpan.FromMilliseconds(100),
    multiplier: 2.0,
    maxDelay: TimeSpan.FromSeconds(30),
    jitterStrategy: JitterStrategies.DecorrelatedJitter());
```

**Benefits:**
- Adapts to system load automatically
- Reduces cascading failures in distributed systems
- Better than full jitter for highly concurrent scenarios

## Pipeline Integration

```csharp
public sealed class ApiRetryPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Configure exponential backoff for API calls
        context.UseExponentialBackoffDelay(
            baseDelay: TimeSpan.FromSeconds(1),
            multiplier: 2.0,
            maxDelay: TimeSpan.FromMinutes(1),
            jitterStrategy: JitterStrategies.FullJitter());

        var source = builder.AddSource<ApiSource, ApiResponse>("api-source");
        var transform = builder.AddTransform<ApiTransform, ApiResponse, Result>("transform");
        var sink = builder.AddSink<ResultSink, Result>("sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        // Configure retry options
        builder.WithRetryOptions(new PipelineRetryOptions(
            MaxItemRetries: 5,
            MaxNodeRestartAttempts: 2,
            MaxSequentialNodeAttempts: 5
        ));
    }
}
```

## Best Practices

1. **Start Conservative**: Use 1 second base delay, adjust if needed
2. **Always Use Jitter**: Prevents thundering herd in distributed systems
3. **Set Reasonable Max**: Cap delays at 1-5 minutes for external services
4. **Monitor Metrics**: Track actual retry delays in production
5. **Test with Fixed Delays**: Use NoJitter in tests for predictability
6. **Consider Circuit Breaker**: Combine with circuit breaker to fail fast

## Related Topics

- [Retry Configuration](retries.md) - Overall retry configuration options
- [Retry Delays](retry-delays.md) - Overview of all retry delay strategies
- [Linear Backoff](retry-delay-linear.md) - Linear delay progression
- [Fixed Delay](retry-delay-fixed.md) - Constant delay strategy
- [Advanced Patterns](retry-delay-advanced.md) - Decorrelated jitter and custom strategies
- [Testing Retries](retry-delay-testing.md) - Testing retry behavior
- [Monitoring Retries](retry-delay-monitoring.md) - Observing retry metrics

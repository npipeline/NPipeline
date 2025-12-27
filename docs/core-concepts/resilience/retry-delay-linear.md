---
title: Linear Backoff
description: Configure linear backoff retry delays for database operations in NPipeline
sidebar_position: 8.2
---

# Linear Backoff

Linear backoff increases delays by a fixed increment with each retry, providing predictable behavior suitable for database operations and connection pool issues.

## When to Use

Use linear backoff when:
- Handling database connection timeouts
- Dealing with connection pool contention
- You need predictable, gradual delay increases
- Resource recovery is expected within a timeframe

## Why This Matters

**Without linear backoff** (using fixed delays), you might retry too aggressively during lock contention, creating a resource starvation loop where retries prevent other operations from completing.

**With linear backoff**, delays grow gradually, giving the database connection pool time to recover between retries. The predictable progression matches how databases typically recover from temporary resource exhaustion.

**Real-world impact:**
- Database locked: Linear backoff allows lock to release gracefully
- Fixed 1-second delays: Lock might still be held, causing repeated failures
- Connection pool exhausted: Linear backoff lets connections return before retry

## Basic Configuration

```csharp
using NPipeline;
using NPipeline.Pipeline;

var context = PipelineContext.Default;
context.UseLinearBackoffDelay(
    baseDelay: TimeSpan.FromMilliseconds(100),
    increment: TimeSpan.FromMilliseconds(200),  // Required
    maxDelay: TimeSpan.FromSeconds(5));
```

**Parameters:**
- **baseDelay**: Initial delay for the first retry - **Required**
- **increment**: Amount to add to delay with each retry - **Required**
- **maxDelay**: Maximum delay cap - **Optional** (default: 5 minutes)

## Delay Progression

With base delay of 100ms and increment of 200ms:

| Attempt | Delay |
|---------|-------|
| 1 | 100 ms |
| 2 | 300 ms |
| 3 | 500 ms |
| 4 | 700 ms |
| 5 | 900 ms |
| 6+ | 5 seconds (capped) |

## Common Configurations

### Database Connections (Recommended)

```csharp
context.UseLinearBackoffDelay(
    baseDelay: TimeSpan.FromMilliseconds(100),
    increment: TimeSpan.FromMilliseconds(200),
    maxDelay: TimeSpan.FromSeconds(5));
```

**Rationale:**
- Short initial delay for quick recovery
- Predictable increment works well for pool recovery
- 5 second cap prevents excessively long waits

### Aggressive Lock Contention

```csharp
context.UseLinearBackoffDelay(
    baseDelay: TimeSpan.FromMilliseconds(50),
    increment: TimeSpan.FromMilliseconds(100),
    maxDelay: TimeSpan.FromSeconds(2));
```

**Use when:**
- Dealing with row-level locks
- Expecting quick lock release
- High throughput scenarios

### Conservative Retry

```csharp
context.UseLinearBackoffDelay(
    baseDelay: TimeSpan.FromMilliseconds(500),
    increment: TimeSpan.FromMilliseconds(500),
    maxDelay: TimeSpan.FromSeconds(10));
```

**Use when:**
- Need longer wait times between retries
- Database is under heavy load
- Can tolerate slower recovery

## With Jitter

Adding jitter prevents simultaneous retries from concurrent requests:

```csharp
var context = PipelineContext.Default;
context.UseLinearBackoffDelay(
    baseDelay: TimeSpan.FromMilliseconds(100),
    increment: TimeSpan.FromMilliseconds(200),
    maxDelay: TimeSpan.FromSeconds(5),
    jitterStrategy: JitterStrategies.FullJitter());
```

## Pipeline Integration

```csharp
public sealed class DatabaseRetryPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Configure linear backoff for database operations
        context.UseLinearBackoffDelay(
            baseDelay: TimeSpan.FromMilliseconds(100),
            increment: TimeSpan.FromMilliseconds(200),
            maxDelay: TimeSpan.FromSeconds(5),
            jitterStrategy: JitterStrategies.FullJitter());

        var source = builder.AddSource<DatabaseSource, Row>("db-source");
        var transform = builder.AddTransform<Transform, Row, ProcessedRow>("transform");
        var sink = builder.AddSink<DatabaseSink, ProcessedRow>("db-sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        builder.WithRetryOptions(new PipelineRetryOptions(
            MaxItemRetries: 3,
            MaxNodeRestartAttempts: 2,
            MaxSequentialNodeAttempts: 5
        ));
    }
}
```

## Comparison with Exponential Backoff

| Aspect | Linear | Exponential |
|--------|--------|-------------|
| Delay Curve | Straight line | Steep curve |
| Predictability | High | Medium |
| Speed to Max | Medium | Fast |
| Best For | Database | APIs |
| Resource Impact | Controlled | Can escalate quickly |

## Best Practices

1. **Start with 100ms Base**: Allows quick initial recovery
2. **Match Increment to Domain**: DB = 200ms, files = 500ms
3. **Cap at Reasonable Max**: 5-10 seconds for databases
4. **Add Jitter**: Prevents synchronized retries
5. **Monitor Lock Contention**: Adjust increment based on actual delays
6. **Test with Fixed Delays**: Disable jitter during unit tests

## Related Topics

- [Retry Configuration](retries.md) - Overall retry configuration options
- [Retry Delays](retry-delays.md) - Overview of all retry delay strategies
- [Exponential Backoff](retry-delay-exponential.md) - Exponential delay progression
- [Fixed Delay](retry-delay-fixed.md) - Constant delay strategy
- [Advanced Patterns](retry-delay-advanced.md) - Custom strategies and patterns
- [Testing Retries](retry-delay-testing.md) - Testing retry behavior
- [Monitoring Retries](retry-delay-monitoring.md) - Observing retry metrics

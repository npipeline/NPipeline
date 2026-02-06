---
title: Retry Configuration
description: Learn how to configure retry behavior in NPipeline using PipelineRetryOptions to manage item retries and node restarts.
sidebar_position: 7
---

# Retry Configuration

Retry configuration in NPipeline allows you to define how the pipeline should respond to transient failures by retrying operations. This is essential for building resilient pipelines that can recover from temporary issues without manual intervention.

## Overview

NPipeline provides configurable retry options that control both individual item retries (for node-level errors) and node restart attempts (for pipeline-level errors). These options can be set globally for the entire pipeline or overridden for specific nodes.

## PipelineRetryOptions

The [`PipelineRetryOptions`](../../src/NPipeline/Configuration/PipelineRetryOptions.cs) record configures retry behavior for items and node restarts.

```csharp
public sealed record PipelineRetryOptions(
    int MaxItemRetries,
    int MaxNodeRestartAttempts,
    int MaxSequentialNodeAttempts,
    int? MaxMaterializedItems = null) // Null => unbounded (no cap)
{
    public static PipelineRetryOptions Default { get; } = new(0, 3, 5);

    public PipelineRetryOptions With(
        int? maxItemRetries = null,
        int? maxNodeRestartAttempts = null,
        int? maxSequentialNodeAttempts = null,
        int? maxMaterializedItems = null)
    {
        return new PipelineRetryOptions(
            maxItemRetries ?? MaxItemRetries,
            maxNodeRestartAttempts ?? MaxNodeRestartAttempts,
            maxSequentialNodeAttempts ?? MaxSequentialNodeAttempts,
            maxMaterializedItems ?? MaxMaterializedItems);
    }
}
```

* **`MaxItemRetries`**: The maximum number of times an individual item will be re-processed by a node's execution strategy if its `INodeErrorHandler` returns `NodeErrorDecision.Retry`.
* **`MaxNodeRestartAttempts`**: The maximum number of times a node's entire stream will be re-executed by `ResilientExecutionStrategy` if `IPipelineErrorHandler` returns `PipelineErrorDecision.RestartNode`.
* **`MaxSequentialNodeAttempts`**: (Used by `SequentialExecutionStrategy` for node restarts) The maximum number of attempts for a node in a sequential pipeline.
* **`MaxMaterializedItems`**: An optional cap on the number of items to materialize (buffer) for replay when using `ResilientExecutionStrategy`.
  * When `null` (default): Unbounded materialization - all items are buffered
  * When has a value: Limited materialization - only the specified number of items are buffered, after which new items replace the oldest ones
  * This parameter prevents excessive memory consumption in case of large streams and is particularly important for long-running pipelines with high throughput

## Basic Retry Configuration

### Global Retry Options

Retry options can be set globally for the pipeline:

```csharp
using NPipeline;
using NPipeline.Nodes;
using NPipeline.Pipeline;

public sealed class RetryPipelineDefinition : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var sourceHandle = builder.AddSource<DataSource, string>();
        var transformHandle = builder.AddTransform<DataTransform, string, string>();
        var sinkHandle = builder.AddSink<DataSink, string>();

        builder.Connect(sourceHandle, transformHandle);
        builder.Connect(transformHandle, sinkHandle);

        // Configure retry policy using PipelineRetryOptions
        builder.WithRetryOptions(new PipelineRetryOptions(
            MaxItemRetries: 3,
            MaxNodeRestartAttempts: 2,
            MaxSequentialNodeAttempts: 5
        ));
    }
}

public static class Program
{
    public static async Task Main(string[] args)
    {
        var runner = PipelineRunner.Create();
        await runner.RunAsync<RetryPipelineDefinition>();
    }
}
```

### Per-Node Retry Options

Override the global retry options for a specific node using the `WithRetryOptions` method that accepts a `NodeHandle`:

```csharp
using NPipeline;
using NPipeline.Nodes;
using NPipeline.Pipeline;

public sealed class PerNodeRetryPipelineDefinition : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var sourceHandle = builder.AddSource<DataSource, string>("source");
        var criticalTransformHandle = builder.AddTransform<CriticalDataTransform, string, string>("critical-transform");
        var normalTransformHandle = builder.AddTransform<NormalDataTransform, string, string>("normal-transform");
        var sinkHandle = builder.AddSink<DataSink, string>("sink");

        builder.Connect(sourceHandle, criticalTransformHandle);
        builder.Connect(criticalTransformHandle, normalTransformHandle);
        builder.Connect(normalTransformHandle, sinkHandle);

        // Configure global retry options (default for all nodes)
        builder.WithRetryOptions(new PipelineRetryOptions(
            MaxItemRetries: 2,
            MaxNodeRestartAttempts: 3,
            MaxSequentialNodeAttempts: 5
        ));

        // Override retry options for the critical transform
        builder.WithRetryOptions(criticalTransformHandle, new PipelineRetryOptions(
            MaxItemRetries: 5,              // More retries for critical processing
            MaxNodeRestartAttempts: 10,      // More restart attempts
            MaxSequentialNodeAttempts: 15,   // Higher total attempt limit
            MaxMaterializedItems: 1000       // Higher memory limit for replay
        ));
    }
}
```

#### Use Cases for Per-Node Retry Configuration

Per-node retry options are useful in scenarios where different parts of your pipeline have different resilience requirements:

**1. External API Calls vs. Internal Processing**

```csharp
// More aggressive retries for external API calls
builder.WithRetryOptions(apiTransformHandle, new PipelineRetryOptions(
    MaxItemRetries: 5,
    MaxNodeRestartAttempts: 3,
    MaxSequentialNodeAttempts: 10
));

// Conservative retries for internal data processing
builder.WithRetryOptions(internalTransformHandle, new PipelineRetryOptions(
    MaxItemRetries: 1,
    MaxNodeRestartAttempts: 2,
    MaxSequentialNodeAttempts: 3
));
```

**2. Critical vs. Non-Critical Processing**

```csharp
// Maximum resilience for critical business logic
builder.WithRetryOptions(criticalProcessorHandle, new PipelineRetryOptions(
    MaxItemRetries: 10,
    MaxNodeRestartAttempts: 5,
    MaxSequentialNodeAttempts: 20,
    MaxMaterializedItems: 5000
));

// Best-effort processing for non-critical data
builder.WithRetryOptions(loggingProcessorHandle, new PipelineRetryOptions(
    MaxItemRetries: 0,  // No retries for logging
    MaxNodeRestartAttempts: 1,
    MaxSequentialNodeAttempts: 2
));
```

**3. Resource-Intensive Operations**

```csharp
// Limited retries for memory-intensive operations
builder.WithRetryOptions(memoryIntensiveHandle, new PipelineRetryOptions(
    MaxItemRetries: 1,
    MaxNodeRestartAttempts: 1,
    MaxSequentialNodeAttempts: 2,
    MaxMaterializedItems: 100  // Strict memory limit
));
```

#### Precedence Rules

When both global and per-node retry options are configured:

1. **Per-node options take precedence** over global options
2. **Unspecified properties** in per-node options inherit from global options
3. **Global options** apply to all nodes without specific overrides

```csharp
// Global configuration
builder.WithRetryOptions(new PipelineRetryOptions(
    MaxItemRetries: 2,
    MaxNodeRestartAttempts: 3,
    MaxSequentialNodeAttempts: 5
));

// Per-node override (only MaxItemRetries is overridden)
builder.WithRetryOptions(specificNodeHandle, new PipelineRetryOptions(
    MaxItemRetries: 5,              // Overridden
    MaxNodeRestartAttempts: 3,       // Inherited from global
    MaxSequentialNodeAttempts: 5      // Inherited from global
));
```

## Related Documentation

For delay strategies and advanced configuration patterns, see [Retry Delay Strategies](retry-delays.md).

## Best Practices

1. **Start conservative**: Begin with low retry counts and increase only if needed.
2. **Consider resource impact**: Each retry consumes resources. Set reasonable limits.
3. **Use per-node configuration**: Different nodes often need different retry strategies.
4. **Monitor retry metrics**: Track how often retries occur to identify persistent issues.
5. **Combine with error handlers**: Use retry options with error handlers for comprehensive resilience.

## See Also

* [Error Handling](error-handling.md) - Configure what happens on errors (retry, skip, dead-letter)
* [Retry Delay Strategies](retry-delays.md) - Configure delays between retry attempts
* [Circuit Breakers](circuit-breakers.md) - Prevent cascading failures
* [Getting Started with Resilience](getting-started.md) - Quick guide to resilience features

### Why Backoff and Jitter Matter

**The Thundering Herd Problem:** When multiple pipeline instances encounter failures simultaneously and retry immediately, they can overwhelm a recovering service, causing cascading failures. Backoff strategies prevent this by increasing delays between retries, and jitter adds randomness to prevent synchronized retries.

### Available Backoff Strategies

#### Exponential Backoff

Delays grow exponentially, ideal for distributed systems with transient failures:

```csharp
// Configuration approach
var config = new ExponentialBackoffConfiguration(
    baseDelay: TimeSpan.FromSeconds(1),
    multiplier: 2.0,
    maxDelay: TimeSpan.FromMinutes(1));

// Runtime configuration via PipelineContext
context.UseExponentialBackoffDelay(
    baseDelay: TimeSpan.FromSeconds(1),
    multiplier: 2.0,
    maxDelay: TimeSpan.FromMinutes(1));

// Direct delegate creation
var exponentialBackoff = BackoffStrategies.ExponentialBackoff(
    baseDelay: TimeSpan.FromSeconds(1),
    multiplier: 2.0,
    maxDelay: TimeSpan.FromMinutes(1));
```

**Delay progression:** 1s → 2s → 4s → 8s → 16s → 32s → 60s (capped)

**Use cases:**
* Web API calls with transient network failures
* Database connections during temporary overload
* Microservice communication during partial outages

#### Linear Backoff

Delays grow linearly, providing predictable recovery:

```csharp
// Configuration approach
var config = new LinearBackoffConfiguration(
    baseDelay: TimeSpan.FromSeconds(1),
    increment: TimeSpan.FromSeconds(2),
    maxDelay: TimeSpan.FromSeconds(30));

// Runtime configuration
context.UseLinearBackoffDelay(
    baseDelay: TimeSpan.FromSeconds(1),
    increment: TimeSpan.FromSeconds(2),
    maxDelay: TimeSpan.FromSeconds(30));

// Direct delegate creation
var linearBackoff = BackoffStrategies.LinearBackoff(
    baseDelay: TimeSpan.FromSeconds(1),
    increment: TimeSpan.FromSeconds(2),
    maxDelay: TimeSpan.FromSeconds(30));
```

**Delay progression:** 1s → 3s → 5s → 7s → 9s → ... → 30s (capped)

**Use cases:**
* File processing with temporary resource contention
* Batch operations with predictable recovery patterns

#### Fixed Delay

Constant delay between all retries:

```csharp
// Configuration approach
var config = new FixedDelayConfiguration(
    delay: TimeSpan.FromSeconds(5));

// Runtime configuration
context.UseFixedDelay(
    delay: TimeSpan.FromSeconds(5));

// Direct delegate creation
var fixedBackoff = BackoffStrategies.FixedDelay(
    delay: TimeSpan.FromSeconds(5));
```

**Delay progression:** 5s → 5s → 5s → 5s → ...

**Use cases:**
* Testing and debugging scenarios
* Known recovery times

### Available Jitter Strategies

```csharp
// Jitter strategy delegate type
using JitterStrategy = Func<TimeSpan, Random, TimeSpan>;

// Built-in jitter strategies
JitterStrategy fullJitter = JitterStrategies.FullJitter();
JitterStrategy equalJitter = JitterStrategies.EqualJitter();
JitterStrategy decorrelatedJitter = JitterStrategies.DecorrelatedJitter();
JitterStrategy noJitter = JitterStrategies.NoJitter();
```

This delegate approach reduces complexity while maintaining the same functionality. Each jitter strategy takes a base delay and a Random instance, and returns a jittered delay.

#### Decorrelated Jitter

Adapts based on previous delays:

```csharp
var config = new DecorrelatedJitterConfiguration(
    maxDelay: TimeSpan.FromMinutes(1),
    multiplier: 3.0);

// Direct delegate creation
var decorrelatedJitter = JitterStrategies.DecorrelatedJitter();
```

#### No Jitter

Deterministic timing (useful for testing):

```csharp
var config = new NoJitterConfiguration();

// Direct delegate creation
var noJitter = JitterStrategies.NoJitter();
```

### Recommended Strategy Combinations

**Web APIs (Recommended):**

```csharp
context.UseExponentialBackoffDelay(
    baseDelay: TimeSpan.FromSeconds(1),
    multiplier: 2.0,
    maxDelay: TimeSpan.FromMinutes(1));

// Or using delegates directly
var backoff = BackoffStrategies.ExponentialBackoff(
    TimeSpan.FromSeconds(1), 2.0, TimeSpan.FromMinutes(1));
var jitter = JitterStrategies.FullJitter();
```

**Database Operations:**

```csharp
context.UseLinearBackoffDelay(
    baseDelay: TimeSpan.FromMilliseconds(100),
    increment: TimeSpan.FromMilliseconds(200),
    maxDelay: TimeSpan.FromSeconds(5));

// Or using delegates directly
var backoff = BackoffStrategies.LinearBackoff(
    TimeSpan.FromMilliseconds(100), 
    TimeSpan.FromMilliseconds(200), 
    TimeSpan.FromSeconds(5));
```

**File Processing:**

```csharp
context.UseFixedDelay(
    delay: TimeSpan.FromSeconds(2));

// Or using delegates directly
var backoff = BackoffStrategies.FixedDelay(TimeSpan.FromSeconds(2));
```

### Configuration at Initialization

Configure delay strategies when building your pipeline:

```csharp
var retryOptions = new PipelineRetryOptions(
    MaxItemRetries: 3,
    MaxNodeRestartAttempts: 2,
    MaxSequentialNodeAttempts: 5,
    delayStrategyConfiguration: new RetryDelayStrategyConfiguration(
        new ExponentialBackoffConfiguration(
            TimeSpan.FromSeconds(1), 2.0, TimeSpan.FromMinutes(1)),
        new FullJitterConfiguration()));

builder.WithRetryOptions(retryOptions);
```

## Related Documentation

For delay strategies and advanced configuration patterns, see [Retry Delay Strategies](retry-delays.md).

## Best Practices

1. **Start conservative**: Begin with low retry counts and increase only if needed.
2. **Consider resource impact**: Each retry consumes resources. Set reasonable limits.
3. **Use per-node configuration**: Different nodes often need different retry strategies.
4. **Monitor retry metrics**: Track how often retries occur to identify persistent issues.
5. **Combine with error handlers**: Use retry options with error handlers for comprehensive resilience.

## See Also

* **[Resilience Overview](../resilience/index.md)**: Comprehensive guide to building fault-tolerant pipelines
* **[Materialization and Buffering](materialization.md)**: Understanding buffer requirements for resilience
* **[Error Handling Guide](error-handling.md)**: Comprehensive error handling patterns and practical implementation guidance
* **[Troubleshooting](troubleshooting.md)**: Diagnose and resolve common resilience issues

## Related Topics

* **[Node-level Error Handling](error-handling.md)**: Learn about handling errors for individual items.
* **[Pipeline-level Error Handling](error-handling.md)**: Learn about handling errors that affect entire node streams.
* **[Circuit Breakers](circuit-breakers.md)**: Configure circuit breaker patterns.
* **[Dead-Letter Queues](dead-letter-queues.md)**: Implement dead-letter queues for problematic items.
* **[Error Handling Overview](error-handling.md)**: Return to the error handling overview.

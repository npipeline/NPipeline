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

You can set retry options globally for the pipeline:

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
        var runner = new PipelineRunner();
        await runner.RunAsync<RetryPipelineDefinition>();
    }
}
```

### Per-Node Retry Options

You can override the global retry options for a specific node using the `WithRetryOptions` method that accepts a `NodeHandle`:

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

## Retry Delay Strategies

NPipeline provides built-in retry delay strategies that combine **backoff** (how delays increase over time) with **jitter** (randomness to prevent synchronized retries). These strategies are essential for distributed systems to prevent thundering herd problems.

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
- Web API calls with transient network failures
- Database connections during temporary overload
- Microservice communication during partial outages

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
- File processing with temporary resource contention
- Batch operations with predictable recovery patterns

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
- Testing and debugging scenarios
- Known recovery times

### Available Jitter Strategies

Jitter strategies are now implemented as delegates rather than interfaces, providing a more streamlined API:

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

You can configure delay strategies when building your pipeline:

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

### Runtime Configuration

Configure delay strategies at runtime within your pipeline definition:

```csharp
public void Define(PipelineBuilder builder, PipelineContext context)
{
    // Check system conditions and configure accordingly
    if (IsHighLoad(context))
    {
        context.UseExponentialBackoffDelay(
            baseDelay: TimeSpan.FromSeconds(2),
            multiplier: 3.0,
            maxDelay: TimeSpan.FromMinutes(5));
    }
    else
    {
        context.UseExponentialBackoffDelay(
            baseDelay: TimeSpan.FromSeconds(1),
            multiplier: 2.0,
            maxDelay: TimeSpan.FromMinutes(1));
    }

    var source = builder.AddSource<MySource, MyData>("Source");
    var transform = builder.AddTransform<MyTransform, MyData, ProcessedData>("Transform");
    builder.Connect(source, transform);
}
```

For more advanced retry delay patterns and scenarios, see the [Advanced Retry Delay Strategies](../../advanced-topics/retry-delay-advanced.md) guide.

## Retry Delay API Reference

### Core Interfaces

#### IRetryDelayStrategy

```csharp
public interface IRetryDelayStrategy
{
    ValueTask<TimeSpan> GetDelayAsync(int attemptNumber, CancellationToken cancellationToken = default);
}
```

Defines the contract for calculating retry delays. The `attemptNumber` is 0-based (0 = first retry).

#### BackoffStrategy (Delegate Type)

```csharp
public delegate TimeSpan BackoffStrategy(int attemptNumber);
```

Represents a backoff strategy that calculates delay based on attempt number. This delegate type replaces the IBackoffStrategy interface with a simpler function-based approach.

### Strategy Classes

#### BackoffStrategies Static Class

```csharp
public static class BackoffStrategies
{
    public static BackoffStrategy ExponentialBackoff(TimeSpan baseDelay, double multiplier = 2.0, TimeSpan? maxDelay = null);
    public static BackoffStrategy LinearBackoff(TimeSpan baseDelay, TimeSpan? increment = null, TimeSpan? maxDelay = null);
    public static BackoffStrategy FixedDelay(TimeSpan delay);
}
```

Provides factory methods for creating backoff strategy delegates.

#### Exponential Backoff

```csharp
var exponentialBackoff = BackoffStrategies.ExponentialBackoff(
    baseDelay: TimeSpan.FromSeconds(1),
    multiplier: 2.0,
    maxDelay: TimeSpan.FromMinutes(1));
```

Formula: `delay = baseDelay × multiplier^attemptNumber` (capped at maxDelay)

#### Linear Backoff

```csharp
var linearBackoff = BackoffStrategies.LinearBackoff(
    baseDelay: TimeSpan.FromSeconds(1),
    increment: TimeSpan.FromSeconds(2),
    maxDelay: TimeSpan.FromSeconds(30));
```

Formula: `delay = baseDelay + (increment × attemptNumber)` (capped at maxDelay)

#### Fixed Delay

```csharp
var fixedBackoff = BackoffStrategies.FixedDelay(delay: TimeSpan.FromSeconds(5));
```

Returns the same delay for all attempts.

#### Jitter Strategies

**JitterStrategies.FullJitter():** Returns a delegate that implements `random(0, baseDelay)`
**JitterStrategies.EqualJitter():** Returns a delegate that implements `baseDelay/2 + random(0, baseDelay/2)`
**JitterStrategies.DecorrelatedJitter():** Returns a delegate that implements `random(baseDelay, min(maxDelay, previousDelay × multiplier))`
**JitterStrategies.NoJitter():** Returns a delegate that returns baseDelay unchanged

These static methods on the `JitterStrategies` class provide convenient access to built-in jitter implementations as delegates, eliminating the need to instantiate strategy classes.

#### CompositeRetryDelayStrategy

```csharp
public sealed class CompositeRetryDelayStrategy : IRetryDelayStrategy
{
    public CompositeRetryDelayStrategy(
        BackoffStrategy backoffStrategy,
        JitterStrategy jitterStrategy);
}
```

Combines backoff and jitter strategies. Both strategies are now delegates rather than interface implementations.

#### NoOpRetryDelayStrategy

```csharp
public sealed class NoOpRetryDelayStrategy : IRetryDelayStrategy
{
    public static NoOpRetryDelayStrategy Instance { get; }
}
```

Singleton that always returns `TimeSpan.Zero` for immediate retries.

### Configuration Classes

#### ExponentialBackoffConfiguration

```csharp
public sealed class ExponentialBackoffConfiguration : BackoffStrategyConfiguration
{
    public ExponentialBackoffConfiguration(TimeSpan baseDelay, double multiplier, TimeSpan maxDelay);
    public TimeSpan BaseDelay { get; }
    public double Multiplier { get; }
    public TimeSpan MaxDelay { get; }
}
```

**Constraints:**

* `baseDelay` > TimeSpan.Zero
* `multiplier` ≥ 1.0
* `maxDelay` ≥ baseDelay

#### LinearBackoffConfiguration

```csharp
public sealed class LinearBackoffConfiguration : BackoffStrategyConfiguration
{
    public LinearBackoffConfiguration(TimeSpan baseDelay, TimeSpan increment, TimeSpan maxDelay);
    public TimeSpan BaseDelay { get; }
    public TimeSpan Increment { get; }
    public TimeSpan MaxDelay { get; }
}
```

**Constraints:**

* `baseDelay` > TimeSpan.Zero
* `increment` ≥ TimeSpan.Zero
* `maxDelay` ≥ baseDelay

#### FixedDelayConfiguration

```csharp
public sealed class FixedDelayConfiguration : BackoffStrategyConfiguration
{
    public FixedDelayConfiguration(TimeSpan delay);
    public TimeSpan Delay { get; }
}
```

**Constraints:**

* `delay` > TimeSpan.Zero

#### Jitter Configurations

* `FullJitterConfiguration` - No parameters required
* `EqualJitterConfiguration` - No parameters required
* `DecorrelatedJitterConfiguration` - No parameters required
* `NoJitterConfiguration` - No parameters required

#### RetryDelayStrategyConfiguration

```csharp
public sealed class RetryDelayStrategyConfiguration
{
    public RetryDelayStrategyConfiguration(
        BackoffStrategyConfiguration backoffConfiguration,
        JitterStrategyConfiguration jitterConfiguration);
}
```

Combines backoff and jitter configurations.

### Extension Methods

#### PipelineContextRetryDelayExtensions

```csharp
public static class PipelineContextRetryDelayExtensions
{
    public static IRetryDelayStrategy GetRetryDelayStrategy(this PipelineContext context);
    public static Task<TimeSpan> GetRetryDelayAsync(this PipelineContext context, int attempt);
    
    public static PipelineContext UseExponentialBackoffDelay(
        this PipelineContext context,
        TimeSpan baseDelay,
        double multiplier = 2.0,
        TimeSpan? maxDelay = null);
    
    public static PipelineContext UseLinearBackoffDelay(
        this PipelineContext context,
        TimeSpan baseDelay,
        TimeSpan increment,
        TimeSpan? maxDelay = null);
    
    public static PipelineContext UseFixedDelay(
        this PipelineContext context,
        TimeSpan delay);
    
    public static PipelineContext UseExponentialBackoffWithJitter(
        this PipelineContext context,
        TimeSpan baseDelay,
        double multiplier = 2.0,
        TimeSpan? maxDelay = null,
        TimeSpan? jitterMax = null);
}
```

### Factory

#### DefaultRetryDelayStrategyFactory

```csharp
public sealed class DefaultRetryDelayStrategyFactory
{
    public IRetryDelayStrategy CreateStrategy(RetryDelayStrategyConfiguration configuration);
    public IRetryDelayStrategy CreateExponentialBackoff(ExponentialBackoffConfiguration config, JitterStrategy jitterStrategy = null);
    public IRetryDelayStrategy CreateLinearBackoff(LinearBackoffConfiguration config, JitterStrategy jitterStrategy = null);
    public IRetryDelayStrategy CreateFixedDelay(FixedDelayConfiguration config, JitterStrategy jitterStrategy = null);
}
```

Creates retry delay strategies from configurations. The jitter strategy parameter is now a delegate rather than an interface implementation.

### Validation

#### RetryDelayStrategyValidator

```csharp
public static class RetryDelayStrategyValidator
{
    public static void ValidateExponentialBackoffConfiguration(ExponentialBackoffConfiguration configuration);
    public static void ValidateLinearBackoffConfiguration(LinearBackoffConfiguration configuration);
    public static void ValidateFixedDelayConfiguration(FixedDelayConfiguration configuration);
    public static bool IsValidAttemptNumber(int attemptNumber);
    public static bool IsValidDelay(TimeSpan delay);
}
```

Validates retry delay strategy configurations.

## Retry Strategies

### Fixed Delay Retry

The simplest retry strategy uses a fixed delay between attempts:

```csharp
public class FixedDelayRetryHandler : INodeErrorHandler<ITransformNode<string, string>, string>
{
    private readonly TimeSpan _delay = TimeSpan.FromSeconds(2);
    private readonly ILogger _logger;

    public FixedDelayRetryHandler(ILogger logger)
    {
        _logger = logger;
    }

    public async Task<NodeErrorDecision> HandleAsync(
        ITransformNode<string, string> node,
        string failedItem,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        if (IsTransientError(error))
        {
            _logger.LogInformation("Retrying in {Delay}ms for item: {Item}", _delay.TotalMilliseconds, failedItem);
            await Task.Delay(_delay, cancellationToken);
            return NodeErrorDecision.Retry;
        }

        return NodeErrorDecision.Skip;
    }

    private static bool IsTransientError(Exception error)
    {
        return error is TimeoutException or HttpRequestException;
    }
}
```

### Exponential Backoff with Jitter

Exponential backoff with jitter is a more sophisticated strategy that helps prevent thundering herd problems:

```csharp
public class ExponentialBackoffRetryHandler : INodeErrorHandler<ITransformNode<string, string>, string>
{
    private readonly TimeSpan _baseDelay = TimeSpan.FromSeconds(1);
    private readonly TimeSpan _maxDelay = TimeSpan.FromMinutes(1);
    private readonly Random _jitter = new();
    private readonly ILogger _logger;

    public ExponentialBackoffRetryHandler(ILogger logger)
    {
        _logger = logger;
    }

    public async Task<NodeErrorDecision> HandleAsync(
        ITransformNode<string, string> node,
        string failedItem,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        if (IsTransientError(error))
        {
            // Get retry count from context (you would need to implement this)
            var retryCount = GetRetryCount(context, failedItem);

            // Calculate exponential delay with jitter
            var exponentialDelay = TimeSpan.FromSeconds(Math.Pow(2, retryCount));
            var jitter = TimeSpan.FromMilliseconds(_jitter.Next(0, 1000));
            var delay = TimeSpan.FromTicks(Math.Min(
                (_baseDelay + exponentialDelay + jitter).Ticks,
                _maxDelay.Ticks));

            _logger.LogInformation("Retrying in {Delay}ms (attempt {RetryCount}) for item: {Item}",
                delay.TotalMilliseconds, retryCount + 1, failedItem);

            await Task.Delay(delay, cancellationToken);
            return NodeErrorDecision.Retry;
        }

        return NodeErrorDecision.Skip;
    }

    private static bool IsTransientError(Exception error)
    {
        return error is TimeoutException or HttpRequestException;
    }

    private int GetRetryCount(PipelineContext context, string item)
    {
        // In a real implementation, you would track retry count in the context
        // This is a simplified example
        return 0;
    }
}
```

## Context-Aware Retry

Sometimes you want to adjust retry behavior based on the context:

```csharp
public class ContextAwareRetryHandler : INodeErrorHandler<ITransformNode<string, string>, string>
{
    private readonly ILogger _logger;
    private readonly Dictionary<string, RetryPolicy> _policies;

    public ContextAwareRetryHandler(ILogger logger)
    {
        _logger = logger;
        _policies = new Dictionary<string, RetryPolicy>
        {
            ["critical"] = new RetryPolicy(maxRetries: 5, baseDelay: TimeSpan.FromSeconds(1)),
            ["normal"] = new RetryPolicy(maxRetries: 3, baseDelay: TimeSpan.FromSeconds(2)),
            ["low"] = new RetryPolicy(maxRetries: 1, baseDelay: TimeSpan.FromSeconds(5))
        };
    }

    public async Task<NodeErrorDecision> HandleAsync(
        ITransformNode<string, string> node,
        string failedItem,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Determine priority from context or item
        var priority = GetPriority(context, failedItem);
        var policy = _policies[priority];

        if (IsTransientError(error) && policy.CanRetry)
        {
            _logger.LogInformation("Retrying with {Priority} policy (attempt {RetryCount}/{MaxRetries}) for item: {Item}",
                priority, policy.CurrentRetry + 1, policy.MaxRetries, failedItem);

            await Task.Delay(policy.GetDelay(), cancellationToken);
            policy.IncrementRetry();

            return NodeErrorDecision.Retry;
        }

        return NodeErrorDecision.Skip;
    }

    private static bool IsTransientError(Exception error)
    {
        return error is TimeoutException or HttpRequestException;
    }

    private string GetPriority(PipelineContext context, string item)
    {
        // In a real implementation, you would determine priority from context or item
        // This is a simplified example
        return item.Contains("critical") ? "critical" :
               item.Contains("low") ? "low" : "normal";
    }

    private class RetryPolicy
    {
        public int MaxRetries { get; }
        public TimeSpan BaseDelay { get; }
        public int CurrentRetry { get; private set; }

        public bool CanRetry => CurrentRetry < MaxRetries;

        public RetryPolicy(int maxRetries, TimeSpan baseDelay)
        {
            MaxRetries = maxRetries;
            BaseDelay = baseDelay;
        }

        public TimeSpan GetDelay()
        {
            // Simple exponential backoff
            return TimeSpan.FromSeconds(Math.Pow(2, CurrentRetry) * BaseDelay.TotalSeconds);
        }

        public void IncrementRetry()
        {
            CurrentRetry++;
        }
    }
}
```

## Best Practices

1. **Differentiate between transient and permanent errors**: Only retry transient errors that might resolve themselves.

2. **Implement appropriate delays**: Use delays between retries to give the system time to recover.

3. **Set reasonable retry limits**: Prevent infinite loops by setting maximum retry counts.

4. **Use jitter in distributed systems**: Add randomness to retry delays to prevent thundering herd problems.

5. **Monitor retry patterns**: Track retry metrics to identify systemic issues.

6. **Consider resource constraints**: Be mindful of the resources consumed by retries, especially in high-volume scenarios.

7. **Implement circuit breakers**: Combine retries with circuit breakers to prevent cascading failures.

## Production Example

Here's a comprehensive example that combines multiple retry strategies:

```csharp
using NPipeline;
using NPipeline.ErrorHandling;
using NPipeline.Pipeline;

public class ProductionRetryHandler : INodeErrorHandler<ITransformNode<string, string>, string>
{
    private readonly ILogger _logger;
    private readonly IMetrics _metrics;
    private readonly Dictionary<string, RetryState> _retryStates = new();
    private readonly TimeSpan _baseDelay = TimeSpan.FromSeconds(1);
    private readonly TimeSpan _maxDelay = TimeSpan.FromMinutes(1);
    private readonly Random _jitter = new();

    private class RetryState
    {
        public int RetryCount { get; set; }
        public DateTime FirstFailureTime { get; set; }
    }

    public ProductionRetryHandler(ILogger logger, IMetrics metrics)
    {
        _logger = logger;
        _metrics = metrics;
    }

    public async Task<NodeErrorDecision> HandleAsync(
        ITransformNode<string, string> node,
        string failedItem,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var itemKey = $"{node.Id}:{failedItem}";

        if (!_retryStates.TryGetValue(itemKey, out var retryState))
        {
            retryState = new RetryState { FirstFailureTime = DateTime.UtcNow };
            _retryStates[itemKey] = retryState;
        }

        // Record metrics
        _metrics.Increment("item_retries", new[]
        {
            new KeyValuePair<string, object>("node_id", node.Id),
            new KeyValuePair<string, object>("error_type", error.GetType().Name),
            new KeyValuePair<string, object>("retry_count", retryState.RetryCount)
        });

        if (IsTransientError(error))
        {
            // Check if we've exceeded the maximum retry count
            if (retryState.RetryCount >= 3)
            {
                _logger.LogWarning("Maximum retries exceeded for item {Item} in node {NodeId}", failedItem, node.Id);
                _retryStates.Remove(itemKey);
                return NodeErrorDecision.DeadLetter;
            }

            // Calculate delay with exponential backoff and jitter
            var exponentialDelay = TimeSpan.FromSeconds(Math.Pow(2, retryState.RetryCount));
            var jitter = TimeSpan.FromMilliseconds(_jitter.Next(0, 1000));
            var delay = TimeSpan.FromTicks(Math.Min(
                (_baseDelay + exponentialDelay + jitter).Ticks,
                _maxDelay.Ticks));

            _logger.LogInformation("Retrying item {Item} in node {NodeId} (attempt {RetryCount}/{MaxRetries}) after {Delay}ms",
                failedItem, node.Id, retryState.RetryCount + 1, 3, delay.TotalMilliseconds);

            retryState.RetryCount++;
            await Task.Delay(delay, cancellationToken);
            return NodeErrorDecision.Retry;
        }

        // For non-transient errors, don't retry
        _logger.LogWarning("Non-transient error for item {Item} in node {NodeId}: {ErrorType}",
            failedItem, node.Id, error.GetType().Name);

        _retryStates.Remove(itemKey);
        return NodeErrorDecision.Skip;
    }

    private static bool IsTransientError(Exception error)
    {
        return error is TimeoutException or HttpRequestException or OperationCanceledException;
    }
}
```

> :warning: Materialization Requirements
When configuring retries with `MaxMaterializedItems`, it's important to understand how buffering enables replay functionality. Materialization is critical because it creates a snapshot of input items that can be replayed if a node fails and needs to restart, preventing data loss and ensuring processing continuity. See [Materialization and Buffering](../resilience/materialization-and-buffering.md) in the resilience section for detailed guidance.

## Troubleshooting Retry Strategies

### Common Issues and Solutions

#### Too Many Retries

* **Symptoms:** High retry counts, long processing times
* **Solutions:**
  * Increase base delay or multiplier to back off more aggressively
  * Lower maximum retry count
  * Add circuit breaker to fail fast
  * Check for systemic issues beyond transient failures

#### Too Few Retries

* **Symptoms:** Premature failures, low success rate
* **Solutions:**
  * Decrease base delay or multiplier for faster retry attempts
  * Increase maximum retry count
  * Verify error classification (distinguish transient vs permanent failures)
  * Check timeout configurations

#### Thundering Herd Problem

* **Symptoms:** Synchronized retry spikes, sudden service overload
* **Solutions:**
  * Add or increase jitter (full jitter is most effective)
  * Use decorrelated jitter for adaptive behavior
  * Reduce concurrent retry attempts
  * Implement rate limiting

#### Long Recovery Times

* **Symptoms:** Slow recovery after service restoration
* **Solutions:**
  * Decrease maximum delay cap
  * Use linear backoff instead of exponential for more predictable delays
  * Lower base delay for faster initial retries
  * Implement circuit breaker reset mechanisms

### Debugging Retry Patterns

Monitor retry behavior using PipelineContext:

```csharp
public class RetryAnalyzer
{
    public async Task AnalyzeRetryPatternAsync(PipelineContext context)
    {
        var strategy = context.GetRetryDelayStrategy();
        var delays = new List<TimeSpan>();
        
        // Simulate retry delays for analysis
        for (int attempt = 0; attempt < 5; attempt++)
        {
            var delay = await strategy.GetDelayAsync(attempt);
            delays.Add(delay);
            Console.WriteLine($"Attempt {attempt}: {delay.TotalMilliseconds:F2}ms");
        }
        
        Console.WriteLine($"Average delay: {delays.Average(d => d.TotalMilliseconds):F2}ms");
        Console.WriteLine($"Max delay: {delays.Max(d => d.TotalMilliseconds):F2}ms");
    }
}
```

### Performance Profiling

Profile strategy performance:

```csharp
public class RetryStrategyProfiler
{
    public async Task ProfileStrategyAsync(
        IRetryDelayStrategy strategy, 
        int attempts = 10)
    {
        var stopwatch = Stopwatch.StartNew();
        var delays = new List<TimeSpan>();
        
        for (int i = 0; i < attempts; i++)
        {
            var delay = await strategy.GetDelayAsync(i);
            delays.Add(delay);
        }
        
        stopwatch.Stop();
        
        Console.WriteLine($"Strategy: {strategy.GetType().Name}");
        Console.WriteLine($"Total time: {stopwatch.ElapsedMilliseconds}ms");
        Console.WriteLine($"Average delay: {delays.Average(d => d.TotalMilliseconds):F2}ms");
        Console.WriteLine($"Min delay: {delays.Min(d => d.TotalMilliseconds):F2}ms");
        Console.WriteLine($"Max delay: {delays.Max(d => d.TotalMilliseconds):F2}ms");
    }
}
```

## Implementation Guidelines

### Integration Checklist

1. **Assess current retry behavior** - Understand existing patterns and limitations
2. **Choose appropriate strategy** - Based on your service type and use case
3. **Test with new configuration** - Validate retry behavior in test environments
4. **Monitor in production** - Observe actual retry patterns and success rates
5. **Fine-tune parameters** - Optimize base delay, multiplier/increment, and max delay based on metrics
6. **Document decisions** - Record why specific strategies were chosen for future reference

### Parameter Selection Guidelines

#### Base Delay

* **Too short:** Can overwhelm the failing service, defeating the purpose of backoff
* **Too long:** Unnecessary delays during normal recovery
* **Guidance:** Start with 10-50% of expected operation time, then adjust based on observation

#### Backoff Multiplier/Increment

* **Exponential multiplier:** 1.5-3.0 (2.0 is a common default)
* **Linear increment:** 50-200% of base delay
* **Consider:** Service recovery patterns and whether you want aggressive or conservative backoff

#### Maximum Delay

* **Web APIs:** 30 seconds to 5 minutes (services usually recover quickly)
* **Databases:** 5-30 seconds (recovery time depends on lock contention and query queues)
* **File operations:** 1-10 seconds (filesystem recovery is usually immediate)
* **Message queues:** 10-60 seconds (depends on queue depth and processing rate)

### Monitoring Key Metrics

```csharp
public class RetryMetricsCollector
{
    public void CollectMetrics(PipelineContext context)
    {
        var metrics = new
        {
            TotalAttempts = context.Metrics.GetCounter("total_attempts"),
            SuccessfulRetries = context.Metrics.GetCounter("successful_retries"),
            FailedRetries = context.Metrics.GetCounter("failed_retries"),
            AverageRetryDelay = context.Metrics.GetGauge("average_retry_delay_ms"),
            MaxRetryDelayReached = context.Metrics.GetCounter("max_retry_delay_reached")
        };
        
        var successRate = (double)metrics.SuccessfulRetries / metrics.TotalAttempts;
        Console.WriteLine($"Retry success rate: {successRate:P}");
        Console.WriteLine($"Average retry delay: {metrics.AverageRetryDelay:F2}ms");
    }
}
```

## See Also

* **[Resilience Overview](../resilience/index.md)**: Comprehensive guide to building fault-tolerant pipelines
* **[Materialization and Buffering](../resilience/materialization-and-buffering.md)**: Understanding buffer requirements for resilience
* **[Configuration Guide](../resilience/configuration-guide.md)**: Practical implementation guidance with code examples
* **[Troubleshooting](../resilience/troubleshooting.md)**: Diagnose and resolve common resilience issues

## Related Topics

* **[Node-level Error Handling](error-handling-guide.md)**: Learn about handling errors for individual items.
* **[Pipeline-level Error Handling](error-handling-guide.md)**: Learn about handling errors that affect entire node streams.
* **[Circuit Breaker Configuration](circuit-breaker-configuration.md)**: Configure circuit breaker patterns.
* **[Dead-Letter Queues](dead-letter-queues.md)**: Implement dead-letter queues for problematic items.
* **[Error Handling Overview](error-handling-guide.md)**: Return to the error handling overview.


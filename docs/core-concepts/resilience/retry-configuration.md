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

## Integrating External Retry Libraries

For advanced retry patterns like exponential backoff and jitter, you can integrate external libraries like Polly:

```csharp
using Polly;
using Polly.Retry;
using NPipeline.ErrorHandling;

public class AdvancedRetryHandler : INodeErrorHandler<ITransformNode<string, string>, string>
{
    private readonly AsyncRetryPolicy _retryPolicy;

    public AdvancedRetryHandler()
    {
        // Configure exponential backoff with jitter
        _retryPolicy = Policy
            .Handle<HttpRequestException>() // Retry on network errors
            .WaitAndRetryAsync(
                retryCount: 5,
                sleepDurationProvider: retryAttempt =>
                    TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)) + // Exponential backoff
                    TimeSpan.FromMilliseconds(new Random().Next(0, 1000)), // Add jitter
                onRetry: (outcome, timespan, retryAttempt, context) =>
                {
                    Console.WriteLine($"Retry {retryAttempt} after {timespan.TotalSeconds}s due to: {outcome.Exception?.Message}");
                });
    }

    public async Task<NodeErrorDecision> HandleAsync(
        ITransformNode<string, string> node,
        string failedItem,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        if (error is HttpRequestException)
        {
            try
            {
                // Execute the retry policy
                await _retryPolicy.ExecuteAsync(async () =>
                {
                    // In a real implementation, you would re-execute the node logic here
                    // This is a simplified example
                    Console.WriteLine($"Retrying processing of: {failedItem}");
                    await Task.Delay(100, cancellationToken);
                });

                return NodeErrorDecision.Retry;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"All retries failed: {ex.Message}");
                return NodeErrorDecision.DeadLetter; // Send to dead-letter after all retries fail
            }
        }

        return NodeErrorDecision.Skip; // For other error types, just skip
    }
}
```

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


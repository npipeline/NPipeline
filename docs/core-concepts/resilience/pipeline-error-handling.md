---
title: Pipeline-Level Error Handling
description: Handle errors that affect entire node streams using IPipelineErrorHandler
sidebar_position: 3
---

# Pipeline-Level Error Handling

Pipeline-level error handling manages errors that affect an entire node's stream rather than individual items. These are typically more severe errors that might impact the pipeline execution flow, such as infrastructure failures or external service outages.

When an error occurs that affects an entire node's stream (e.g., an external service going down), NPipeline's `ResilientExecutionStrategy` consults your configured `IPipelineErrorHandler` to determine how to react.

## IPipelineErrorHandler Interface

For errors affecting an entire node's stream, implement [`IPipelineErrorHandler`](../../../src/NPipeline/Abstractions/ErrorHandling/IPipelineErrorHandler.cs):

```csharp
public interface IPipelineErrorHandler
{
    Task<PipelineErrorDecision> HandleNodeFailureAsync(
        string nodeId,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken);
}
```

## PipelineErrorDecision Enum

This enum defines the decision when a node stream fails:

- **`RestartNode`**: The node's entire input stream will be re-processed from the beginning (requires the input stream to be replayable, e.g., materialized by `ResilientExecutionStrategy`).
- **`ContinueWithoutNode`**: The failing node is effectively removed from the pipeline, and its output stream will be empty. The pipeline continues without it.
- **`FailPipeline`**: The entire pipeline execution terminates.

## Implementing a Custom Pipeline Error Handler

Here's a basic example:

```csharp
using NPipeline;
using NPipeline.ErrorHandling;
using NPipeline.Pipeline;

/// <summary>
/// Pipeline-level error handler for managing node failures.
/// Demonstrates circuit breaker pattern and restart logic.
/// </summary>
public sealed class MyPipelineErrorHandler : IPipelineErrorHandler
{
    private readonly ILogger _logger;
    private readonly Dictionary<string, int> _nodeRestartAttempts = new();

    public MyPipelineErrorHandler(ILogger logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Handles node-level failures that affect entire stream processing.
    /// Implements circuit breaker pattern to prevent infinite restart loops.
    /// </summary>
    public Task<PipelineErrorDecision> HandleNodeFailureAsync(
        string nodeId,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Track restart attempts for each node
        _nodeRestartAttempts.TryGetValue(nodeId, out var attempts);
        attempts++;
        _nodeRestartAttempts[nodeId] = attempts;

        // Log failure with context for monitoring
        _logger.LogError(error, "Pipeline-level error in node '{NodeId}': {ErrorMessage}",
            nodeId, error.Message);

        // Implement circuit breaker pattern - limit restart attempts
        if (attempts < 3)
        {
            _logger.LogInformation("Attempting to restart node '{NodeId}'. Attempt: {Attempt}",
                nodeId, attempts);
            return Task.FromResult(PipelineErrorDecision.RestartNode);
        }
        else
        {
            _logger.LogError("Node '{NodeId}' failed too many times, failing pipeline.", nodeId);
            return Task.FromResult(PipelineErrorDecision.FailPipeline);
        }
    }
}
```

## Registering a Pipeline Error Handler

Register using the `AddPipelineErrorHandler` method on `PipelineBuilder`:

```csharp
using NPipeline;
using NPipeline.ErrorHandling;
using NPipeline.Pipeline;

public sealed class ErrorHandlingPipelineDefinition : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var sourceHandle = builder.AddSource<DataSource, string>();
        var transformHandle = builder.AddTransform<DataTransform, string, string>();
        var sinkHandle = builder.AddSink<DataSink, string>();

        builder.Connect(sourceHandle, transformHandle);
        builder.Connect(transformHandle, sinkHandle);

        // Configure retry options
        builder.WithRetryOptions(new PipelineRetryOptions(
            MaxItemRetries: 3,
            MaxNodeRestartAttempts: 2,
            MaxSequentialNodeAttempts: 5
        ));

        // Register the pipeline error handler
        builder.AddPipelineErrorHandler<MyPipelineErrorHandler>();
    }
}
```

Also register with your DI container:

```csharp
services.AddSingleton<IPipelineErrorHandler, MyPipelineErrorHandler>();
```

## Common Scenarios

### Scenario 1: Resource Exhaustion Handling

```csharp
using NPipeline;
using NPipeline.ErrorHandling;
using NPipeline.Pipeline;

/// <summary>
/// Pipeline error handler for resource exhaustion scenarios.
/// Demonstrates critical error handling for system resource issues.
/// </summary>
public class ResourceExhaustionHandler : IPipelineErrorHandler
{
    private readonly ILogger _logger;
    private readonly Dictionary<string, int> _nodeFailureCounts = new();

    public ResourceExhaustionHandler(ILogger logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Handles resource exhaustion by failing fast to prevent system damage.
    /// Critical resource errors should immediately terminate processing.
    /// </summary>
    public Task<PipelineErrorDecision> HandleNodeFailureAsync(
        string nodeId,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Track failure count for each node
        _nodeFailureCounts.TryGetValue(nodeId, out var count);
        _nodeFailureCounts[nodeId] = count + 1;

        // Handle critical resource errors immediately
        if (error is OutOfMemoryException or InsufficientMemoryException)
        {
            _logger.LogCritical("Resource exhaustion in node '{NodeId}': {Error}", nodeId, error.Message);
            return Task.FromResult(PipelineErrorDecision.FailPipeline);
        }

        // For other errors, allow limited restarts
        if (_nodeFailureCounts[nodeId] <= 3)
        {
            _logger.LogWarning("Restarting node '{NodeId}' (attempt {Attempt})", 
                nodeId, _nodeFailureCounts[nodeId]);
            return Task.FromResult(PipelineErrorDecision.RestartNode);
        }

        // Too many failures - continue without the problematic node
        _logger.LogError("Node '{NodeId}' failed too many times, continuing without it", nodeId);
        return Task.FromResult(PipelineErrorDecision.ContinueWithoutNode);
    }
}
```

### Scenario 2: External Service Dependency Handling

```csharp
using NPipeline;
using NPipeline.ErrorHandling;
using NPipeline.Pipeline;

/// <summary>
/// Pipeline error handler for external service dependencies.
/// Demonstrates graceful degradation when external services fail.
/// </summary>
public class ExternalServiceErrorHandler : IPipelineErrorHandler
{
    private readonly ILogger _logger;
    private readonly Dictionary<string, DateTime> _lastFailureTime = new();
    private readonly TimeSpan _circuitBreakerWindow = TimeSpan.FromSeconds(60);

    public ExternalServiceErrorHandler(ILogger logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Handles external service failures with circuit breaker pattern.
    /// Fails fast if a node has failed multiple times recently.
    /// </summary>
    public Task<PipelineErrorDecision> HandleNodeFailureAsync(
        string nodeId,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Check if node has failed recently (circuit breaker open)
        _lastFailureTime.TryGetValue(nodeId, out var lastFailure);
        if (DateTime.UtcNow - lastFailure < _circuitBreakerWindow)
        {
            _logger.LogWarning("Circuit breaker open for node '{NodeId}', continuing without it", nodeId);
            return Task.FromResult(PipelineErrorDecision.ContinueWithoutNode);
        }

        // Record failure time
        _lastFailureTime[nodeId] = DateTime.UtcNow;

        _logger.LogError(error, "External service error in node '{NodeId}': {Message}", 
            nodeId, error.Message);

        // For transient network errors, retry the node
        if (error is HttpRequestException or TimeoutException)
        {
            return Task.FromResult(PipelineErrorDecision.RestartNode);
        }

        // For persistent failures, continue without the node
        return Task.FromResult(PipelineErrorDecision.ContinueWithoutNode);
    }
}
```

### Scenario 3: Production-Ready Pipeline Error Handler with Metrics

```csharp
using NPipeline;
using NPipeline.ErrorHandling;
using NPipeline.Pipeline;

/// <summary>
/// Production-ready pipeline error handler with circuit breaker pattern.
/// Demonstrates sophisticated error handling for production environments.
/// </summary>
public class ProductionPipelineErrorHandler : IPipelineErrorHandler
{
    private readonly ILogger _logger;
    private readonly IMetrics _metrics;
    private readonly Dictionary<string, int> _failureCounts = new();

    public ProductionPipelineErrorHandler(ILogger logger, IMetrics metrics)
    {
        _logger = logger;
        _metrics = metrics;
    }

    /// <summary>
    /// Handles node failures with circuit breaker pattern and metrics.
    /// Prevents cascading failures by limiting restart attempts.
    /// </summary>
    public Task<PipelineErrorDecision> HandleNodeFailureAsync(
        string nodeId,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Track failure count for circuit breaker logic
        _failureCounts.TryGetValue(nodeId, out var count);
        _failureCounts[nodeId] = count + 1;

        // Record metrics for monitoring
        _metrics.Increment("pipeline_node_failures", new[] { 
            new KeyValuePair<string, object>("node_id", nodeId),
            new KeyValuePair<string, object>("error_type", error.GetType().Name)
        });

        // Log failure for monitoring
        _logger.LogError(error, "Node {NodeId} failed (attempt {Attempt})", 
            nodeId, _failureCounts[nodeId]);

        // Implement circuit breaker based on error type and count
        return error switch
        {
            // Critical resource errors - fail immediately
            OutOfMemoryException => Task.FromResult(PipelineErrorDecision.FailPipeline),
            
            // Transient errors - allow limited restarts
            _ when _failureCounts[nodeId] < 3 => Task.FromResult(PipelineErrorDecision.RestartNode),
            
            // Persistent failures - continue without node
            _ => Task.FromResult(PipelineErrorDecision.ContinueWithoutNode)
        };
    }
}
```

## Error Decision Patterns

### Pattern 1: Fail-Fast for Critical Errors

```csharp
public Task<PipelineErrorDecision> HandleNodeFailureAsync(
    string nodeId,
    Exception error,
    PipelineContext context,
    CancellationToken cancellationToken)
{
    // Critical errors should fail immediately
    if (error is OutOfMemoryException or StackOverflowException)
    {
        _logger.LogCritical("Critical error in node {NodeId}", nodeId);
        return Task.FromResult(PipelineErrorDecision.FailPipeline);
    }

    // Other errors can be retried
    return Task.FromResult(PipelineErrorDecision.RestartNode);
}
```

### Pattern 2: Graceful Degradation

```csharp
public Task<PipelineErrorDecision> HandleNodeFailureAsync(
    string nodeId,
    Exception error,
    PipelineContext context,
    CancellationToken cancellationToken)
{
    _logger.LogError(error, "Node {NodeId} failed", nodeId);
    
    // Try to restart once, then continue without the node
    if (!_attemptedRestart.Contains(nodeId))
    {
        _attemptedRestart.Add(nodeId);
        return Task.FromResult(PipelineErrorDecision.RestartNode);
    }

    // Node failed again, continue without it
    return Task.FromResult(PipelineErrorDecision.ContinueWithoutNode);
}
```

### Pattern 3: Circuit Breaker Pattern

```csharp
public Task<PipelineErrorDecision> HandleNodeFailureAsync(
    string nodeId,
    Exception error,
    PipelineContext context,
    CancellationToken cancellationToken)
{
    _failureCount.TryGetValue(nodeId, out var count);
    _failureCount[nodeId] = count + 1;

    // Open circuit after N failures
    if (_failureCount[nodeId] >= 3)
    {
        _logger.LogWarning("Circuit breaker open for node {NodeId}", nodeId);
        return Task.FromResult(PipelineErrorDecision.ContinueWithoutNode);
    }

    return Task.FromResult(PipelineErrorDecision.RestartNode);
}
```

## Best Practices

1. **Track failure patterns**: Keep track of when and how often nodes fail to detect persistent issues.

2. **Implement circuit breaker patterns**: Prevent cascading failures by temporarily stopping attempts to failing nodes.

3. **Differentiate between error types**: Critical errors should fail the pipeline immediately, while transient errors might be worth retrying.

4. **Set reasonable limits**: Prevent infinite restarts by setting limits on the number of restart attempts.

5. **Monitor and alert**: Implement proper monitoring and alerting for pipeline failures.

6. **Consider graceful degradation**: Design your pipeline to continue functioning even when some nodes fail.

7. **Log with context**: Include node IDs, error types, and failure counts in your logs for troubleshooting.

## Prerequisites ⚠️

For resilience features like `PipelineErrorDecision.RestartNode` to work properly, you must understand the dependency chain between components. See [Getting Started with Resilience](getting-started.md) for detailed prerequisites.

## Related Documentation

- [Error Handling Overview](error-handling-overview.md) - Understand both levels of error handling
- [Node-Level Error Handling](node-error-handling.md) - Handle individual item failures
- [Getting Started with Resilience](getting-started.md) - Quick guide and prerequisites
- [Retries](retries.md) - Configure retry policies
- [Circuit Breakers](circuit-breakers.md) - Circuit breaker patterns and configuration

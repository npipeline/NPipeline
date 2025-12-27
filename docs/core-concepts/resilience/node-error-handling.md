---
title: Node-Level Error Handling
description: Handle errors for individual items within nodes using INodeErrorHandler
sidebar_position: 2
---

# Node-Level Error Handling

Node-level error handling allows you to define what happens to problematic items without affecting the entire pipeline. When an error occurs during the processing of an individual item in a node, NPipeline invokes your error handler to determine how to proceed.

## INodeErrorHandler Interface

To handle errors within a specific node, implement the [`INodeErrorHandler<in TNode, in TData>`](../../../src/NPipeline/Abstractions/ErrorHandling/INodeErrorHandler.cs) interface.

```csharp
public interface INodeErrorHandler
{
}

/// <summary>
///     Defines the contract for handling errors that occur within a specific node.
/// </summary>
/// <typeparam name="TNode">The type of node where the error occurred.</typeparam>
/// <typeparam name="TData">The type of the data item that failed.</typeparam>
public interface INodeErrorHandler<in TNode, in TData> : INodeErrorHandler where TNode : INode
{
    /// <summary>
    ///     Handles an error that occurred during node execution.
    /// </summary>
    /// <param name="node">The instance of node that failed.</param>
    /// <param name="failedItem">The data item that caused the error.</param>
    /// <param name="error">The exception that was thrown.</param>
    /// <param name="context">The current pipeline context.</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests.</param>
    /// <returns>A <see cref="NodeErrorDecision" /> indicating how to proceed.</returns>
    Task<NodeErrorDecision> HandleAsync(
        TNode node,
        TData failedItem,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken);
}
```

### Interface Components

- **`INodeErrorHandler`**: Marker interface for dependency injection registration.
- **`INodeErrorHandler<in TNode, in TData>`**: Generic interface defining the actual error handling logic.
- **`TNode`**: The type of node where the error occurred.
- **`TData`**: The type of the data item that caused the error.
- **`HandleAsync`**: Called when an error occurs. Receives the failing node, item, exception, and pipeline context. Must return a `NodeErrorDecision`.

## NodeErrorDecision Enum

This enum dictates how the pipeline should proceed after a node-level error:

- **`Skip`**: The failed item is discarded, and the pipeline continues processing subsequent items.
- **`Retry`**: The pipeline attempts to re-process the failed item. The number of retries is configured via `PipelineRetryOptions`.
- **`DeadLetter`**: The failed item is sent to a configured dead-letter sink, and the pipeline continues.
- **`Fail`**: The pipeline immediately terminates with an exception.

## Implementing a Custom Node Error Handler

Here's a basic example:

```csharp
using NPipeline;
using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;

/// <summary>
/// Custom node error handler for transform nodes processing string data.
/// Demonstrates error classification and appropriate response strategies.
/// </summary>
public sealed class MyNodeErrorHandler : INodeErrorHandler<ITransformNode<string, string>, string>
{
    private readonly ILogger _logger;

    public MyNodeErrorHandler(ILogger logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Handles errors that occur during string transformation.
    /// Implements different strategies based on error type for optimal recovery.
    /// </summary>
    public Task<NodeErrorDecision> HandleAsync(
        ITransformNode<string, string> node,
        string failedItem,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Log error with full context for troubleshooting
        _logger.LogError(error, "Error in node '{NodeName}' processing '{FailedItem}': {ErrorMessage}",
            node.Name, failedItem, error.Message);

        // Choose error handling strategy based on exception type
        return error switch
        {
            // Data format errors are permanent - send to dead letter queue
            FormatException => Task.FromResult(NodeErrorDecision.DeadLetter),
            
            // Items marked for retry get another chance
            _ when failedItem.Contains("retry") => Task.FromResult(NodeErrorDecision.Retry),
            
            // All other errors are skipped to continue processing
            _ => Task.FromResult(NodeErrorDecision.Skip)
        };
    }
}
```

## Registering a Node Error Handler

Register your error handler with the `PipelineBuilder`:

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
    }
}
```

Also register with your DI container:

```csharp
services.AddSingleton<INodeErrorHandler<ITransformNode<string, string>, string>, MyNodeErrorHandler>();
```

The marker interface `INodeErrorHandler` (non-generic) is used for DI registration, allowing the container to discover all node error handler implementations.

## Common Scenarios

### Scenario 1: Handling Transient Network Errors

```csharp
public class NetworkErrorHandler : INodeErrorHandler<IApiTransformNode, string>
{
    private readonly ILogger _logger;
    private int _retryCount = 0;

    public NetworkErrorHandler(ILogger logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Handles network errors with exponential backoff retry strategy.
    /// Transient errors are retried, persistent failures are redirected.
    /// </summary>
    public Task<NodeErrorDecision> HandleAsync(
        IApiTransformNode node,
        string failedItem,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Handle network-related errors specifically
        if (error is HttpRequestException httpEx)
        {
            _retryCount++;
            _logger.LogWarning("Network error (attempt {RetryCount}): {ErrorMessage}", 
                _retryCount, httpEx.Message);

            // Retry up to 3 times for transient network errors
            if (_retryCount <= 3)
            {
                return Task.FromResult(NodeErrorDecision.Retry);
            }
            else
            {
                // After max retries, reset counter and redirect to dead letter
                _retryCount = 0;
                return Task.FromResult(NodeErrorDecision.DeadLetter);
            }
        }

        // Non-network errors are skipped to continue processing
        return Task.FromResult(NodeErrorDecision.Skip);
    }
}
```

### Scenario 2: Data Validation Errors

```csharp
public class ValidationErrorHandler : INodeErrorHandler<IValidatorNode, string>
{
    private readonly ILogger _logger;

    public ValidationErrorHandler(ILogger logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Handles validation errors by redirecting to dead letter queue.
    /// Data quality issues are logged separately from system errors.
    /// </summary>
    public Task<NodeErrorDecision> HandleAsync(
        IValidatorNode node,
        string failedItem,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Handle validation errors specifically
        if (error is ValidationException validationEx)
        {
            _logger.LogWarning("Validation failed for item: {Item}. Error: {Error}", 
                failedItem, validationEx.Message);

            // Validation failures indicate data quality issues - redirect for manual review
            return Task.FromResult(NodeErrorDecision.DeadLetter);
        }

        // Other types of errors are skipped to continue processing
        return Task.FromResult(NodeErrorDecision.Skip);
    }
}
```

### Scenario 3: Production-Ready Error Handler with Metrics Integration

```csharp
public class ProductionNodeErrorHandler : INodeErrorHandler<ITransformNode<string, string>, string>
{
    private readonly ILogger _logger;
    private readonly IMetrics _metrics;

    public ProductionNodeErrorHandler(ILogger logger, IMetrics metrics)
    {
        _logger = logger;
        _metrics = metrics;
    }

    /// <summary>
    /// Handles errors with comprehensive logging and metrics collection.
    /// Enables monitoring and alerting for production environments.
    /// </summary>
    public Task<NodeErrorDecision> HandleAsync(
        ITransformNode<string, string> node,
        string failedItem,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Record error metrics for monitoring
        _metrics.Increment("node_errors", new[] { 
            new KeyValuePair<string, object>("node_type", node.GetType().Name),
            new KeyValuePair<string, object>("error_type", error.GetType().Name)
        });

        // Log error with full context
        _logger.LogError(error, "Error processing item in node {NodeName}", node.Name);

        // Implement error handling strategy based on exception type
        return error switch
        {
            // Data validation errors - redirect to dead letter
            ValidationException => Task.FromResult(NodeErrorDecision.DeadLetter),
            
            // Transient errors - retry
            TimeoutException => Task.FromResult(NodeErrorDecision.Retry),
            HttpRequestException => Task.FromResult(NodeErrorDecision.Retry),
            
            // All other errors - skip
            _ => Task.FromResult(NodeErrorDecision.Skip)
        };
    }
}
```

## Fluent Error Handler Builder

For simpler scenarios, use the fluent builder API to construct error handlers inline:

```csharp
using NPipeline.ErrorHandling;

// Create a handler that retries on timeout, skips on validation errors
var handler = ErrorHandler.ForNode<MyTransform, string>()
    .On<TimeoutException>().Retry(3)
    .On<ValidationException>().Skip()
    .OnAny().DeadLetter()
    .Build();
```

### Pre-built Handler Factories

```csharp
// Retry all errors up to N times, then dead-letter
var retryHandler = ErrorHandler.RetryAlways<MyTransform, string>(maxRetries: 3);

// Skip all errors and continue processing
var skipHandler = ErrorHandler.SkipAlways<MyTransform, string>();

// Send all errors to dead-letter sink
var deadLetterHandler = ErrorHandler.DeadLetterAlways<MyTransform, string>();
```

### Exception Type Matching

Rules are evaluated in order, so place more specific rules first:

```csharp
var handler = ErrorHandler.ForNode<MyTransform, string>()
    .On<TimeoutException>().Retry(3)
    .On<IOException>().Retry(5)
    .On<ArgumentException>().Skip()
    .On<InvalidOperationException>().Fail()
    .OnAny().DeadLetter()  // Catch-all (must be last)
    .Build();
```

⚠️ **Important**: `OnAny()` must be the last rule because it matches all exceptions.

### Custom Predicate Matching

For complex scenarios, use custom predicates:

```csharp
var handler = ErrorHandler.ForNode<MyTransform, string>()
    .When(ex => ex.Message.Contains("timeout", StringComparison.OrdinalIgnoreCase))
        .Retry(3)
    .When(ex => ex.Message.Contains("invalid", StringComparison.OrdinalIgnoreCase))
        .Skip()
    .OnAny().Fail()
    .Build();
```

### Default Behavior

Use `Otherwise()` for a default behavior when no rules match:

```csharp
var handler = ErrorHandler.ForNode<MyTransform, string>()
    .On<TimeoutException>().Retry(2)
    .Otherwise(NodeErrorDecision.Skip)  // Default for unmatched exceptions
    .Build();
```

## Fluent Builder Best Practices

1. **Rule Evaluation Order**: Rules are evaluated in order added. First match wins.
2. **Retry Counting**: Automatically tracks retry attempts and transitions to dead-letter when exhausted.
3. **Type Hierarchy**: Exception matching respects inheritance - `On<ArgumentException>()` matches derived types.
4. **Catch-All Pattern**: Use `OnAny()` only at the end as a catch-all.

## When to Use Each Approach

### Use Fluent Builder When:
- ✅ Simple error handling logic with clear exception-to-action mapping
- ✅ Prototyping or quick implementations
- ✅ Straightforward retry/skip/dead-letter strategies

### Implement INodeErrorHandler When:
- ✅ Complex state management across multiple items
- ✅ Advanced logging, metrics, or custom recovery logic
- ✅ Error handling that requires dependency injection
- ✅ Need access to custom services or configuration

## Best Practices

1. **Be specific about error types**: Different error types should be handled differently. Transient errors might be worth retrying, while data validation errors should probably be redirected.

2. **Implement retry limits**: Always limit the number of retries to prevent infinite loops and resource exhaustion.

3. **Log detailed error information**: Include sufficient context in your error logs to help with troubleshooting.

4. **Use dead-letter queues for problematic items**: Items that consistently fail should be redirected to a dead-letter queue for later analysis.

5. **Consider performance implications**: Error handling logic adds overhead to normal processing, so keep it efficient.

## Related Documentation

- [Error Handling Overview](error-handling-overview.md) - Understand both levels of error handling
- [Pipeline-Level Error Handling](pipeline-error-handling.md) - Handle stream/node failures
- [Retries](retries.md) - Configure retry policies and strategies
- [Dead Letter Queues](dead-letter-queues.md) - Route problematic items for analysis

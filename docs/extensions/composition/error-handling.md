# Error Handling in Composite Pipelines

## Overview

Error handling in composite pipelines follows NPipeline's standard error handling model, with errors propagating through the pipeline hierarchy in a predictable manner.

## Error Propagation

### Basic Flow

Errors in sub-pipelines propagate to the parent pipeline:

```
Main Pipeline
  ↓ (executes)
Composite Node
  ↓ (executes)
Sub-Pipeline
  ↓ (throws error)
Transform Node (error occurs here)

Error propagates:
  ↑ Sub-Pipeline error handler
  ↑ Composite Node (re-throws)
  ↑ Main Pipeline error handler
```

### Example

```csharp
// Sub-pipeline with potential error
public class ValidationPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddSource<PipelineInputSource<Data>, Data>("input");
        
        // This transform might throw
        var validate = builder.AddTransform<ValidatorNode, Data, Data>("validate");
        
        var output = builder.AddSink<PipelineOutputSink<Data>, Data>("output");
        
        builder.Connect(input, validate);
        builder.Connect(validate, output);
    }
}

// ValidatorNode that throws on invalid data
public class ValidatorNode : TransformNode<Data, Data>
{
    public override Task<Data> ExecuteAsync(Data input, PipelineContext context, CancellationToken ct)
    {
        if (!input.IsValid)
        {
            throw new ValidationException($"Invalid data: {input.Id}");
        }
        
        return Task.FromResult(input);
    }
}

// Parent pipeline
public class ProcessingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<DataSource, Data>("source");
        var validate = builder.AddComposite<Data, Data, ValidationPipeline>("validate");
        var sink = builder.AddSink<DataSink, Data>("sink");
        
        builder.Connect(source, validate);
        builder.Connect(validate, sink);
    }
}

// Usage with error handling
try
{
    await runner.RunAsync<ProcessingPipeline>(context);
}
catch (ValidationException ex)
{
    // Error from sub-pipeline caught here
    Console.WriteLine($"Validation failed: {ex.Message}");
}
```

## Error Handling Strategies

### Strategy 1: Catch in Sub-Pipeline

Handle errors within the sub-pipeline:

```csharp
public class ResilientSubPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddSource<PipelineInputSource<Data>, Data>("input");
        
        // Transform that handles its own errors
        var transform = builder.AddTransform<ResilientTransform, Data, Result>("transform");
        
        var output = builder.AddSink<PipelineOutputSink<Result>, Result>("output");
        
        builder.Connect(input, transform);
        builder.Connect(transform, output);
    }
}

public class ResilientTransform : TransformNode<Data, Result>
{
    public override async Task<Result> ExecuteAsync(Data input, PipelineContext context, CancellationToken ct)
    {
        try
        {
            var processed = await ProcessAsync(input);
            return Result.Success(processed);
        }
        catch (Exception ex)
        {
            // Handle error and return error result
            return Result.Failure(ex.Message);
        }
    }
    
    private Task<Data> ProcessAsync(Data input) { /* ... */ }
}

// Result type encapsulates success/failure
public class Result
{
    public bool IsSuccess { get; init; }
    public Data? Data { get; init; }
    public string? Error { get; init; }
    
    public static Result Success(Data data) => new() { IsSuccess = true, Data = data };
    public static Result Failure(string error) => new() { IsSuccess = false, Error = error };
}
```

### Strategy 2: Let Errors Propagate

Allow errors to bubble up to parent:

```csharp
public class SubPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddSource<PipelineInputSource<Data>, Data>("input");
        
        // Transform that throws on error
        var transform = builder.AddTransform<ThrowingTransform, Data, Data>("transform");
        
        var output = builder.AddSink<PipelineOutputSink<Data>, Data>("output");
        
        builder.Connect(input, transform);
        builder.Connect(transform, output);
    }
}

// Parent handles all errors - configure via context
public class ParentPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<DataSource, Data>("source");
        var process = builder.AddComposite<Data, Data, SubPipeline>("process");
        var sink = builder.AddSink<DataSink, Data>("sink");
        
        builder.Connect(source, process);
        builder.Connect(process, sink);
    }
}

// Configure error handler via context when running
var context = new PipelineContext(
    PipelineContextConfiguration.WithErrorHandling(
        pipelineErrorHandler: new CustomErrorHandler()));

await runner.RunAsync<ParentPipeline>(context);

public class CustomErrorHandler : IPipelineErrorHandler
{
    public Task<PipelineErrorDecision> HandleNodeFailureAsync(
        string nodeId, 
        Exception error,
        PipelineContext context, 
        CancellationToken cancellationToken)
    {
        // Log error
        Log.Error(error, "Error in node {NodeId}", nodeId);
        
        // Decide how to proceed
        if (error is ValidationException)
        {
            // Continue without this node
            return Task.FromResult(PipelineErrorDecision.ContinueWithoutNode);
        }
        
        // Fail pipeline
        return Task.FromResult(PipelineErrorDecision.FailPipeline);
    }
}
```

### Strategy 3: Hybrid Approach

Handle some errors in sub-pipeline, let others propagate:

```csharp
public class HybridSubPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddSource<PipelineInputSource<Data>, Data>("input");
        
        // Handle expected errors
        var transform = builder.AddTransform<SafeTransform, Data, Data>("transform");
        
        // Let unexpected errors propagate
        var verify = builder.AddTransform<VerifierTransform, Data, Data>("verify");
        
        var output = builder.AddSink<PipelineOutputSink<Data>, Data>("output");
        
        builder.Connect(input, transform);
        builder.Connect(transform, verify);
        builder.Connect(verify, output);
    }
}

public class SafeTransform : TransformNode<Data, Data>
{
    public override async Task<Data> ExecuteAsync(Data input, PipelineContext context, CancellationToken ct)
    {
        try
        {
            return await ProcessAsync(input);
        }
        catch (ExpectedException ex)
        {
            // Handle expected errors
            LogWarning(ex);
            return input; // Return original data
        }
        // Other exceptions propagate
    }
}

public class VerifierTransform : TransformNode<Data, Data>
{
    public override Task<Data> ExecuteAsync(Data input, PipelineContext context, CancellationToken ct)
    {
        // Throws on critical errors
        if (input.IsCriticallyInvalid)
        {
            throw new CriticalException("Critical validation failure");
        }
        
        return Task.FromResult(input);
    }
}
```

## Error Context

### Accessing Error Information

Errors include context about where they occurred:

```csharp
try
{
    await runner.RunAsync<MainPipeline>(context);
}
catch (NodeExecutionException ex)
{
    Console.WriteLine($"Node: {ex.NodeId}");
    Console.WriteLine($"Pipeline: {ex.PipelineId}");
    Console.WriteLine($"Error: {ex.InnerException?.Message}");
    
    // For composite nodes, check if error came from sub-pipeline
    if (ex.NodeId.Contains("composite"))
    {
        Console.WriteLine("Error occurred in sub-pipeline");
    }
}
```

### Adding Context in Sub-Pipelines

Enrich errors with context:

```csharp
public class ContextEnrichingTransform : TransformNode<Data, Data>
{
    public override async Task<Data> ExecuteAsync(Data input, PipelineContext context, CancellationToken ct)
    {
        try
        {
            return await ProcessAsync(input);
        }
        catch (Exception ex)
        {
            // Add context and rethrow
            throw new ProcessingException(
                $"Failed to process item {input.Id} in sub-pipeline",
                ex);
        }
    }
}
```

## Retry and Circuit Breaker

### Retry in Sub-Pipelines

Configure retry behavior for sub-pipelines:

```csharp
public class RetrySubPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Configure retry for this sub-pipeline
        builder.WithRetryOptions(new RetryOptions
        {
            MaxRetries = 3,
            RetryDelay = TimeSpan.FromSeconds(1),
            RetryableExceptions = new[] { typeof(TransientException) }
        });
        
        var input = builder.AddSource<PipelineInputSource<Data>, Data>("input");
        var transform = builder.AddTransform<UnreliableTransform, Data, Data>("transform");
        var output = builder.AddSink<PipelineOutputSink<Data>, Data>("output");
        
        builder.Connect(input, transform);
        builder.Connect(transform, output);
    }
}

// Usage in parent (retry happens at sub-pipeline level)
builder.AddComposite<Data, Data, RetrySubPipeline>("retry-pipeline");
```

### Circuit Breaker for Composite Nodes

Protect parent pipeline from failing sub-pipelines:

```csharp
public class ProtectedParentPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Configure circuit breaker
        builder.WithCircuitBreaker(new CircuitBreakerOptions
        {
            FailureThreshold = 5,
            ResetTimeout = TimeSpan.FromMinutes(1)
        });
        
        var source = builder.AddSource<DataSource, Data>("source");
        
        // If sub-pipeline fails repeatedly, circuit opens
        var process = builder.AddComposite<Data, Data, UnstableSubPipeline>("process");
        
        var sink = builder.AddSink<DataSink, Data>("sink");
        
        builder.Connect(source, process);
        builder.Connect(process, sink);
    }
}
```

## Cancellation Handling

### Respecting Cancellation in Sub-Pipelines

Sub-pipelines automatically respect cancellation:

```csharp
// Cancellation propagates through hierarchy
var cts = new CancellationTokenSource();

// Start pipeline
var task = runner.RunAsync<MainPipeline>(context);

// Cancel after 5 seconds
cts.CancelAfter(TimeSpan.FromSeconds(5));

try
{
    await task;
}
catch (OperationCanceledException)
{
    // Cancellation occurred in main or sub-pipeline
    Console.WriteLine("Pipeline cancelled");
}
```

### Handling Cancellation in Transforms

```csharp
public class CancellableTransform : TransformNode<Data, Data>
{
    public override async Task<Data> ExecuteAsync(Data input, PipelineContext context, CancellationToken ct)
    {
        // Check cancellation before expensive operation
        ct.ThrowIfCancellationRequested();
        
        await ExpensiveOperationAsync(input, ct);
        
        // Check cancellation after operation
        ct.ThrowIfCancellationRequested();
        
        return input;
    }
}
```

## Best Practices

### 1. Fail Fast

Don't catch errors you can't handle:

```csharp
✅ Good: Let critical errors propagate
public override Task<Data> ExecuteAsync(Data input, PipelineContext context, CancellationToken ct)
{
    // Let critical errors propagate
    ValidateCritical(input);
    
    try
    {
        return ProcessAsync(input);
    }
    catch (TransientException ex)
    {
        // Only handle transient errors
        return HandleTransientError(ex, input);
    }
}

❌ Bad: Catch everything
public override Task<Data> ExecuteAsync(Data input, PipelineContext context, CancellationToken ct)
{
    try
    {
        return ProcessAsync(input);
    }
    catch (Exception ex)
    {
        // Swallowing all errors
        return Task.FromResult(input);
    }
}
```

### 2. Provide Context

Include relevant information in error messages:

```csharp
✅ Good: Detailed error message
throw new ProcessingException(
    $"Failed to process customer {input.Id} in validation pipeline. " +
    $"Error: {ex.Message}", ex);

❌ Bad: Generic error
throw new Exception("Error occurred");
```

### 3. Log Appropriately

Log errors at the appropriate level:

```csharp
public class LoggingTransform : TransformNode<Data, Data>
{
    private readonly ILogger _logger;
    
    public LoggingTransform(ILogger logger)
    {
        _logger = logger;
    }
    
    public override async Task<Data> ExecuteAsync(Data input, PipelineContext context, CancellationToken ct)
    {
        try
        {
            return await ProcessAsync(input);
        }
        catch (Exception ex)
        {
            // Log with context
            _logger.LogError(ex,
                "Processing failed for item {ItemId} in pipeline {Pipeline}",
                input.Id,
                context.Properties.GetValueOrDefault("PipelineId"));
            
            throw; // Re-throw for upstream handling
        }
    }
}
```

### 4. Use Typed Exceptions

Create specific exception types for different error scenarios:

```csharp
public class ValidationException : Exception
{
    public string ItemId { get; }
    public List<string> Errors { get; }
    
    public ValidationException(string itemId, List<string> errors)
        : base($"Validation failed for {itemId}")
    {
        ItemId = itemId;
        Errors = errors;
    }
}

public class ProcessingException : Exception
{
    public ProcessingException(string message, Exception inner)
        : base(message, inner) { }
}
```

### 5. Test Error Scenarios

Test how your pipelines handle errors:

```csharp
[Fact]
public async Task SubPipeline_WithInvalidData_ShouldThrowValidationException()
{
    // Arrange
    var context = new PipelineContext();
    context.Parameters[CompositeContextKeys.InputItem] = CreateInvalidData();
    
    // Act & Assert
    await Assert.ThrowsAsync<ValidationException>(() =>
        runner.RunAsync<ValidationPipeline>(context));
}

[Fact]
public async Task ParentPipeline_WithSubPipelineError_ShouldHandleGracefully()
{
    // Arrange
    var context = new PipelineContext();
    var errorHandler = new MockErrorHandler();
    builder.WithErrorHandler(errorHandler);
    
    // Act
    await runner.RunAsync<ParentPipeline>(context);
    
    // Assert
    Assert.True(errorHandler.WasCalled);
}
```

## Summary

| Strategy | Pros | Cons | Use When |
|----------|------|------|----------|
| **Catch in Sub-Pipeline** | Isolated errors, graceful degradation | May hide issues | Expected, recoverable errors |
| **Propagate to Parent** | Centralized handling, fail fast | Less isolation | Unexpected, critical errors |
| **Hybrid** | Flexibility, best of both | More complexity | Complex error scenarios |

Choose the error handling strategy that best fits your:

- **Error types**: Expected vs unexpected
- **Recovery options**: Retry, fallback, or fail
- **Observability needs**: Logging and monitoring requirements
- **User experience**: How errors should affect the pipeline flow

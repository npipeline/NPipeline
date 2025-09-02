---
title: Error Handling Architecture
description: How errors propagate and are handled throughout NPipeline.
sidebar_position: 6
---

# Error Handling Architecture

NPipeline provides multiple strategies for handling errors that occur during pipeline execution, from early failure to graceful degradation.

## Error Propagation

By default, errors propagate up the pipeline and stop execution:

```csharp
var sourcePipe = await source.ExecuteAsync(context, ct);      // Returns 100 items
var transformPipe = new TransformPipe(sourcePipe, transform); // Processing...

// Error occurs on item #50
try
{
    await foreach (var item in transformPipe.WithCancellation(ct))
    {
        await sink.ProcessAsync(item);
    }
}
catch (InvalidOperationException ex)
{
    // Error caught here - items 51-100 never processed
}
```

## Error Containment

Contain errors within a node to prevent pipeline failure:

```csharp
public class SafeTransform : ITransformNode<Input, Output>
{
    private readonly ITransformNode<Input, Output> _inner;
    private readonly ILogger<SafeTransform> _logger;

    public SafeTransform(
        ITransformNode<Input, Output> inner,
        ILogger<SafeTransform> logger)
    {
        _inner = inner;
        _logger = logger;
    }

    public async Task<Output> ExecuteAsync(
        Input item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        try
        {
            return await _inner.ExecuteAsync(item, context, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing item: {@input}", item);
            // Rethrow or return default value depending on strategy
            throw;
        }
    }
}
```

## Dead-Letter Handling

Route failed items to a dead-letter sink configured in the pipeline context:

```csharp
// Configure dead-letter sink when building pipeline context
var deadLetterSink = new FileDeadLetterSink("dead-letters.json");
var context = PipelineContext.WithErrorHandling(deadLetterSink: deadLetterSink);

// In a transform node, use INodeErrorHandler to route failed items
public class OrderTransform : ITransformNode<Order, ProcessedOrder>
{
    public INodeErrorHandler? ErrorHandler { get; set; }

    public async Task<ProcessedOrder> ExecuteAsync(
        Order item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        try
        {
            // Validate and process order
            if (item.Amount <= 0)
                throw new InvalidOperationException("Invalid order amount");
                
            return new ProcessedOrder { /* ... */ };
        }
        catch (Exception ex)
        {
            // Error handler can route to dead-letter sink
            ErrorHandler?.Handle(item, ex, context);
            throw; // Or return default value depending on strategy
        }
    }
}
```

## Retry Patterns

Configure retry behavior using `PipelineRetryOptions`:

```csharp
// Global retry configuration
var builder = new PipelineBuilder();
builder.ConfigureRetry(new PipelineRetryOptions
{
    MaxRetries = 3,
    InitialDelayMs = 100,
    MaxDelayMs = 5000,
    BackoffMultiplier = 2.0
});

// Or per-node retry configuration
var nodeRetryOptions = new PipelineRetryOptions
{
    MaxRetries = 3,
    RetryDelay = TimeSpan.FromSeconds(1),
    ShouldRetry = (exception) => 
        exception is TimeoutException or HttpRequestException
};
builder.ConfigureNodeRetry<FetchInventoryTransform>(nodeRetryOptions);

var pipeline = builder
    .AddSourceNode<OrderSourceNode>()
    .AddTransformNode<FetchInventoryTransform>()  // May fail temporarily
    .AddTransformNode<ProcessOrderTransform>()
    .AddSinkNode<OrderSinkNode>()
    .BuildPipeline();
```

## Error Context and Lineage

Track errors using the current node ID and lineage tracking:

```csharp
// Access current node information during error handling
public override async Task<ProcessedOrder> ExecuteAsync(
    Order item,
    PipelineContext context,
    CancellationToken cancellationToken)
{
    try
    {
        // Process order
        return await ProcessAsync(item, cancellationToken);
    }
    catch (Exception ex)
    {
        var currentNodeId = context.CurrentNodeId;
        
        // Log complete error with context
        logger.LogError(
            ex,
            "Error at node {nodeId}: {error}",
            currentNodeId,
            ex.Message);
            
        throw;
    }
}

// Enable item-level lineage tracking to see all nodes that have processed an item
var builder = new PipelineBuilder();
builder.EnableItemLevelLineage();
```

## Supporting Components

### Materialization Node

Buffer entire streams to catch downstream errors early:

```csharp
// Materialize (collect all items) to detect errors before processing
var materialized = new MaterializationNode<Order>();
var pipeline = PipelineBuilder
    .AddSourceNode<OrderSourceNode>()
    .AddNode(materialized) // Buffers all orders
    .AddTransformNode<ValidateOrderTransform>()
    .AddSinkNode<OrderSinkNode>()
    .BuildPipeline();
```

### Stateful Registry

Maintain error state across pipeline executions:

```csharp
var registry = new StatefulRegistry();

for (int i = 0; i < 5; i++)
{
    try
    {
        await runner.ExecuteAsync(pipeline, context);
    }
    catch (Exception ex)
    {
        registry.RecordError(ex);
    }
}

var stats = registry.GetErrorStatistics();
logger.LogInformation("Total errors: {count}", stats.ErrorCount);
```

## Error Handling Strategies

| Strategy | Best For | Trade-offs |
|----------|----------|-----------|
| **Fail Fast** | Data quality critical | May lose unprocessed items |
| **Skip Errors** | Best-effort processing | Silent failures may hide bugs |
| **Dead-Letter** | Audit trail required | Added storage overhead |
| **Retry** | Transient failures | Delayed processing, retry storms |
| **Materialize First** | Need all data or nothing | Memory overhead |

## Next Steps

- **[Cancellation Model](cancellation-model.md)** - Learn how cancellation interacts with error handling
- **[Performance Characteristics](performance-characteristics.md)** - Understand error handling performance impact

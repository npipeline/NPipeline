---
title: Transform Nodes
description: Learn how to implement transform nodes and optimize for high performance using ValueTask<T>.
sidebar_position: 3
---

# Transform Nodes (`ITransformNode<TIn, TOut>`)

Transform nodes take an input stream of `TInput` items, perform some operation on them, and then produce an output stream of `TOutput` items. They are the workhorses of data manipulation within a pipeline.

## Interface Definition

```csharp
public interface ITransformNode<TIn, TOut> : INode
{
    Task<TOut> ExecuteAsync(TIn item, PipelineContext context, CancellationToken cancellationToken);
}
```

## Stream Transform Nodes (`IStreamTransformNode<TIn, TOut>`)

Stream transform nodes operate on entire streams of data rather than individual items. They are designed for scenarios where you need to transform the stream itself - such as batching, unbatching, windowing, or other operations that change the cardinality or timing of data flow.

### When to Use Stream Transforms

Use `IStreamTransformNode` when your node needs to:

* **Change stream cardinality** - Convert one item to many, many to one, or many to many
* **Process data in groups** - Apply operations to batches or windows of data
* **Introduce timing elements** - Add temporal constraints to data flow
* **Optimize for specific patterns** - Implement batching, buffering, or stream-aware algorithms

### Interface Definition

```csharp
public interface IStreamTransformNode<TIn, TOut> : INode
{
    IExecutionStrategy ExecutionStrategy { get; set; }
    INodeErrorHandler? ErrorHandler { get; set; }

    IAsyncEnumerable<TOut> ExecuteAsync(
        IAsyncEnumerable<TIn> items,
        PipelineContext context,
        CancellationToken cancellationToken);
}
```

## Choosing Between Transform Types

| **Aspect** | **ITransformNode** | **IStreamTransformNode** |
|---|---|---|
| Processing Model | One item at a time | Entire stream at once |
| Return Type | `Task<TOut>` | `IAsyncEnumerable<TOut>` |
| Use Cases | Simple transformations, enrichment, validation | Batching, unbatching, windowing, filtering |
| Memory Pattern | Low memory footprint | Higher memory for stream processing |
| Performance | Low per-item overhead | Optimized for stream operations |

**Table 1: Comparison of Transform Node Types**

## Implementation Example

### Batching Transform (Stream-based)

```csharp
using NPipeline;
using NPipeline.Nodes;
using NPipeline.Pipeline;

/// <summary>
/// Groups individual items into batches for efficient processing.
/// Demonstrates IStreamTransformNode for stream-based transformations.
/// </summary>
public sealed class BatchingTransform<T> : IStreamTransformNode<T, IReadOnlyCollection<T>>
{
    private readonly int _batchSize;
    private readonly TimeSpan _timeout;

    public BatchingTransform(int batchSize, TimeSpan timeout)
    {
        _batchSize = batchSize;
        _timeout = timeout;
    }

    public IExecutionStrategy ExecutionStrategy { get; set; } = new BatchingExecutionStrategy();
    public INodeErrorHandler? ErrorHandler { get; set; }

    public IAsyncEnumerable<IReadOnlyCollection<T>> ExecuteAsync(
        IAsyncEnumerable<T> items,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Use BatchAsync extension method for efficient batching
        return items.BatchAsync(_batchSize, _timeout, cancellationToken);
    }
}
```

### Unbatching Transform (Stream-based)

```csharp
using NPipeline;
using NPipeline.Nodes;
using NPipeline.Pipeline;

/// <summary>
/// Flattens a stream of batches back to individual items.
/// Demonstrates reverse operation to batching transform.
/// </summary>
public sealed class UnbatchingTransform<T> : IStreamTransformNode<IEnumerable<T>, T>
{
    public IExecutionStrategy ExecutionStrategy { get; set; } = new UnbatchingExecutionStrategy();
    public INodeErrorHandler? ErrorHandler { get; set; }

    public IAsyncEnumerable<T> ExecuteAsync(
        IAsyncEnumerable<IEnumerable<T>> batches,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Use FlattenAsync extension method for efficient unbatching
        return batches.FlattenAsync(cancellationToken);
    }
}
```

## Implementation Example

A transform that squares each incoming number:

```csharp
using NPipeline;
using NPipeline.Nodes;
using NPipeline.Pipeline;

/// <summary>
/// Basic transform that squares each incoming integer.
/// Demonstrates the simplest possible transform implementation
/// with synchronous work using Task.FromResult to avoid async overhead.
/// </summary>
public sealed class SquareTransform : ITransformNode<int, int>
{
    /// <summary>
    /// Processes each integer by squaring it.
    /// Uses Task.FromResult for synchronous work to avoid unnecessary Task allocation.
    /// </summary>
    public Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
    {
        // Synchronous calculation - no async work needed
        return Task.FromResult(item * item);
    }
}
```

```csharp
using NPipeline;
using NPipeline.Nodes;
using NPipeline.Pipeline;

/// <summary>
/// Transform that enriches order data with customer information.
/// Demonstrates async transform pattern with external service dependency.
/// </summary>
public sealed class EnrichmentTransform : ITransformNode<Order, EnrichedOrder>
{
    private readonly ILookupService _lookupService;

    public EnrichmentTransform(ILookupService lookupService)
    {
        _lookupService = lookupService;
    }

    /// <summary>
    /// Enriches each order with customer information from lookup service.
    /// Uses async/await pattern as external service call is inherently asynchronous.
    /// </summary>
    public async Task<EnrichedOrder> ExecuteAsync(Order order, PipelineContext context, CancellationToken cancellationToken)
    {
        // Fetch customer data from external service (async operation)
        var customerInfo = await _lookupService.GetCustomerAsync(order.CustomerId, cancellationToken);
        
        // Create enriched order with customer details
        return new EnrichedOrder
        {
            OrderId = order.OrderId,
            CustomerId = order.CustomerId,
            CustomerName = customerInfo.Name,
            Total = order.Total
        };
    }
}

// Supporting types for the example
public record Order(string OrderId, string CustomerId, decimal Total);
public record EnrichedOrder(string OrderId, string CustomerId, string CustomerName, decimal Total);
public interface ILookupService
{
    Task<CustomerInfo> GetCustomerAsync(string customerId, CancellationToken cancellationToken);
}
public record CustomerInfo(string Name);
```

```csharp
using NPipeline;
using NPipeline.Nodes;
using NPipeline.Pipeline;

/// <summary>
/// Transform that validates input data and converts to output format.
/// Demonstrates ValueTask optimization for synchronous validation scenarios.
/// In pipelines with high throughput, this eliminates Task allocations for validation.
/// </summary>
public sealed class ValidationTransform : ITransformNode<RawData, ValidatedData>
{
    /// <summary>
    /// Validates raw data and converts to validated format.
    /// Uses ValueTask for synchronous validation to avoid heap allocation.
    /// </summary>
    public ValueTask<ValidatedData> ExecuteAsync(RawData item, PipelineContext context, CancellationToken cancellationToken)
    {
        // Synchronous validation - check for empty or null values
        if (string.IsNullOrWhiteSpace(item.Value))
        {
            throw new InvalidOperationException("Value cannot be empty or null");
        }

        // Create validated data with trimmed value
        var validated = new ValidatedData
        {
            Id = item.Id,
            Value = item.Value.Trim()
        };

        // Return via ValueTask - no heap allocation for synchronous work
        return new ValueTask<ValidatedData>(validated);
    }
}

// Supporting types for the example
public record RawData(string Id, string Value);
public record ValidatedData(string Id, string Value);
```

```csharp
using NPipeline;
using NPipeline.Nodes;
using NPipeline.Pipeline;

/// <summary>
/// Transform that converts string values to integers.
/// Demonstrates ValueTask pattern for simple type conversions
/// that can fail and throw exceptions.
/// </summary>
public sealed class ConversionTransform : ITransformNode<string, int>
{
    /// <summary>
    /// Converts string to integer with validation.
    /// Uses ValueTask to avoid allocation when conversion succeeds synchronously.
    /// </summary>
    public ValueTask<int> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
    {
        // Try to parse the string to integer
        if (int.TryParse(item, out var result))
        {
            // Success case: return immediately via ValueTask (no allocation)
            return new ValueTask<int>(result);
        }

        // Failure case: throw exception for invalid format
        throw new FormatException($"Cannot convert '{item}' to integer");
    }
}
```

## Performance Considerations

Transform nodes support both `Task<T>` and `ValueTask<T>` return types. For high-throughput
scenarios where transforms often complete synchronously (cache hits, simple calculations),
`ValueTask<T>` can eliminate up to 90% of garbage collection pressure.

**See:** [Synchronous Fast Paths and ValueTask Optimization](../../advanced-topics/synchronous-fast-paths.md)
for complete implementation guide, including critical constraints, real-world examples, and dangerous patterns you must understand.

## Common Transform Patterns

### Data Enrichment

```csharp
public sealed class EnrichmentTransform : ITransformNode<Order, EnrichedOrder>
{
    private readonly ILookupService _lookupService;

    public EnrichmentTransform(ILookupService lookupService)
    {
        _lookupService = lookupService;
    }

    public async Task<EnrichedOrder> ExecuteAsync(Order order, PipelineContext context, CancellationToken cancellationToken)
    {
        var customerInfo = await _lookupService.GetCustomerAsync(order.CustomerId, cancellationToken);
        return new EnrichedOrder
        {
            OrderId = order.OrderId,
            CustomerId = order.CustomerId,
            CustomerName = customerInfo.Name,
            Total = order.Total
        };
    }
}
```

### Validation and Filtering

For validation and filtering transforms that can complete synchronously, consider using `ValueTask<T>` for optimal performance. See the [Synchronous Fast Paths guide](../../advanced-topics/synchronous-fast-paths.md) for complete implementation details and critical constraints.

## Next Steps

* **[Sink Nodes](sink-nodes.md)**: Learn how to consume and finalize data at the end of your pipeline
* **[Performance Hygiene](../../advanced-topics/performance-hygiene.md)**: Dive deeper into optimization techniques
* **[Synchronous Fast Paths](../../advanced-topics/synchronous-fast-paths.md)**: Master the `ValueTask<T>` pattern in detail

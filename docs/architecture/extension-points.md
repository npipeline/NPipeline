---
title: Extension Points
description: Customization mechanisms in NPipeline.
sidebar_position: 9
---

# Extension Points

NPipeline provides multiple ways to extend and customize its behavior without modifying the core framework.

## Custom Nodes

Build your own source, transform, or sink nodes:

```csharp
// Custom Source
public class DatabaseSourceNode : ISourceNode<Order>
{
    private readonly string _connectionString;

    public DatabaseSourceNode(string connectionString)
    {
        _connectionString = connectionString;
    }

    public IDataPipe<Order> Initialize(
        PipelineContext context,
        CancellationToken cancellationToken = default)
    {
        async IAsyncEnumerable<Order> StreamOrders(
            [EnumeratorCancellation] CancellationToken ct)
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(ct);
            
            using var command = new SqlCommand("SELECT * FROM Orders", connection);
            using var reader = await command.ExecuteReaderAsync(ct);
            
            while (await reader.ReadAsync(ct))
            {
                yield return new Order
                {
                    Id = reader.GetInt32(0),
                    Amount = reader.GetDecimal(1),
                    // ...
                };
            }
        }

        return new StreamingDataPipe<Order>(StreamOrders(cancellationToken), "DatabaseSource");
    }
}
```

```csharp
// Custom Transform
public class EnrichmentTransform : ITransformNode<Order, EnrichedOrder>
{
    private readonly IEnrichmentService _enrichmentService;

    public EnrichmentTransform(IEnrichmentService enrichmentService)
    {
        _enrichmentService = enrichmentService;
    }

    public async Task<EnrichedOrder> ExecuteAsync(
        Order input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var enrichedData = await _enrichmentService.EnrichAsync(input, cancellationToken);
        return new EnrichedOrder
        {
            Order = input,
            EnrichedData = enrichedData
        };
    }
}
```

```csharp
// Custom Sink
public class MetricsCollectorSink : ISinkNode<Result>
{
    private readonly IMetricsCollector _metrics;

    public MetricsCollectorSink(IMetricsCollector metrics)
    {
        _metrics = metrics;
    }

    public async Task ExecuteAsync(
        IDataPipe<Result> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var activity = context.Tracer.CurrentActivity;
        
        await foreach (var result in input.WithCancellation(cancellationToken))
        {
            _metrics.RecordSuccess(result.ProcessingTimeMs);
            await Task.Delay(100); // Simulate processing
        }
    }
}
```

## Build-Time Roslyn Analyzers

NPipeline includes built-in Roslyn analyzers that provide compile-time validation of your pipeline configurations. These analyzers detect common mistakes before they reach production.

**Key Analyzer:**

- **NP9002** - Detects incomplete resilience configurations where `RestartNode` is used without required prerequisites

The analyzer framework is designed to be extensible, allowing community contributions of additional analyzers for NPipeline-specific patterns.

**Learn more:** [Build-Time Resilience Analyzer Guide](../analyzers/resilience.md)

---

## Custom Execution Strategies

Create custom node execution strategies:

```csharp
public class ThrottledExecutionStrategy : INodeExecutionStrategy
{
    private readonly int _maxConcurrent;
    private readonly SemaphoreSlim _semaphore;

    public ThrottledExecutionStrategy(int maxConcurrent)
    {
        _maxConcurrent = maxConcurrent;
        _semaphore = new SemaphoreSlim(maxConcurrent);
    }

    public async Task ExecuteAsync(
        Func<CancellationToken, Task> work,
        CancellationToken cancellationToken)
    {
        await _semaphore.WaitAsync(cancellationToken);
        try
        {
            await work(cancellationToken);
        }
        finally
        {
            _semaphore.Release();
        }
    }
}
```

## Context Data

Store and retrieve arbitrary data in pipeline context using the `Items` dictionary:

```csharp
// Store data
var context = PipelineContext.Default;
context.Items["startTime"] = DateTime.UtcNow;
context.Items["userId"] = 12345;
context.Items["requestId"] = Guid.NewGuid();

// Access in transform
public async Task<Output> ExecuteAsync(
    Input input,
    PipelineContext context,
    CancellationToken cancellationToken)
{
    var startTime = (DateTime)context.Items["startTime"];
    var userId = (int)context.Items["userId"];
    var requestId = (Guid)context.Items["requestId"];

    return new Output
    {
        UserId = userId,
        RequestId = requestId,
        ProcessingTime = DateTime.UtcNow - startTime
    };
}
```

For type-safe access with null handling:

```csharp
// Safe access with TryGetValue
if (context.Items.TryGetValue("startTime", out var startTimeObj) && startTimeObj is DateTime startTime)
{
    // Use startTime
}
```

## Composite Patterns

Combine extensions for complex behaviors:

```csharp
public class ResilientTransform : ITransformNode<T, T>
{
    private readonly ITransformNode<T, T> _inner;
    private readonly IRetryPolicy _retryPolicy;
    private readonly IFallbackProvider<T> _fallback;
    private readonly IDiagnostics _diagnostics;

    public ResilientTransform(
        ITransformNode<T, T> inner,
        IRetryPolicy retryPolicy,
        IFallbackProvider<T> fallback,
        IDiagnostics diagnostics)
    {
        _inner = inner;
        _retryPolicy = retryPolicy;
        _fallback = fallback;
        _diagnostics = diagnostics;
    }

    public async IAsyncEnumerable<T> ProcessAsync(
        T input,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        try
        {
            _diagnostics.LogAttempt(_inner.GetType().Name);
            
            await foreach (var result in _retryPolicy.ExecuteAsync(
                () => _inner.ProcessAsync(input, cancellationToken),
                cancellationToken))
            {
                _diagnostics.LogSuccess(_inner.GetType().Name);
                yield return result;
            }
        }
        catch (Exception ex)
        {
            _diagnostics.LogFailure(_inner.GetType().Name, ex);
            
            var fallback = await _fallback.GetFallbackAsync(input, ex);
            yield return fallback;
        }
    }
}
```

## Next Steps

- **[Design Principles](design-principles.md)** - Understand guiding principles for extensions
- **[Advanced Testing Pipelines](../extensions/testing/advanced-testing.md)** - Test your custom nodes

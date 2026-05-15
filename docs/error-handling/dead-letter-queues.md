---
title: "Dead-Letter Queues"
description: "Capture failed items for later inspection instead of losing them."
order: 6
---

# Dead-Letter Queues

When a resilience policy returns `ResilienceDecision.DeadLetter`, the failed item is routed to a **dead-letter sink** rather than being discarded or crashing the pipeline. This preserves failed items for debugging, reprocessing, or alerting — without blocking the pipeline from continuing.

## When Items Are Dead-Lettered

An item reaches the dead-letter sink when:

1. A resilience policy explicitly returns `ResilienceDecision.DeadLetter`.
2. A retry policy exhausts its maximum retries and its `exhaustedDecision` is `DeadLetter` (the default for the fluent builder's `.Retry()` method).

```csharp
// After 3 failed retries, the item goes to dead-letter
ResiliencePolicyBuilder
    .ForNode<MyTransform, Order>()
    .On<TimeoutException>().Retry(maxRetries: 3)  // exhausted → DeadLetter
    .Build();
```

## The DeadLetterEnvelope

Every dead-lettered item is wrapped in a `DeadLetterEnvelope` that captures full failure context:

```csharp
public sealed record DeadLetterEnvelope(
    object Item,                        // The item that failed
    Exception Error,                    // The exception that caused the failure
    NodeFailureAttribution Attribution  // Where the failure originated and was decided
);
```

The `NodeFailureAttribution` record provides traceability:

```csharp
public sealed record NodeFailureAttribution(
    string OriginNodeId,       // Node where the exception was thrown
    string DecisionNodeId,     // Node where the error handling decision was made
    Guid OriginPipelineId,     // Pipeline containing the origin node
    Guid DecisionPipelineId,   // Pipeline containing the decision node
    Guid? RunId = null,        // Pipeline run identifier
    Guid? CorrelationId = null,// Item-level correlation ID
    int RetryCount = 0         // Number of retry attempts before dead-lettering
);
```

## The IDeadLetterSink Interface

Implement `IDeadLetterSink` to control where dead-lettered items go:

```csharp
public interface IDeadLetterSink
{
    Task HandleAsync(
        DeadLetterEnvelope envelope,
        PipelineContext context,
        CancellationToken cancellationToken);
}
```

## Built-In: BoundedInMemoryDeadLetterSink

NPipeline ships a memory-bounded dead-letter sink for development and testing:

```csharp
using NPipeline.ErrorHandling;

// Default: holds up to 1000 items
var sink = new BoundedInMemoryDeadLetterSink();

// Custom capacity for high-volume scenarios
var highCapacity = new BoundedInMemoryDeadLetterSink(capacity: 5000);

// Reduced capacity for memory-constrained environments
var constrained = new BoundedInMemoryDeadLetterSink(capacity: 100);
```

**Behavior:**

- Items are stored in a thread-safe `ConcurrentQueue`.
- When capacity is reached, the next dead-letter attempt throws an `InvalidOperationException`, failing the pipeline.
- Access collected items via the `Items` property (`IReadOnlyCollection<DeadLetterEnvelope>`).

Register it on the builder:

```csharp
builder.AddDeadLetterSink(new BoundedInMemoryDeadLetterSink());
```

Or register by type (resolved via DI):

```csharp
builder.AddDeadLetterSink<BoundedInMemoryDeadLetterSink>();
```

## Custom Dead-Letter Sinks

For production, implement `IDeadLetterSink` to route failures to durable storage:

```csharp
public class DatabaseDeadLetterSink : IDeadLetterSink
{
    private readonly IDbConnection _connection;

    public DatabaseDeadLetterSink(IDbConnection connection)
    {
        _connection = connection;
    }

    public async Task HandleAsync(
        DeadLetterEnvelope envelope,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        await _connection.ExecuteAsync(
            "INSERT INTO dead_letters (item, error, origin_node, retry_count, timestamp) " +
            "VALUES (@Item, @Error, @Origin, @Retries, @Timestamp)",
            new
            {
                Item = JsonSerializer.Serialize(envelope.Item),
                Error = envelope.Error.ToString(),
                Origin = envelope.Attribution.OriginNodeId,
                Retries = envelope.Attribution.RetryCount,
                Timestamp = DateTime.UtcNow
            });
    }
}
```

Other common implementations:

- **File-based:** Write JSON lines to a dead-letter file for batch reprocessing.
- **Message queue:** Publish to a separate queue for retry workflows.
- **Alerting:** Send to a monitoring system when items start dead-lettering.

## Inspecting Dead-Lettered Items

With `BoundedInMemoryDeadLetterSink`, inspect failures after pipeline execution:

```csharp
var deadLetterSink = new BoundedInMemoryDeadLetterSink();
builder.AddDeadLetterSink(deadLetterSink);

// ... run pipeline ...

foreach (var envelope in deadLetterSink.Items)
{
    Console.WriteLine($"Failed item from node '{envelope.Attribution.OriginNodeId}':");
    Console.WriteLine($"  Error: {envelope.Error.Message}");
    Console.WriteLine($"  Retry attempts: {envelope.Attribution.RetryCount}");
    Console.WriteLine($"  Item: {envelope.Item}");
}
```

## Complete Example

```csharp
public class ResilientOrderPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Configure retries with delay
        builder.WithRetryOptions(options => options with { MaxItemRetries = 3 }
            .WithExponentialBackoffAndFullJitter());

        // Dead-letter sink captures failures for inspection
        builder.AddDeadLetterSink(new BoundedInMemoryDeadLetterSink(capacity: 500));

        // Policy: retry transient errors, dead-letter validation errors
        var policy = ResiliencePolicyBuilder
            .ForNode<ValidateOrder, Order>()
            .On<HttpRequestException>().Retry(3)
            .On<ValidationException>().DeadLetter()
            .OnAny().Fail()
            .Build();

        builder.AddResiliencePolicy(policy);

        var source = builder.AddSource<OrderSource, Order>("orders");
        var validate = builder.AddTransform<ValidateOrder, Order, Order>("validate");
        var save = builder.AddSink<OrderSink, Order>("save");

        validate.WithResilience(builder);

        builder.Connect(source, validate);
        builder.Connect(validate, save);
    }
}
```

In this pipeline:

- Transient HTTP errors are retried up to 3 times with exponential backoff.
- If retries are exhausted, the item is dead-lettered (default exhausted decision).
- Validation errors are dead-lettered immediately (no retry).
- Any other exception fails the pipeline.

## Next Steps

- [Materialization](materialization.md) — buffer items to enable node-level restart
- [Retry Strategies](retry-strategies.md) — configure delays between retry attempts
- [Resilience Policies](resilience-policies.md) — customize which items get dead-lettered

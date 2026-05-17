---
title: "Testing"
description: "Test harness, in-memory nodes, mock nodes, assertion libraries, and advanced testing patterns."
order: 9
---

# Testing

The `NPipeline.Extensions.Testing` package provides utilities for testing pipelines: a fluent test harness, in-memory source/sink nodes, mock nodes, and error capture. Two companion packages add assertion methods for [FluentAssertions](#fluentassertions) and [AwesomeAssertions](#awesomeassertions).

## Installation

```bash
dotnet add package NPipeline.Extensions.Testing

# Pick one assertion library:
dotnet add package NPipeline.Extensions.Testing.FluentAssertions
# or
dotnet add package NPipeline.Extensions.Testing.AwesomeAssertions
```

## Test Harness

`PipelineTestHarness<TPipeline>` is the primary entry point for testing:

```csharp
var result = await new PipelineTestHarness<OrderPipeline>()
    .WithParameter("date", DateTime.Today)
    .CaptureErrors()
    .RunAsync();

result.ShouldBeSuccessful();
result.ShouldHaveNoErrors();
```

### Configuration Methods

```csharp
harness.WithParameter("key", value)          // set context parameter
harness.WithParameters(dictionary)            // set multiple parameters
harness.WithContextItem("key", value)         // set context item
harness.WithExecutionObserver(observer)       // attach observer
harness.CaptureErrors(ResilienceDecision.Skip) // capture errors instead of throwing
```

### PipelineExecutionResult

| Property | Type | Description |
|----------|------|-------------|
| `Success` | `bool` | Whether the pipeline completed without unhandled errors |
| `Duration` | `TimeSpan` | Total execution time |
| `Errors` | `IReadOnlyList<Exception>` | Captured exceptions |
| `Context` | `PipelineContext` | The pipeline context after execution |

## In-Memory Nodes

### InMemorySourceNode\<T>

Provides items from a collection as a source stream:

```csharp
var source = builder.AddInMemorySource(new[]
{
    new Order { Id = 1, Amount = 100 },
    new Order { Id = 2, Amount = 200 }
}, "test-source");
```

### InMemorySinkNode\<T>

Captures all items written to a sink for inspection:

```csharp
var sink = builder.AddInMemorySink<Order>("test-sink");

// After pipeline runs...
var items = context.GetInMemorySink<Order>().Items;
```

### MockNode\<TIn, TOut>

Injects custom transform logic for testing:

```csharp
var mock = builder.AddMockNode<Order, EnrichedOrder>(
    (order, ctx, ct) => Task.FromResult(new EnrichedOrder
    {
        Id = order.Id,
        Total = order.Amount * 1.1m
    }),
    "mock-enrich");
```

### PassThroughTransformNode\<T>

No-op transform for pipeline structure testing:

```csharp
var pass = builder.AddPassThrough<Order>("pass");
```

## Builder Extensions

```csharp
builder.AddInMemorySource<T>(items, name?)        // IEnumerable<T> → source
builder.AddInMemorySourceNode<T>(items, name?)     // alias
builder.AddMockNode<TIn, TOut>(transformLogic, name?)
builder.AddInMemorySink<T>(name?)
builder.AddPassThrough<T>(name?)
```

## Context Extensions

```csharp
// Retrieve in-memory sink from context
var sink = context.GetInMemorySink<Order>();

// Safe try-get
if (context.TryGetInMemorySink<Order>(out var sink))
{
    // ...
}
```

## Testing Nodes with DI Dependencies

Use a mocking framework (e.g., Moq) to inject dependencies:

```csharp
[Fact]
public async Task EmailNode_SendsNotification()
{
    var mockEmailService = new Mock<IEmailService>();
    mockEmailService
        .Setup(s => s.SendAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
        .ReturnsAsync(true);

    var node = new EmailNotificationNode(mockEmailService.Object);

    var result = await new PipelineTestHarness<NotificationPipeline>()
        .WithServiceOverride<IEmailService>(mockEmailService.Object)
        .RunAsync();

    result.ShouldBeSuccessful();
    mockEmailService.Verify(
        s => s.SendAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
        Times.AtLeastOnce);
}
```

## Testing Error Handling

Use `CaptureErrors()` to test resilience and error paths:

```csharp
[Fact]
public async Task Pipeline_CapturesFormatErrors()
{
    var result = await new PipelineTestHarness<OrderPipeline>()
        .WithParameter("source", new[]
        {
            new RawOrder { Id = "abc" },  // invalid ID
            new RawOrder { Id = "123" }   // valid
        })
        .CaptureErrors()
        .RunAsync();

    result.ShouldHaveCapturedErrors(1);
    result.Errors[0].Should().BeOfType<FormatException>();
}

[Fact]
public async Task Pipeline_HandlesEmptySource()
{
    var result = await new PipelineTestHarness<OrderPipeline>()
        .WithParameter("source", Array.Empty<RawOrder>())
        .RunAsync();

    result.ShouldBeSuccessful();
    result.ShouldHaveNoErrors();
}
```

## Parameterized Tests

Use `[Theory]` and `[InlineData]` for data-driven testing:

```csharp
[Theory]
[InlineData(100, true)]
[InlineData(0, false)]
[InlineData(-1, false)]
public async Task OrderValidation_ChecksAmount(decimal amount, bool shouldPass)
{
    var result = await new PipelineTestHarness<ValidationPipeline>()
        .WithParameter("source", new[] { new Order { Amount = amount } })
        .CaptureErrors()
        .RunAsync();

    if (shouldPass)
        result.ShouldBeSuccessful();
    else
        result.ShouldHaveCapturedErrors(1);
}
```

## FluentAssertions

`NPipeline.Extensions.Testing.FluentAssertions` adds assertion methods using [FluentAssertions](https://fluentassertions.com/):

### Sink Assertions

```csharp
sink.ShouldHaveReceived(5);
sink.ShouldContain(o => o.Id == 1);
sink.ShouldContain(expectedOrder);
sink.ShouldNotContain(unexpectedOrder);
sink.ShouldOnlyContain(o => o.Amount > 0);
```

### Result Assertions

```csharp
result.ShouldBeSuccessful();
result.ShouldFail();
result.ShouldHaveNoErrors();
result.ShouldHaveCapturedErrors(3);
result.ShouldHaveExecutedInUnder(TimeSpan.FromSeconds(5));
```

All result assertions return `PipelineExecutionResult` for fluent chaining:

```csharp
result.ShouldBeSuccessful()
      .ShouldHaveNoErrors()
      .ShouldHaveExecutedInUnder(TimeSpan.FromSeconds(10));
```

## AwesomeAssertions

`NPipeline.Extensions.Testing.AwesomeAssertions` provides the same assertion methods using [AwesomeAssertions](https://github.com/AwesomeAssertions/AwesomeAssertions). The API is identical to the FluentAssertions version.

## Best Practices

| Practice | Rationale |
|----------|-----------|
| **One behavior per test** | Isolate failures - test one node or one path per `[Fact]` |
| **Test success and failure paths** | Always test the error path, not just the happy path |
| **Use `[Theory]` for variants** | Data-driven tests are more maintainable than duplicated `[Fact]`s |
| **Mock external services** | Use Moq/NSubstitute for I/O dependencies (HTTP, DB, email) |
| **Keep tests fast** | Avoid real I/O - use `InMemorySourceNode` and `InMemorySinkNode` |
| **Test pipeline structure** | Use `PassThroughTransformNode` to verify wiring without logic |
| **Capture errors explicitly** | Always call `.CaptureErrors()` when testing error paths |
| **Assert on sink contents** | Use `ShouldContain` / `ShouldHaveReceived` for output verification |

## See Also

- [Testing Pipelines Guide](../testing/testing-pipelines.md) - step-by-step walkthrough
- [Test Utilities Guide](../testing/test-utilities.md) - advanced testing patterns
- [Extensions Overview](index.md)

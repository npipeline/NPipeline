---
title: "Testing Pipelines"
description: "Unit test individual nodes and integration test full pipelines with the testing extension."
order: 2
---

# Testing Pipelines

> **Prerequisites:** [Defining Pipelines](../guides/defining-pipelines.md), [Custom Nodes](../guides/custom-nodes.md)

The `NPipeline.Extensions.Testing` package provides test harnesses, in-memory nodes, and assertion helpers for testing pipelines and individual nodes.

## Installation

```bash
dotnet add package NPipeline.Extensions.Testing
dotnet add package NPipeline.Extensions.Testing.AwesomeAssertions  # or FluentAssertions
```

## Unit Testing a Node

Test transform nodes directly:

```csharp
[Fact]
public async Task TransformAsync_ValidOrder_ReturnsEnrichedOrder()
{
    var node = new EnrichOrder(new HttpClient());
    var context = PipelineContext.Default;

    var result = await node.TransformAsync(
        new Order(1, "Widget"), context, CancellationToken.None);

    result.Should().NotBeNull();
    result.OrderId.Should().Be(1);
}
```

## Integration Testing a Pipeline

Use `PipelineTestHarness<T>` to run a full pipeline with captured results:

```csharp
[Fact]
public async Task OrderPipeline_ProcessesAllOrders()
{
    var result = await new PipelineTestHarness<OrderPipeline>()
        .WithParameter("inputPath", "TestData/orders.csv")
        .CaptureErrors()
        .RunAsync();

    result.AssertSuccess();
    result.AssertNoErrors();
    result.Duration.Should().BeLessThan(TimeSpan.FromSeconds(10));
}
```

## In-Memory Test Nodes

Replace real data sources and sinks with in-memory implementations:

```csharp
public class TestPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddInMemorySource<Order>("orders");
        var transform = builder.AddTransform<ProcessOrder, Order, Result>("process");
        var sink = builder.AddSink<InMemorySinkNode<Result>, Result>("results");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}

[Fact]
public async Task Pipeline_ProcessesOrders()
{
    var context = PipelineContext.Default;
    context.SetSourceData(new[] { new Order(1), new Order(2) });

    var runner = PipelineRunner.Create();
    await runner.RunAsync<TestPipeline>(context);

    var sink = context.GetSink<Result>();
    sink.Items.Should().HaveCount(2);
}
```

### Available Test Nodes

| Node | Description |
|------|-------------|
| `InMemorySourceNode<T>` | Emits items from a list or from context |
| `InMemorySinkNode<T>` | Collects items into a thread-safe `ConcurrentBag<T>` |
| `MockNode<TIn, TOut>` | Configurable transform via delegate |
| `ExceptionThrowingNode<TIn>` | Always throws — for error handling tests |
| `PassThroughTransformNode<TIn, TOut>` | Identity transform with optional type conversion |

## Assertion Extensions

The `AwesomeAssertions` (or `FluentAssertions`) package adds pipeline-specific assertions:

```csharp
// Pipeline result assertions
result.ShouldBeSuccessful();
result.ShouldFail();
result.ShouldHaveNoErrors();

// Sink assertions
sink.ShouldHaveReceived<Result>(2);
sink.ShouldContain<Result>(r => r.Status == "Processed");
sink.ShouldNotContain<Result>(r => r.Status == "Failed");
```

## Test Utilities

| Utility | Purpose |
|---------|---------|
| `CapturingErrorHandler` | Wraps a resilience policy, captures exceptions for assertions |
| `CapturingLogger` | Captures all log entries for verification |
| `TestPipelineRunner` | `RunAndGetResultAsync<TDef, TResult>()` — runs and extracts sink items |

## Next Steps

- [Testing Extension Reference](../extensions/testing.md) — test harness, in-memory nodes, and assertions API
- [Test Utilities](test-utilities.md) — detailed reference for all test helpers
- [Error Handling](../error-handling/index.md) — test error handling behavior

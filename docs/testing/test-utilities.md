---
title: "Test Utilities Reference"
description: "Reference for all test helpers, in-memory nodes, and assertion extensions in NPipeline.Extensions.Testing."
order: 3
---

# Test Utilities Reference

> **Prerequisites:** [Testing Pipelines](testing-pipelines.md)

This page is a reference for all test utilities provided by `NPipeline.Extensions.Testing`.

## PipelineTestHarness\<T\>

Fluent test harness that runs a pipeline and captures results:

```csharp
var result = await new PipelineTestHarness<MyPipeline>()
    .WithParameter("key", value)
    .WithContextItem("shared", sharedState)
    .CaptureErrors()
    .RunAsync();
```

| Method | Description |
|--------|-------------|
| `WithParameter(key, value)` | Set a runtime parameter |
| `WithParameters(dict)` | Set multiple parameters |
| `WithContextItem(key, value)` | Set a shared context item |
| `CaptureErrors()` | Install `CapturingErrorHandler` to collect exceptions |
| `RunAsync()` | Execute the pipeline and return `PipelineExecutionResult` |
| `Context` | Access the `PipelineContext` directly |
| `CapturedErrors` | Exceptions captured by `CaptureErrors()` |

## PipelineExecutionResult

```csharp
record PipelineExecutionResult(
    bool Success,
    TimeSpan Duration,
    IReadOnlyList<Exception> Errors,
    PipelineContext Context);
```

### Result Assertions

| Method | Description |
|--------|-------------|
| `AssertSuccess()` | Throws if the pipeline failed |
| `AssertFailure()` | Throws if the pipeline succeeded |
| `AssertNoErrors()` | Throws if any errors were captured |
| `AssertErrorOfType<T>()` | Throws if no error of specified type |
| `AssertErrorCount(n)` | Throws if error count doesn't match |

## In-Memory Nodes

### InMemorySourceNode\<T\>

Emits items from a pre-seeded list or resolves from `PipelineContext`:

```csharp
// Pre-seeded in builder
builder.AddInMemorySource<Order>(new[] { order1, order2 });
builder.AddInMemorySource<Order>("source-name", items);

// From context
context.SetSourceData<Order>(items);      // by type
context.SetSourceData<Order>(items, "id"); // by node ID
```

### InMemorySinkNode\<T\>

Thread-safe sink that collects all received items:

```csharp
var sink = context.GetSink<Result>();
sink.Items;      // ConcurrentBag<T> of all received items
sink.Completion; // Task that completes when the sink finishes
```

### MockNode\<TIn, TOut\>

Transform node with a configurable delegate:

```csharp
var mock = new MockNode<Order, Result>(
    (order, context, ct) => Task.FromResult(new Result(order.Id)));
```

### ExceptionThrowingNode\<TIn\>

Always throws — use for testing error handling:

```csharp
var node = new ExceptionThrowingNode<Order>(new InvalidOperationException("test"));
```

## Context Extensions

| Method | Description |
|--------|-------------|
| `context.SetSourceData<T>(items)` | Store items for `InMemorySourceNode<T>` |
| `context.SetSourceData<T>(items, nodeId)` | Store items for a specific named node |
| `context.GetSink<T>()` | Retrieve an `InMemorySinkNode<T>` from context |

## Capturing Utilities

### CapturingErrorHandler

Wraps a resilience policy to capture exceptions without changing pipeline behavior:

```csharp
harness.CaptureErrors(); // installs automatically
var errors = harness.CapturedErrors;
```

### CapturingLogger

`ILogger` implementation that captures all log entries:

```csharp
var logger = new CapturingLogger();
var entries = logger.Entries; // List<LogEntry>
entries.Should().Contain(e => e.LogLevel == LogLevel.Error);
```

## Assertion Libraries

Both AwesomeAssertions and FluentAssertions packages provide the same API:

```csharp
// Sink assertions
sink.ShouldHaveReceived<T>(count);
sink.ShouldContain<T>(predicate);
sink.ShouldContain<T>(item);
sink.ShouldNotContain<T>(predicate);
sink.ShouldOnlyContain<T>(predicate);

// Result assertions
result.ShouldBeSuccessful();
result.ShouldFail();
result.ShouldHaveNoErrors();
```

## Next Steps

- [Testing Extension Reference](../extensions/testing.md) — test harness, in-memory nodes, and assertions API
- [Testing Pipelines](testing-pipelines.md) — practical testing guide
- [Error Handling](../error-handling/index.md) — test resilience behavior

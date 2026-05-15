---
title: "Defining Pipelines"
description: "How to define pipelines using IPipelineDefinition and the PipelineBuilder fluent API."
order: 2
---

# Defining Pipelines

> **Prerequisites:** [Your First Pipeline](../getting-started/your-first-pipeline.md), [Key Concepts](../getting-started/key-concepts.md)

Every NPipeline pipeline is a directed acyclic graph (DAG) of [nodes](../reference/glossary.md#node) connected by typed edges. You define the graph in a `Define` method, then hand it to a runner for execution.

## The IPipelineDefinition Interface

The recommended approach is a class that implements `IPipelineDefinition`:

```csharp
using NPipeline.Pipeline;

public class OrderPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<OrderSource, Order>("orders");
        var validate = builder.AddTransform<ValidateOrder, Order, ValidatedOrder>("validate");
        var sink = builder.AddSink<OrderSink, ValidatedOrder>("save");

        builder.Connect(source, validate);
        builder.Connect(validate, sink);
    }
}
```

Each `Add*` method returns a **typed handle** (`SourceNodeHandle<T>`, `TransformNodeHandle<TIn, TOut>`, `SinkNodeHandle<TIn>`). Handles carry type information so `Connect` can verify at compile time that the output type of one node matches the input type of the next.

## Running a Pipeline

Use `PipelineRunner` to execute a definition:

```csharp
var runner = PipelineRunner.Create();
await runner.RunAsync<OrderPipeline>();
```

`PipelineRunner.Create()` builds a runner with default services. You can also pass a `PipelineContext` and cancellation token:

```csharp
var context = PipelineContext.Default;
await runner.RunAsync<OrderPipeline>(context, cancellationToken);
```

Or run a pre-instantiated definition (useful when the definition has constructor parameters):

```csharp
var definition = new OrderPipeline(someConfig);
await runner.RunAsync(definition, context, cancellationToken);
```

## The PipelineBuilder API

### Registering Nodes

| Method | Returns | Purpose |
|--------|---------|---------|
| `AddSource<TNode, TOut>(name?)` | `SourceNodeHandle<TOut>` | Register a source node |
| `AddTransform<TNode, TIn, TOut>(name?)` | `TransformNodeHandle<TIn, TOut>` | Register an item-by-item transform |
| `AddStreamTransform<TNode, TIn, TOut>(name?)` | `TransformNodeHandle<TIn, TOut>` | Register a stream-level transform |
| `AddSink<TNode, TIn>(name?)` | `SinkNodeHandle<TIn>` | Register a sink node |
| `AddJoin<TNode, TIn1, TIn2, TOut>(name?)` | `JoinNodeHandle<TIn1, TIn2, TOut>` | Register a join node |
| `AddAggregate<TNode, TIn, TKey, TResult>(name?)` | `AggregateNodeHandle<TIn, TResult>` | Register an aggregate node |

Node names are optional but recommended — they appear in logs, metrics, and validation errors.

### Connecting Nodes

```csharp
builder.Connect(source, transform);   // source output → transform input
builder.Connect(transform, sink);     // transform output → sink input
```

`Connect` is type-safe. This won't compile:

```csharp
var strings = builder.AddSource<StringSource, string>("strings");
var numbers = builder.AddTransform<DoubleIt, int, int>("double");
builder.Connect(strings, numbers); // Compile error: string ≠ int
```

### Fan-out (One Source, Multiple Targets)

Call `Connect` multiple times from the same source:

```csharp
builder.Connect(source, analytics);
builder.Connect(source, notifications);
builder.Connect(source, mainProcessor);
```

Each downstream node receives its own copy of the stream.

## Building Without Running

Use `Build()` to create a `Pipeline` object without executing it:

```csharp
var pipeline = builder.Build();  // validates and returns Pipeline
```

For validation without throwing, use `TryBuild`:

```csharp
if (builder.TryBuild(out var pipeline, out var result))
{
    // pipeline is valid
}
else
{
    foreach (var error in result.Errors)
        Console.WriteLine(error);
}
```

## Configuring Resilience in the Definition

The builder exposes configuration methods for error handling, retry, and circuit breakers directly in the definition:

```csharp
public void Define(PipelineBuilder builder, PipelineContext context)
{
    var source = builder.AddSource<OrderSource, Order>("orders");
    var transform = builder.AddTransform<ProcessOrder, Order, Result>("process");
    var sink = builder.AddSink<ResultSink, Result>("results");

    builder.Connect(source, transform);
    builder.Connect(transform, sink);

    // Enable resilient execution on a specific node
    builder.WithResilience(transform);
    builder.WithRetryOptions(transform, new PipelineRetryOptions { MaxRetries = 3 });

    // Pipeline-wide circuit breaker
    builder.WithCircuitBreaker(failureThreshold: 10, openDuration: TimeSpan.FromSeconds(30));
}
```

> 🔗 **See also:** [Error Handling](../error-handling/index.md) for the full resilience model.

## Next Steps

- [Lambda Nodes](lambda-nodes.md) — define nodes inline without separate classes
- [Custom Nodes](custom-nodes.md) — write your own source, transform, and sink nodes
- [Pipeline Context](pipeline-context.md) — share state and configuration across nodes
- [Dependency Injection](dependency-injection.md) — wire pipelines into a DI container

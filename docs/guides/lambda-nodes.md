---
title: "Lambda Nodes"
description: "Define lightweight inline nodes using lambdas instead of dedicated classes."
order: 3
---

# Lambda Nodes

> **Prerequisites:** [Defining Pipelines](defining-pipelines.md)

For simple transformations, creating a full class for every [node](../reference/glossary.md#node) is overkill. Lambda nodes let you define sources, transforms, and sinks inline as functions.

## Lambda Sources

Provide a factory that returns the data to emit:

```csharp
// From a synchronous collection
var source = builder.AddSource(() => new[] { 1, 2, 3, 4, 5 }, "numbers");

// From an async stream (preferred for large or I/O-bound data)
var source = builder.AddSource(
    (CancellationToken ct) => ReadLinesAsync("data.txt", ct),
    "file-lines");
```

The synchronous overload accepts `Func<IEnumerable<T>>`. The async overload accepts `Func<CancellationToken, IAsyncEnumerable<T>>` and streams items lazily.

## Lambda Transforms

Map each input item to an output:

```csharp
// Synchronous - no allocations via ValueTask fast path
var doubled = builder.AddTransform((int x) => x * 2, "double");

// Asynchronous - for I/O-bound work
var enriched = builder.AddTransform(
    async (Order order, CancellationToken ct) =>
    {
        var details = await _api.GetDetailsAsync(order.Id, ct);
        return new EnrichedOrder(order, details);
    },
    "enrich");
```

The synchronous overload (`Func<TIn, TOut>`) uses an optimized `ValueTask` path internally, avoiding per-item `Task` allocations.

## Lambda Sinks

Consume each item as it arrives:

```csharp
// Synchronous
var logger = builder.AddSink((string line) => Console.WriteLine(line), "console");

// Asynchronous
var dbSink = builder.AddSink(
    async (Order order, CancellationToken ct) =>
    {
        await _db.InsertAsync(order, ct);
    },
    "database");
```

## Complete Example

A full pipeline using only lambda nodes:

```csharp
public class PricingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(
            () => new[] { 100m, 250m, 50m, 75m }, "prices");

        var discount = builder.AddTransform(
            (decimal price) => price * 0.9m, "apply-discount");

        var format = builder.AddTransform(
            (decimal price) => $"${price:F2}", "format");

        var print = builder.AddSink(
            (string price) => Console.WriteLine($"Final: {price}"), "display");

        builder.Connect(source, discount);
        builder.Connect(discount, format);
        builder.Connect(format, print);
    }
}
```

## Mixing Lambdas with Class-Based Nodes

Lambda nodes and class-based nodes are interchangeable in the same pipeline. Use lambdas for simple operations and classes for complex logic:

```csharp
public void Define(PipelineBuilder builder, PipelineContext context)
{
    var source = builder.AddSource(() => LoadOrders(), "orders");
    var validate = builder.AddTransform<OrderValidator, Order, ValidatedOrder>("validate");
    var log = builder.AddSink(
        (ValidatedOrder o) => Console.WriteLine($"Processed: {o.Id}"), "log");

    builder.Connect(source, validate);
    builder.Connect(validate, log);
}
```

## When to Use Lambda Nodes

| Use Lambdas When | Use Classes When |
|-----------------|-----------------|
| Logic fits in 1–3 lines | Transform has complex branching or state |
| No constructor dependencies needed | Node needs injected services |
| Prototyping or scripting | Node is reused across pipelines |
| The transform is purely functional | Node holds resources requiring disposal |

> 💡 **Tip:** You can also pass a pre-configured node instance directly: `builder.AddSource(myNodeInstance, "name")` or `builder.AddSink(mySinkInstance, "name")`.

## Next Steps

- [Custom Nodes](custom-nodes.md) - write full node classes for complex logic
- [Defining Pipelines](defining-pipelines.md) - the builder API in detail
- [Branching and Merging](branching-and-merging.md) - fan-out patterns

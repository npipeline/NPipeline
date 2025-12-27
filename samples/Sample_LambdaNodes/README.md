# Lambda Nodes Sample

This sample demonstrates the simplified lambda-based syntax for creating NPipeline pipelines without defining separate node classes.

## Overview

Instead of creating custom node classes for simple operations, you can use lambda functions directly with the pipeline builder. This reduces boilerplate and is
perfect for:

- Quick prototyping
- Simple transformations
- Testing and debugging
- Operations that won't be reused

## Examples Included

### 1. Simple Synchronous Pipeline

Generate numbers → Double → Add constant → Print

Shows how to use lambda functions for simple transformations without defining separate classes.

### 2. Asynchronous Pipeline

Async read → Async count → Async log

Demonstrates async lambda nodes that respect cancellation tokens and handle I/O operations.

### 3. Hybrid Approach

Extracted testable functions with lambda syntax

Shows the best practice of extracting logic into separate functions that can be tested independently while still using lambda syntax in the pipeline.

### 4. Error Handling

Parse with error handling → Filter → Display

Demonstrates how lambda nodes can include error handling logic inline.

### 5. Complex Transformations

Product objects → Apply discount → Create sales data → Display

Shows working with complex objects and multi-step transformations.

## Key Features

- **Zero boilerplate**: No need to create separate node classes for simple operations
- **Readable code**: Transformation logic is visible right where it's used
- **Both sync and async**: Support for both synchronous and asynchronous operations
- **Testable**: Extract logic into functions for independent unit testing
- **Type-safe**: Full generic type safety with compile-time checking

## Usage Pattern

```csharp
public void Define(PipelineBuilder builder, PipelineContext context)
{
    // Source from a factory
    var source = builder.AddSource(
        () => Enumerable.Range(1, 10),
        "numbers");

    // Synchronous transform
    var transform = builder.AddTransform(
        (int x) => x * 2,
        "double");

    // Asynchronous transform
    var asyncTransform = builder.AddTransformAsync(
        async (int x, ct) =>
        {
            await SomeAsyncOperation(x, ct);
            return x + 1;
        },
        "async");

    // Sink to an action
    var sink = builder.AddSink(
        (int x) => Console.WriteLine(x),
        "console");

    // Connect
    builder.Connect(source, transform);
    builder.Connect(transform, asyncTransform);
    builder.Connect(asyncTransform, sink);
}
```

## Running the Sample

```bash
dotnet run
```

## When to Use Lambda Nodes

### ✅ Use Lambda Nodes For:

- Simple, stateless transformations
- Quick prototyping
- Single-use operations
- Testing and debugging

### ❌ Use Class-Based Nodes For:

- Complex business logic
- Stateful operations
- Reusable components
- Complex configuration

## See Also

- [Lambda Nodes Documentation](../../docs/core-concepts/lambda-nodes.md)
- [Custom Nodes Documentation](../../docs/core-concepts/nodes/)
- [Basic Pipeline Sample](../Sample_BasicPipeline/)

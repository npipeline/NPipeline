---
title: Pipeline Definition (IPipelineDefinition, PipelineBuilder)
description: Learn how to define and construct your data pipelines using IPipelineDefinition and fluent PipelineBuilder API.
sidebar_position: 5
---

# Pipeline Definition: `IPipelineDefinition` and `PipelineBuilder`

Defining a pipeline in NPipeline involves specifying the sequence of nodes and how they connect. NPipeline provides two primary ways to achieve this: implementing `IPipelineDefinition` interface for a class-based approach, or using the fluent `PipelineBuilder` API for direct construction.

## `IPipelineDefinition`: Class-based Pipeline Definition

For more complex or reusable pipeline structures, you can define your pipeline by implementing `IPipelineDefinition` interface. This allows you to encapsulate the pipeline's structure within a dedicated class, making it easier to manage and test. It is particularly useful for complex pipelines or when using dependency injection.

### Interface Definition

```csharp
public interface IPipelineDefinition
{
    void Define(PipelineBuilder builder, PipelineContext context);
}
```

* **`Define`**: This method is where you add your sources, transforms, and sinks to the provided `builder`. The `context` parameter allows for more dynamic pipeline construction based on runtime parameters.

### Implementation Example

Let's define a pipeline that sources strings, transforms them to uppercase, and then sinks them to the console:

```csharp
using NPipeline;
using NPipeline.Nodes;
using NPipeline.Pipeline;

// Re-using nodes from Quick Start
public sealed class HelloWorldSource : ISourceNode<string>
{
    public IDataPipe<string> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        static IAsyncEnumerable<string> Stream()
        {
            return Generate();

            async IAsyncEnumerable<string> Generate()
            {
                yield return "Hello World!";
            }
        }

        return new StreamingDataPipe<string>(Stream());
    }
}

public sealed class UppercaseTransform : ITransformNode<string, string>
{
    public async Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
    {
        return item.ToUpperInvariant();
    }
}

public sealed class ConsoleSink : ISinkNode<string>
{
    public async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            Console.WriteLine(item);
        }
    }
}

public sealed class MyHelloWorldPipelineDefinition : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var sourceHandle = builder.AddSource<HelloWorldSource, string>();
        var transformHandle = builder.AddTransform<UppercaseTransform, string, string>();
        var sinkHandle = builder.AddSink<ConsoleSink, string>();

        builder.Connect(sourceHandle, transformHandle);
        builder.Connect(transformHandle, sinkHandle);
    }
}
```

### Executing with PipelineRunner

Instead of calling `Build()` yourself, you pass the definition type to a `PipelineRunner`. The runner is responsible for creating the pipeline from your definition and executing it.

```csharp
public static async Task Main(string[] args)
{
    var runner = PipelineRunner.Create();

    Console.WriteLine("Starting pipeline from definition...");
    await runner.RunAsync<MyHelloWorldPipelineDefinition>();
    Console.WriteLine("Pipeline finished.");
}
```

This approach separates the *definition* of the pipeline from its *execution*, leading to cleaner, more maintainable code.

## `PipelineBuilder`: Fluent API for Direct Construction

The `PipelineBuilder` offers a fluent, expressive API for constructing pipelines directly in your code. This is often preferred for simpler pipelines or when you want to define a pipeline ad-hoc.

### Key Methods

* `new PipelineBuilder()`: Creates a new pipeline builder instance.
* `AddSource<TNode, TOut>()`: Adds a source node to the pipeline and returns a handle.
* `AddTransform<TNode, TIn, TOut>()`: Adds a transform node and returns a handle. The input type `TIn` must match the output type of the connected source.
* `AddSink<TNode, TIn>()`: Adds a sink node and returns a handle. The input type `TIn` must match the output type of the connected node.
* `Connect(handle1, handle2)`: Connects two node handles in the pipeline.
* `Build()`: Finalizes the pipeline definition and returns a `Pipeline` instance ready for execution.

### Example Usage

Here's how to use PipelineBuilder to create and execute a pipeline:

```csharp
using NPipeline;
using NPipeline.Nodes;
using NPipeline.Pipeline;

// Assume HelloWorldSource, UppercaseTransform, and ConsoleSink are defined as above

public static class Program
{
    public static async Task Main(string[] args)
    {
        var runner = PipelineRunner.Create();

        // Execute the pipeline definition
        await runner.RunAsync<HelloWorldPipelineDefinition>();

        Console.WriteLine("Pipeline finished.");
    }
}

// Pipeline definition that uses PipelineBuilder
public sealed class HelloWorldPipelineDefinition : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var sourceHandle = builder.AddSource<HelloWorldSource, string>();
        var transformHandle = builder.AddTransform<UppercaseTransform, string, string>();
        var sinkHandle = builder.AddSink<ConsoleSink, string>();

        builder.Connect(sourceHandle, transformHandle);
        builder.Connect(transformHandle, sinkHandle);
    }
}
```

The key insight is that even when using PipelineBuilder directly, you still need to wrap it in an IPipelineDefinition class to execute it with PipelineRunner. This provides a consistent execution model while still allowing you to use the fluent PipelineBuilder API.

Alternatively, you can create the pipeline manually using PipelineFactory:

```csharp
using NPipeline;
using NPipeline.Nodes;
using NPipeline.Pipeline;

// Assume HelloWorldSource, UppercaseTransform, and ConsoleSink are defined as above

public static class Program
{
    public static async Task Main(string[] args)
    {
        // Create a pipeline manually
        var context = new PipelineContext();
        var factory = new PipelineFactory();
        var builder = new PipelineBuilder();

        var sourceHandle = builder.AddSource<HelloWorldSource, string>();
        var transformHandle = builder.AddTransform<UppercaseTransform, string, string>();
        var sinkHandle = builder.AddSink<ConsoleSink, string>();

        builder.Connect(sourceHandle, transformHandle);
        builder.Connect(transformHandle, sinkHandle);

        var pipeline = builder.Build();

        // Execute using a custom runner that accepts a Pipeline instance
        var runner = PipelineRunner.Create();

        // Since PipelineRunner only works with IPipelineDefinition,
        // we need to create a wrapper definition
        await runner.RunAsync(() => pipeline);

        Console.WriteLine("Pipeline finished.");
    }
}
```

## Choosing Your Approach

* **`IPipelineDefinition`:** Ideal for complex, multi-stage pipelines that benefit from being defined in their own class, promoting reusability and separation of concerns. Useful when integrating with dependency injection frameworks where pipeline definitions can be registered and resolved.
* **`PipelineBuilder` within `IPipelineDefinition`:** Excellent for simple, straightforward pipelines or when you want to use the fluent API. Even when using PipelineBuilder directly, you still need to wrap it in an IPipelineDefinition class for execution.

Both approaches ultimately use the `PipelineRunner` with an `IPipelineDefinition` for execution, but differ in how you structure your pipeline definition code.

## When to Use IPipelineDefinition

* **Complex Pipelines**: For pipelines with many nodes or complex branching and joining logic, encapsulating the definition in a class improves organization.
* **Dependency Injection**: When your nodes have dependencies that need to be injected, `IPipelineDefinition` is the preferred approach as it integrates cleanly with DI containers.
* **Reusability**: If you have common pipeline structures that you want to reuse, you can create base definition classes.

## Related Topics

* **[Pipeline Execution](pipeline-execution/index.md)**: Learn how to run your defined pipelines.
* **[Pipeline Context](pipeline-context.md)**: Understand how to pass state and configuration to your pipeline nodes.
* **[Pipeline](ipipeline.md)**: Learn about the executable instance of your data pipeline.



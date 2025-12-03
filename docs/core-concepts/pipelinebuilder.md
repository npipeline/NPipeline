---
title: PipelineBuilder
description: Learn how to use the PipelineBuilder to fluently define and construct your pipeline.
sidebar_position: 5
slug: /core-concepts/pipelinebuilder
---

# PipelineBuilder

## Prerequisites

Before using PipelineBuilder, you should be familiar with:
- [Nodes Overview](./nodes/index.md) - Understanding the node types you'll be connecting
- [Core Concepts Overview](./index.md) - Basic NPipeline concepts and terminology

The `PipelineBuilder` is a fluent API that provides a simple and expressive way to define the structure of your data pipeline. It is the primary tool for adding nodes, connecting them, and compiling the final, runnable `IPipeline` instance.

## The Core Workflow

Building a pipeline with `PipelineBuilder` involves three main steps:

1. **Add Nodes**: Use methods like `AddSource`, `AddTransform`, and `AddSink` to register the processing units of your pipeline.
2. **Connect Nodes**: Use the `Connect` method to define the flow of data between the nodes you have added.
3. **Build the Pipeline**: Call the `Build` method to validate your configuration and create an executable `IPipeline` instance.

## Step 1: Adding Nodes

You start by creating a `PipelineBuilder` instance and then adding your nodes. Each `Add` method returns a `Handle` object, which is a lightweight reference to the node that you will use in the next step to create connections.

* `AddSource<TNode, TOut>(name)`: Adds a source node.
* `AddTransform<TNode, TIn, TOut>(name)`: Adds a transform node.
* `AddSink<TNode, TIn>(name)`: Adds a sink node.

Each method takes the node's class type as a generic parameter and an optional unique name.

### Example

```csharp
using NPipeline;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

/// <summary>
/// Source node that produces simple string messages.
/// Demonstrates basic source pattern for starting a pipeline.
/// </summary>
public sealed class HelloWorldSource : SourceNode<string>
{
    public override IDataPipe<string> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        // Create streaming data pipe immediately (synchronous operation)
        return new StreamingDataPipe<string>(GenerateMessages());

        static async IAsyncEnumerable<string> GenerateMessages()
        {
            // Generate a sequence of greeting messages
            string[] messages = { "Hello", "World", "from", "NPipeline" };
            
            foreach (var message in messages)
            {
                yield return message;
                // Small delay to simulate work or external dependency
                await Task.Delay(100, cancellationToken);
            }
        }
    }
}

/// <summary>
/// Transform that converts strings to uppercase.
/// Demonstrates basic synchronous transform pattern.
/// </summary>
public sealed class UppercaseTransform : ITransformNode<string, string>
{
    public Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
    {
        // Synchronous string manipulation - no async work needed
        var uppercase = item.ToUpperInvariant();
        return Task.FromResult(uppercase);
    }
}

/// <summary>
/// Sink node that outputs messages to console.
/// Demonstrates terminal node pattern for pipeline output.
/// </summary>
public sealed class ConsoleSink : ISinkNode<string>
{
    public async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
    {
        // Process each message as it arrives from upstream
        await foreach (var message in input.WithCancellation(cancellationToken))
        {
            Console.WriteLine(message);
        }
    }
}

/// <summary>
/// Pipeline definition that connects source, transform, and sink nodes.
/// Demonstrates the fluent API pattern for building executable pipelines.
/// </summary>
public sealed class HelloWorldPipelineDefinition : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Add nodes to pipeline and get handles for connection
        var sourceHandle = builder.AddSource<HelloWorldSource, string>("message_source");
        var transformHandle = builder.AddTransform<UppercaseTransform, string, string>("uppercase_transform");
        var sinkHandle = builder.AddSink<ConsoleSink, string>("console_sink");

        // Define data flow by connecting nodes in sequence
        builder.Connect(sourceHandle, transformHandle);
        builder.Connect(transformHandle, sinkHandle);
    }
}

public static class Program
{
    public static async Task Main(string[] args)
    {
        // Create pipeline runner to execute the defined pipeline
        var runner = PipelineRunner.Create();
        
        // Run the pipeline using the definition
        await runner.RunAsync<HelloWorldPipelineDefinition>();
    }
}
```

```csharp
using NPipeline;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

/// <summary>
/// Pipeline definition demonstrating the complete fluent API workflow.
/// Shows the three-step process: Add nodes, Connect them, Build pipeline.
/// </summary>
public sealed class CompletePipelineDefinition : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Step 1: Add nodes and store their handles
        var sourceHandle = builder.AddSource<HelloWorldSource, string>("source");
        var transformHandle = builder.AddTransform<UppercaseTransform, string, string>("transform");
        var sinkHandle = builder.AddSink<ConsoleSink, string>("sink");

        // Step 2: Connect nodes to define data flow
        builder.Connect(sourceHandle, transformHandle);
        builder.Connect(transformHandle, sinkHandle);

        // Step 3: Build pipeline (implicit when RunAsync is called)
        // The builder validates the graph and creates executable pipeline
    }
}

public static class Program
{
    public static async Task Main(string[] args)
    {
        // Create builder and define pipeline
        var builder = new PipelineBuilder();
        
        // Create pipeline definition
        var definition = new CompletePipelineDefinition();
        
        // Define the pipeline structure
        var context = PipelineContext.Default;
        definition.Define(builder, context);
        
        // Build the pipeline (validates graph and creates executable instance)
        var pipeline = builder.Build();
        
        // Execute the pipeline
        var runner = PipelineRunner.Create();
        await runner.RunAsync(() => pipeline);
    }
}
```

## See Also

- [Nodes Overview](./nodes/index.md) - Understanding the node types you'll be connecting
- [Pipeline Execution](./pipeline-execution/index.md) - Learn how built pipelines are executed
- [Execution Strategies](./pipeline-execution/execution-strategies.md) - Control how nodes process data
- [Error Handling Guide](./resilience/error-handling.md) - Add resilience to your pipelines
- [Pipeline Context](./pipeline-context.md) - Understanding shared state across nodes
- [Dependency Injection](../extensions/dependency-injection.md) - Using DI with PipelineBuilder

## Next Steps

- [Pipeline Context](./pipeline-context.md) - Learn about the final component that carries state across your pipeline
- [Execution Strategies](./pipeline-execution/execution-strategies.md) - Control how nodes process data
- [Error Handling Guide](./resilience/error-handling.md) - Add resilience to your pipelines
- [Dependency Injection](../extensions/dependency-injection.md) - Using DI with PipelineBuilder

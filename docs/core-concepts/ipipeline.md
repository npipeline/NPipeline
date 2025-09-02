---
title: Pipeline
description: Learn about Pipeline, the executable instance of your data pipeline.
sidebar_position: 2
slug: /core-concepts/ipipeline
---

# Pipeline

The `Pipeline` class represents the compiled, executable form of your data pipeline. It is the final object you interact with to trigger the execution of all the sources, transforms, and sinks you have defined using the [Pipeline Definition](pipeline-definition.md) approaches.

## What is a Pipeline?

Think of `Pipeline` as the "runnable" version of your pipeline definition. After you have used [`PipelineBuilder`](pipelinebuilder.md) to lay out the nodes and their connections, the `Build()` method compiles this definition into a `Pipeline` instance.

This instance encapsulates all the logic required to:

* Start source nodes to begin producing data.
* Manage the flow of data between nodes through data pipes.
* Ensure data is transformed and processed in the correct order.
* Terminate the pipeline gracefully once all data has been processed.

## Pipeline Class Definition

The `Pipeline` class is a sealed class that contains the graph definition of the pipeline:

```csharp
public sealed class Pipeline
{
    public PipelineGraph Graph { get; }

    internal Pipeline(PipelineGraph graph)
    {
        Graph = graph;
    }
}
```

## Obtaining a Pipeline Instance

The most common way to get a `Pipeline` instance is by calling the `Build()` method on a configured `PipelineBuilder`.

```csharp
using NPipeline;
using NPipeline.Nodes;
using NPipeline.Pipeline;

// 1. Configure the pipeline using the builder
var builder = new PipelineBuilder();
var sourceHandle = builder.AddSource<MySource, int>();
var transformHandle = builder.AddTransform<MyTransform, int, string>();
var sinkHandle = builder.AddSink<MySink, string>();

builder.Connect(sourceHandle, transformHandle);
builder.Connect(transformHandle, sinkHandle);

// 2. Build the executable Pipeline instance
var pipeline = builder.Build();
```

## Running the Pipeline

Pipelines are executed using the `PipelineRunner` class, which provides multiple methods for running pipelines. The `PipelineRunner` coordinates all aspects of pipeline processing, including node instantiation, execution flow, error handling, and resource management.

### Using PipelineRunner

```csharp
// Create a runner with default services
var runner = new PipelineRunner();

// Run the pipeline using a pipeline definition
await runner.RunAsync<MyPipelineDefinition>();

// Or run with a specific context
var context = new PipelineContext();
await runner.RunAsync<MyPipelineDefinition>(context);

// Or run with a cancellation token
var cts = new CancellationTokenSource();
var context = new PipelineContext(PipelineContextConfiguration.WithCancellation(cts.Token));
await runner.RunAsync<MyPipelineDefinition>(context);
```

### Cancellation

You can pass a `CancellationToken` to control pipeline execution. This is useful for long-running pipelines or for handling application shutdown events.

```csharp
var cts = new CancellationTokenSource();
var runner = new PipelineRunner();

// Start the pipeline with a cancellation token
var pipelineTask = runner.RunAsync<MyPipelineDefinition>(cts.Token);

// After some time or on a specific event, request cancellation
cts.Cancel();

// Await the task to ensure shutdown is complete
try
{
    await pipelineTask;
}
catch (OperationCanceledException)
{
    Console.WriteLine("Pipeline was successfully canceled.");
}
```

### Advanced Execution Options

The `PipelineRunner` provides several constructors for different use cases:

```csharp
// Default constructor with all default services
var runner = new PipelineRunner();

// With custom factories
var runner = new PipelineRunner(customPipelineFactory, customNodeFactory);

// With full dependency injection
var runner = new PipelineRunner(
    pipelineFactory,
    nodeFactory,
    executionCoordinator,
    infrastructureService,
    observabilitySurface);
```

## Pipeline Context

The `PipelineContext` provides the execution environment, including logging, tracing, factories, and parameters:

```csharp
// Create a context with a cancellation token
var context = new PipelineContext(PipelineContextConfiguration.WithCancellation(cancellationToken));

// Add custom items to the context
context.Items["customSetting"] = "value";

// Add custom properties
context.Properties["retryCount"] = 3;

// Run with a custom context
await runner.RunAsync<MyPipelineDefinition>(context);
```

## Related Topics

* **[Pipeline Definition](pipeline-definition.md)**: Learn how to define the structure of your pipeline using IPipelineDefinition and PipelineBuilder.
* **[Pipeline Context](pipeline-context.md)**: Understand how to pass state and configuration to your pipeline nodes.
* **[Pipeline Execution](pipeline-execution/index.md)**: Explore more advanced execution scenarios.
* **[INode](nodes/index.md)**: Understand the different types of nodes that make up a pipeline.

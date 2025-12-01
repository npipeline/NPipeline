---
title: IPipelineRunner
description: Learn about the IPipelineRunner interface, the core component for executing NPipeline data flows.
sidebar_position: 2
---

# `IPipelineRunner`

The [`IPipelineRunner`](../../../src/NPipeline/Execution/IPipelineRunner.cs) interface is the central component responsible for initiating and managing the execution of a defined NPipeline. After constructing a pipeline using the [`PipelineBuilder`](../../../src/NPipeline/Pipeline/PipelineBuilder.cs), you interact with an implementation of [`IPipelineRunner`](../../../src/NPipeline/Execution/IPipelineRunner.cs) to bring your data flow to life.

## Key Responsibilities

The [`IPipelineRunner`](../../../src/NPipeline/Execution/IPipelineRunner.cs) handles:

* **Pipeline Initialization**: Setting up the necessary infrastructure before data processing begins.
* **Data Flow Management**: Orchestrating the movement of data items through connected nodes.
* **Asynchronous Execution**: Managing the asynchronous nature of data processing within the pipeline.
* **Cancellation**: Providing mechanisms to gracefully stop pipeline execution.
* **Error Propagation**: Ensuring that errors are handled according to the pipeline's configuration.

## `RunAsync` Method

The primary method for executing a pipeline is `RunAsync`, which takes a [`PipelineContext`](../../../src/NPipeline/Pipeline/PipelineContext.cs) parameter and requires the pipeline definition type to have a parameterless constructor.

```csharp
public interface IPipelineRunner
{
    Task RunAsync<TDefinition>(PipelineContext context) where TDefinition : IPipelineDefinition, new();
}
```

* **`TDefinition`**: The type of pipeline definition to run. Must implement [`IPipelineDefinition`](../../../src/NPipeline/Abstractions/Pipeline/IPipelineDefinition.cs) and have a parameterless constructor (indicated by the `new()` constraint).
* **`context`**: The [`PipelineContext`](../../../src/NPipeline/Pipeline/PipelineContext.cs) containing runtime configuration, shared state, and cancellation tokens.
* **`new()` constraint**: This ensures the pipeline definition can be instantiated without parameters, allowing the runner to create an instance of the definition.

### Example: Basic Pipeline Execution

Here's a simple example demonstrating how to build and run a basic pipeline using `RunAsync` with the correct signature.

```csharp
using NPipeline;
using NPipeline.Nodes;
using NPipeline.Extensions.Testing;

public sealed record MyData(int Value);

public sealed class MySource : SourceNode<MyData>
{
    private readonly int _start;
    private readonly int _count;

    public MySource(int start, int count)
    {
        _start = start;
        _count = count;
    }

    public async IAsyncEnumerable<MyData> ExecuteAsync(CancellationToken cancellationToken = default)
    {
        for (int i = 0; i < _count; i++)
        {
            if (cancellationToken.IsCancellationRequested) yield break;
            var data = new MyData(_start + i);
            Console.WriteLine($"Source produced: {data.Value}");
            yield return data;
            await Task.Delay(50, cancellationToken); // Simulate some work
        }
    }
}

public sealed class MyTransform : TransformNode<MyData, MyData>
{
    public async IAsyncEnumerable<MyData> ExecuteAsync(IAsyncEnumerable<MyData> input, CancellationToken cancellationToken = default)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            if (cancellationToken.IsCancellationRequested) yield break;
            var transformedData = new MyData(item.Value * 2);
            Console.WriteLine($"Transform processed: {item.Value} -> {transformedData.Value}");
            yield return transformedData;
        }
    }
}

public sealed class MySink : SinkNode<MyData>
{
    public async Task ExecuteAsync(IAsyncEnumerable<MyData> input, CancellationToken cancellationToken = default)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            if (cancellationToken.IsCancellationRequested) break;
            Console.WriteLine($"Sink consumed: {item.Value}");
        }
    }
}

// Pipeline definition with parameterless constructor
public sealed class MyPipelineDefinition : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var sourceHandle = builder.AddSource<MySource, int>("source");
        var transformHandle = builder.AddTransform<MyTransform, int, string>("transform");
        var sinkHandle = builder.AddSink<MySink, string>("sink");

        builder.Connect(sourceHandle, transformHandle);
        builder.Connect(transformHandle, sinkHandle);
    }
}

public static class Program
{
    public static async Task Main(string[] args)
    {
        var runner = PipelineRunner.Create();
        var context = PipelineContext.Default;

        Console.WriteLine("Starting pipeline execution...");
        await runner.RunAsync<MyPipelineDefinition>(context);
        Console.WriteLine("Pipeline execution finished.");
    }
}
```

### Output

Running the above program would produce output similar to:

```text
Starting pipeline execution...
Source produced: 1
Transform processed: 1 -> 2
Sink consumed: 2
Source produced: 2
Transform processed: 2 -> 4
Sink consumed: 4
Source produced: 3
Transform processed: 3 -> 6
Sink consumed: 6
Source produced: 4
Transform processed: 4 -> 8
Sink consumed: 8
Source produced: 5
Transform processed: 5 -> 10
Sink consumed: 10
Pipeline execution finished.
```

## Cancellation

The [`CancellationToken`](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken) provided through the [`PipelineContext`](../../../src/NPipeline/Pipeline/PipelineContext.cs) is crucial for managing the lifecycle of long-running pipelines. When cancellation is requested, NPipeline attempts to gracefully shut down all active nodes, allowing them to complete any in-flight operations or clean up resources before terminating.

### Example: Cancelling a Pipeline

```csharp
using NPipeline;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;

// ... (MyData, MySource, MyTransform, MySink, MyPipelineDefinition definitions as above) ...

public static class Program
{
    public static async Task Main(string[] args)
    {
        var runner = PipelineRunner.Create();
        using var cts = new CancellationTokenSource();

        // Create context with cancellation token
        var context = PipelineContext.WithCancellation(cts.Token);

        // Start the pipeline in the background
        var pipelineTask = runner.RunAsync<MyPipelineDefinition>(context);

        Console.WriteLine("Pipeline started. Press any key to cancel...");
        Console.ReadKey(); // Wait for user input

        Console.WriteLine("Cancellation requested.");
        cts.Cancel(); // Request cancellation

        try
        {
            await pipelineTask; // Wait for the pipeline to complete or cancel
            Console.WriteLine("Pipeline execution finished gracefully.");
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Pipeline execution was cancelled.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Pipeline execution encountered an error: {ex.Message}");
        }
    }
}
```

In this example, the pipeline starts producing and processing data. When a key is pressed, cancellation is requested via the `CancellationTokenSource`. The `RunAsync` method will then propagate this cancellation request through the `PipelineContext` to all nodes, allowing them to react accordingly.

## Next Steps

* **[Pipeline Context](../pipeline-context.md)**: Understand how `PipelineContext` provides runtime information and shared resources to nodes.
* **[Error Handling](../resilience/error-handling-guide.md)**: Dive deeper into strategies for managing errors within your pipelines.


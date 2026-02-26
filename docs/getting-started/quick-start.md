---
title: Quick Start
description: Build your first NPipeline with a simple "Hello World" example.
sidebar_position: 2
---

# Quick Start: Your First NPipeline

This quick start guide will walk you through creating a basic "Hello World" pipeline using NPipeline. This example demonstrates the core concepts of defining a source, a transform, and a sink.

**Prerequisites:** NPipeline installed and a .NET project set up. If you haven't installed NPipeline yet, see [Installation](installation.md).

> **Installation Tip**
>
> For detailed installation instructions and all available packages, refer to the [Installation Guide](installation.md). The commands below assume you have created a new console project.

## Step 1: Define Your Nodes

In NPipeline, a pipeline is composed of interconnected nodes. We'll define three types for our "Hello World":

* **Source:** To produce our "Hello World" message.
* **Transform:** To modify the message (e.g., convert to uppercase).
* **Sink:** To consume and display the final message.

First, let's create a simple console application and add the necessary packages. If you haven't already, follow the [Installation Guide](installation.md) to add NPipeline and its core extension.

Now, replace the content of your `Program.cs` file with the following:

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NPipeline;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Pipeline;

namespace NpipelineHelloWorld;

// 1. Define a Source Node
// This node will produce a single "Hello World!" string.
public sealed class HelloWorldSource : SourceNode<string>
{
    public override IDataPipe<string> Initialize(
        PipelineContext context,
        CancellationToken cancellationToken = default)
    {
        Console.WriteLine("Source: Producing 'Hello World!'");
        return new StreamingDataPipe<string>(ProduceMessages());
    }

    private async IAsyncEnumerable<string> ProduceMessages()
    {
        yield return "Hello World!";
    }
}

// 2. Define a Transform Node
// This node will convert the incoming string to uppercase.
public sealed class UppercaseTransform : TransformNode<string, string>
{
    public override Task<string> ExecuteAsync(
        string item,
        PipelineContext context,
        CancellationToken cancellationToken = default)
    {
        var result = item.ToUpperInvariant();
        Console.WriteLine($"Transform: Transforming '{item}' to '{result}'");
        return Task.FromResult(result);
    }
}

// 3. Define a Sink Node
// This node will consume the final string and print it to the console.
public sealed class ConsoleSink : SinkNode<string>
{
    public override async Task ExecuteAsync(
        IDataPipe<string> input,
        PipelineContext context,
        CancellationToken cancellationToken = default)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            Console.WriteLine($"Sink: Consuming and displaying: {item}");
        }
    }
}

// 4. Define the Pipeline
public sealed class HelloWorldPipeline : IPipelineDefinition
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

public static class Program
{
    public static async Task Main(string[] args)
    {
        // Set up dependency injection
        var services = new ServiceCollection();

        // Add NPipeline services
        services.AddNPipeline(Assembly.GetExecutingAssembly());

        var serviceProvider = services.BuildServiceProvider();

        Console.WriteLine("Starting pipeline...");

        // Use DI to get the pipeline runner
        using var scope = serviceProvider.CreateScope();
        var runner = scope.ServiceProvider.GetRequiredService<IPipelineRunner>();

        await runner.RunAsync<HelloWorldPipeline>();

        Console.WriteLine("Pipeline finished.");
    }
}
```

## Step 2: Run the Pipeline

Execute your console application:

```bash
dotnet run
```

## Expected Output

You should see output similar to this:

```text
Starting pipeline...
Source: Producing 'Hello World!'
Transform: Transforming 'Hello World!' to 'HELLO WORLD!'
Sink: Consuming and displaying: HELLO WORLD!
Pipeline finished.
```

This simple example illustrates the fundamental flow of data through an NPipeline: from a source, through a transform, and finally to a sink.

## Understanding the Design: Synchronous Pipe Creation + Asynchronous Data Flow

You may have noticed something interesting about our source node:

```csharp
public override IDataPipe<string> Initialize(...)  // Notice: Not async!
{
    // Returns a pipe synchronously - no await here
    return new StreamingDataPipe<string>(Stream());
}
```

The method is called `Initialize`, not `InitializeAsync`, because it returns synchronously — no await is involved!

**Phase 1 (Synchronous):** The source creates a pipe immediately

```csharp
var pipe = source.Initialize(context, cancellationToken);  // Returns instantly
```

**Phase 2 (Asynchronous):** The sink consumes data asynchronously

```csharp
await foreach (var item in input.WithCancellation(cancellationToken))  // Async here
{
    // Process each item as it arrives
}
```

**Why This Design?**
* **Simplicity:** Pipe creation is fast and synchronous
* **Type Safety:** Direct `IDataPipe<T>` returns enable better type compatibility
* **Performance:** No unnecessary Task allocations
* **Clarity:** "ExecuteAsync" signals you're in the async pipeline system, but the pipe is ready to use immediately

Think of it like opening a file: `File.OpenRead()` is synchronous and returns immediately, but `stream.ReadAsync()` is asynchronous when you actually read data from it.

## Next Steps

* **[Core Concepts](../core-concepts/index.md)**: Deep dive into the `IDataPipe`, `INode`, `IPipelineDefinition`, and `PipelineContext`
* **[Common Patterns](../core-concepts/common-patterns.md)**: See practical examples of real-world pipeline implementations
* **[Installation](installation.md)**: Review the installation options and available extensions

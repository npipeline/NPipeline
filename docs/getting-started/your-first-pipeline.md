---
title: "Your First Pipeline"
description: "Build and run a complete NPipeline from scratch in 10 minutes."
order: 3
---

# Your First Pipeline

This tutorial walks you through building a working pipeline that generates messages, transforms them, and outputs the results. By the end, you'll understand the core pattern that every NPipeline uses.

## What You'll Build

A pipeline with three nodes:

1. **Source** - generates a list of greeting messages
2. **Transform** - converts each message to uppercase
3. **Sink** - prints each result to the console

## Step 1: Create the Project

```bash
dotnet new console -n HelloPipeline
cd HelloPipeline
dotnet add package NPipeline
```

## Step 2: Define the Nodes

Replace the contents of `Program.cs` with the following. This defines three node classes and a pipeline definition, then runs it.

```csharp
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;

// --- Node Definitions ---

// A source node produces data. It returns an IDataStream<T> containing
// all items to feed into the pipeline.
public class GreetingSource : SourceNode<string>
{
    public override IDataStream<string> OpenStream(
        PipelineContext context, CancellationToken cancellationToken)
    {
        var messages = new List<string>
        {
            "Hello World",
            "Hello NPipeline",
            "Hello Streaming"
        };

        return new InMemoryDataStream<string>(messages, "greetings");
    }
}

// A transform node receives one item at a time and returns a transformed item.
public class UppercaseTransform : TransformNode<string, string>
{
    public override Task<string> TransformAsync(
        string item, PipelineContext context, CancellationToken cancellationToken)
    {
        return Task.FromResult(item.ToUpperInvariant());
    }
}

// A sink node consumes items at the end of the pipeline.
// It receives the full stream and iterates through it.
public class ConsoleSink : SinkNode<string>
{
    public override async Task ConsumeAsync(
        IDataStream<string> input, PipelineContext context, CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            Console.WriteLine(item);
        }
    }
}

// --- Pipeline Definition ---

// An IPipelineDefinition declares which nodes exist and how they connect.
public class HelloPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<GreetingSource, string>("source");
        var transform = builder.AddTransform<UppercaseTransform, string, string>("uppercase");
        var sink = builder.AddSink<ConsoleSink, string>("console");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}

// --- Run It ---

var runner = PipelineRunner.Create();
await runner.RunAsync<HelloPipeline>();
```

## Step 3: Run the Pipeline

```bash
dotnet run
```

Expected output:

```text
HELLO WORLD
HELLO NPIPELINE
HELLO STREAMING
```

## How It Works

Every NPipeline follows the same three-step pattern:

1. **Define nodes** - classes that inherit from `SourceNode<T>`, `TransformNode<TIn, TOut>`, or `SinkNode<T>`.
2. **Define the pipeline** - a class implementing `IPipelineDefinition` that adds nodes to a builder and connects them.
3. **Run it** - create a `PipelineRunner` and call `RunAsync<T>()`.

The runner handles everything else: instantiating your nodes, wiring up the data streams between them, executing them in the correct order, and cleaning up resources afterward.

## Key Concepts in This Example

**Source nodes** produce data by returning an `IDataStream<T>` from their `OpenStream` method. The data isn't processed yet at this point - it's just made available for downstream nodes to consume.

**Transform nodes** process items one at a time. `TransformAsync` receives a single input item and returns a single output item. The framework calls this method once per item flowing through the pipeline.

**Sink nodes** consume the entire stream. They receive an `IDataStream<T>` and iterate it using `await foreach`. This is where side effects happen - writing to databases, files, or the console.

**Connections** are type-safe. When you call `builder.Connect(source, transform)`, the compiler verifies that the output type of `source` matches the input type of `transform`. A type mismatch is a compile-time error.

## A Shorter Alternative: Lambda Nodes

For simple pipelines, you can skip defining classes entirely and use inline lambda functions:

```csharp
using NPipeline.Execution;
using NPipeline.Pipeline;

public class HelloLambdaPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(
            () => new[] { "Hello World", "Hello NPipeline", "Hello Streaming" },
            "source");

        var transform = builder.AddTransform(
            (string s) => s.ToUpperInvariant(),
            "uppercase");

        var sink = builder.AddSink(
            (string s) => Console.WriteLine(s),
            "console");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}

var runner = PipelineRunner.Create();
await runner.RunAsync<HelloLambdaPipeline>();
```

Lambda nodes are ideal for quick prototyping or when a transformation is a single expression. Use class-based nodes when you need constructor injection, state, or testability.

## Next Steps

- [Key Concepts](key-concepts.md) - understand the mental model behind nodes, streams, and graphs
- [What Next?](what-next.md) - find guides for your specific use case

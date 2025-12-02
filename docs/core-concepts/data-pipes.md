---
title: Data Pipes (IDataPipe)
description: Understand how data flows through NPipeline using IDataPipe and IDataPipe&lt;T&gt;.
sidebar_position: 3
---

# Data Pipes (`IDataPipe` and `IDataPipe<T>`)

In NPipeline, data flows between nodes through **Data Pipes**. These pipes are the fundamental mechanism for transferring items from one processing stage to the next. At their core, data pipes implement the `IDataPipe` interface, with generic variations for typed data.

## `IDataPipe` and `IDataPipe<T>`

`IDataPipe` is the non-generic interface representing a data channel. It's primarily used internally for untyped operations or when the data type is not relevant to the pipe's management.

```csharp
public interface IDataPipe : IAsyncDisposable
{
    string StreamName { get; }
    Type GetDataType();
    
    // Internal method for framework use - not part of public API
    IAsyncEnumerable<object?> ToAsyncEnumerable(CancellationToken cancellationToken = default);
}
```

**Note:** The `ToAsyncEnumerable()` method is an internal framework API and should not be called by external code. External APIs should use the typed `IDataPipe<T>` interface directly, which implements `IAsyncEnumerable<T>` and is both type-safe and zero-overhead.

`IDataPipe<T>` is the generic interface, where `T` represents the type of data item flowing through that specific pipe. This is the interface you will most commonly interact with when defining the connections between your nodes. It provides methods for asynchronously producing and consuming streams of data.

```csharp
public interface IDataPipe<out T> : IDataPipe, IAsyncEnumerable<T> { }
```

Conceptually, a data pipe acts as a conduit:

* An upstream node (a Source or a Transform) **produces** data into the pipe.
* A downstream node (a Transform or a Sink) **consumes** data from the pipe.

NPipeline handles the mechanics of buffering, backpressure, and asynchronous transfer, allowing you to focus on the business logic of your nodes.

## Key Characteristics

* **Asynchronous Streaming:** Data pipes are designed for asynchronous operations, leveraging `IAsyncEnumerable<T>` to efficiently stream data items without blocking threads.
* **Decoupling:** They decouple the producing node from the consuming node, allowing for independent development and testing of pipeline stages.
* **Flow Control:** NPipeline's internal implementation of data pipes manages the flow of data, preventing downstream nodes from being overwhelmed and upstream nodes from producing too quickly.
* **Strongly Typed:** `IDataPipe<T>` ensures type safety throughout your pipeline, reducing runtime errors.
* **Stream Identification:** Each data pipe has a `StreamName` property for identification and debugging purposes.
* **Type Information:** The `GetDataType()` method allows runtime discovery of the data type flowing through the pipe.

## Example Usage (Conceptual)

While you typically don't interact directly with `IDataPipe<T>` instances when building a pipeline with `PipelineBuilder`, it's the underlying mechanism. When you connect nodes, NPipeline implicitly creates and manages these pipes.

Consider a `Transform` that takes `string` and outputs `int`:

```csharp
using NPipeline.Nodes;

public sealed class StringToIntTransform : TransformNode<string, int>
{
    public async IAsyncEnumerable<int> ExecuteAsync(IAsyncEnumerable<string> input, CancellationToken cancellationToken = default)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            if (int.TryParse(item, out var number))
            {
                yield return number;
            }
            // Optionally handle parsing errors or log them
        }
    }
}
```

In this example, the `input` parameter is an `IAsyncEnumerable<string>`, which is backed by an `IDataPipe<string>`. The `yield return number;` effectively pushes data into the output `IDataPipe<int>` managed by NPipeline.

## Consuming a Data Pipe

When you implement a node (typically a Sink or Transform), you receive data from upstream nodes via an `IDataPipe<T>`. You can consume the data using a standard `await foreach` loop:

```csharp
using NPipeline;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;

public class MySinkNode : SinkNode<MyData>
{
    public async Task ExecuteAsync(
        IDataPipe<MyData> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            // Process each item
            Console.WriteLine($"Received: {item}");
        }
    }
}
```

The `WithCancellation` extension method is a convenient way to ensure that the enumeration respects the pipeline's cancellation token.

## Creating a Data Pipe

In a source node, you are responsible for creating the initial data pipe. NPipeline provides helpers to easily create a pipe from an `IAsyncEnumerable<T>` or a simple `IEnumerable<T>`:

```csharp
using NPipeline;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

public class MySourceNode : SourceNode<MyData>
{
    public IDataPipe<MyData> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        static IAsyncEnumerable<MyData> Stream()
        {
            return Generate();

            async IAsyncEnumerable<MyData> Generate()
            {
                var data = new[]
                {
                    new MyData { Id = 1, Name = "First" },
                    new MyData { Id = 2, Name = "Second" }
                };

                foreach (var item in data)
                {
                    yield return item;
                }
            }
        }

        // Create a data pipe from an async stream
        return new StreamingDataPipe<MyData>(Stream());
    }
}
```

By abstracting the data flow into `IDataPipe<T>`, NPipeline allows you to build complex, high-performance streaming workflows with a simple and consistent programming model.

## Synchronous Pipe Creation + Asynchronous Iteration

A key architectural insight of NPipeline is the separation of concerns between **creating a pipe** and **iterating over a pipe**:

**Synchronous Creation Phase:**
```csharp
// Source creates and returns the pipe synchronously
var pipe = source.Execute(context, cancellationToken);  // Returns immediately
```

- No `await` is needed
- Pipeline structure is established instantly
- No Task allocations overhead

**Asynchronous Iteration Phase:**
```csharp
// Downstream nodes iterate asynchronously
await foreach (var item in pipe.WithCancellation(cancellationToken))
{
    // Process each item as it arrives
}
```

- Data flows asynchronously
- Backpressure is managed automatically
- Handles cancellation gracefully

**Why Separate These Phases?**

1. **Clarity:** It's immediately clear when work is synchronous (pipe setup) vs. asynchronous (data flow)
2. **Type Safety:** Enables covariance on `IDataPipe<T>` (not possible with `Task<T>` wrapper)
3. **Performance:** No unnecessary Task allocations for pipe creation
4. **Consistency:** All source nodes follow the same pattern

**Analogy - File I/O:**
```csharp
// Creating a stream is fast and synchronous
var stream = File.OpenRead(filePath);

// Reading from a stream is asynchronous
var buffer = new byte[1024];
int bytesRead = await stream.ReadAsync(buffer, 0, 1024);

// Similarly in NPipeline:
var pipe = source.Execute(...);      // Fast, synchronous
await foreach (var item in pipe) { ... }  // Asynchronous streaming
```

## Data Flow in a Pipeline

When you define a pipeline like this:

```csharp
var pipeline = new PipelineBuilder()
    .AddSource<MyStringSource, string>()
    .AddTransform<StringToIntTransform, string, int>()
    .AddSink<MyIntSink, int>()
    .Build();
```

NPipeline creates:

1. An `IDataPipe<string>` between `MyStringSource` and `StringToIntTransform`.
2. An `IDataPipe<int>` between `StringToIntTransform` and `MyIntSink`.

The data items produced by the source flow through the first pipe, are transformed, and then flow through the second pipe to the sink.

## Next Steps

* **[Nodes](nodes/index.md)**: Learn about the different types of nodes (Source, Transform, Sink) that interact with data pipes.
* **[Pipeline Definition](pipeline-definition.md)**: Understand how to connect nodes and define the overall structure of your data flow.

---
title: Pipeline Context (PipelineContext)
description: Understand how to manage and share state, configuration, and cancellation across your NPipeline nodes using PipelineContext.
sidebar_position: 7
---

# `PipelineContext`

The [`PipelineContext`](src/NPipeline/PipelineContext.cs) is a crucial component in NPipeline that provides a mechanism for sharing runtime information, services, and state across different nodes within a pipeline. It acts as a lightweight, scoped container that is passed through the pipeline during execution, allowing nodes to access common resources without explicit dependency injection in their constructors.

This context is particularly useful for:
*   **Logging**: Providing a consistent logging mechanism.
*   **Metrics**: Capturing and reporting performance metrics.
*   **Correlation IDs**: Propagating unique identifiers for tracing requests across nodes.
*   **Shared State**: Storing transient state that needs to be accessible by multiple nodes.
*   **CancellationToken**: Propagating cancellation requests throughout the pipeline.

## What is `PipelineContext`?

`PipelineContext` is a class that encapsulates runtime information relevant to a specific pipeline execution. It is automatically created when a pipeline starts its `RunAsync` method and is passed implicitly to your nodes.

Key elements managed by `PipelineContext` include:

*   **`CancellationToken`:** A primary mechanism for cooperative cancellation of the pipeline. All nodes should respect this token for graceful shutdown.
*   **Parameters:** A dictionary for holding runtime parameters passed to the pipeline during initialization.
*   **Items:** A dictionary for sharing transient state between pipeline nodes during execution.
*   **Properties:** A dictionary for storing properties that can be used by extensions and plugins, providing a way to extend PipelineContext without modifying its core structure.
*   **Arbitrary State:** You can store and retrieve any custom data or objects that need to be accessible by multiple nodes during the pipeline's execution. This is particularly useful for configuration, metrics, or shared resources.

## Constructor Parameters

The `PipelineContext` constructor accepts several optional parameters to customize the execution environment:

```csharp
public PipelineContext(
    Dictionary<string, object>? parameters = null,
    Dictionary<string, object>? items = null,
    Dictionary<string, object>? properties = null,
    PipelineRetryOptions? retryOptions = null,
    IErrorHandlerFactory? errorHandlerFactory = null,
    IPipelineErrorHandler? pipelineErrorHandler = null,
    IDeadLetterSink? deadLetterSink = null,
    IPipelineLoggerFactory? loggerFactory = null,
    IPipelineTracer? tracer = null,
    IObservabilityFactory? observabilityFactory = null,
    ILineageFactory? lineageFactory = null,
    CancellationToken cancellationToken = default)
```

The parameters are organized into logical groups:

1. **Runtime Data Dictionaries**: `Parameters`, `Items`, `Properties`
2. **Resilience Options**: `RetryOptions`
3. **Error Handling Components**: `ErrorHandlerFactory`, `PipelineErrorHandler`, `DeadLetterSink`
4. **Observability Components**: `LoggerFactory`, `Tracer`, `ObservabilityFactory`
5. **Lineage Components**: `LineageFactory`
6. **Cancellation Token**: `CancellationToken`

## Creating a `PipelineContext`

The simplest way to create a `PipelineContext` is by using its public constructor. **All unspecified components automatically use sensible defaults**, so you only need to configure what you need.

### Default Components

All of these components have built-in defaults - you don't need to explicitly set them:

* **`ErrorHandlerFactory`**: `DefaultErrorHandlerFactory` (handles errors according to pipeline settings)
* **`LineageFactory`**: `DefaultLineageFactory` (tracks data lineage)
* **`ObservabilityFactory`**: `DefaultObservabilityFactory` (provides observability hooks)
* **`LoggerFactory`**: `NullPipelineLoggerFactory` (no-op logger)
* **`Tracer`**: `NullPipelineTracer` (no-op tracer)
* **`RetryOptions`**: `PipelineRetryOptions.Default` (no retries)

### Quick Start - No Configuration Needed

For the most basic case, just create a context:

```csharp
// This gives you a fully configured context with all defaults
var context = new PipelineContext();

var runner = PipelineRunner.Create();
await runner.RunAsync<MyPipeline>(context);
```

### With Cancellation Token

The most common scenario:

```csharp
var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));

// Pass cancellation token directly through configuration
var context = new PipelineContext(PipelineContextConfiguration.WithCancellation(cts.Token));

await runner.RunAsync<MyPipeline>(context);
```

### With Multiple Configurations

For complex scenarios, use the `PipelineContextConfiguration` factory methods:

```csharp
var config = PipelineContextConfiguration.Create(
    cancellationToken: cancellationToken,
    retryOptions: new PipelineRetryOptions(maxItemRetries: 3),
    parameters: new Dictionary<string, object> { { "userId", 123 } },
    errorHandlerFactory: myCustomErrorHandlerFactory,
    loggerFactory: myLoggerFactory,
    tracer: myTracer);

var context = new PipelineContext(config);

await runner.RunAsync<MyPipeline>(context);
```

### Entry Points

You have several options depending on your needs:

| Method | When to Use |
|--------|-----------|
| `new PipelineContext()` | Simple context with all defaults |
| `new PipelineContext(PipelineContextConfiguration.WithCancellation(token))` | For cancellation token only |
| `new PipelineContext(new PipelineContextConfiguration(...))` | When you need to supply multiple constructor arguments |
| `new PipelineContext(PipelineContextConfiguration.Default with { RetryOptions = custom })` | Compose advanced configs via record `with` expressions |
| `PipelineContext.Default` | Simple read-only default context (rarely used) |

## Accessing `PipelineContext`

### Standard Access

Nodes that implement `TransformNode<TInput, TOutput>` or `SinkNode<TInput>` can access the [`PipelineContext`](src/NPipeline/PipelineContext.cs) through their `ExecuteAsync` and `ExecuteAsync` methods, respectively.

```csharp
public class ExecuteAsync<TInput, TOutput>
{
    IAsyncEnumerable<TOutput> ExecuteAsync(
        IAsyncEnumerable<TInput> input,
        PipelineContext context,
        CancellationToken cancellationToken = default);
}

public class SinkNode<TInput>
{
    Task ExecuteAsync(
        IAsyncEnumerable<TInput> input,
        PipelineContext context,
        CancellationToken cancellationToken = default);
}
```

### Context-Aware Nodes

To access the full `PipelineContext` object, your nodes can implement the `IContextAwareNode` interface.

```csharp
public interface IContextAwareNode
{
    void SetContext(PipelineContext context);
}
```

NPipeline will automatically detect if your node implements `IContextAwareNode` and inject the `PipelineContext` instance into it before the pipeline starts.

## Key Properties and Methods

The [`PipelineContext`](src/NPipeline/PipelineContext.cs) includes properties and methods for:

*   **`CancellationToken`**: A token that signals if the pipeline execution has been requested to stop. Nodes should monitor this token and cease operations if cancellation is requested.
*   **`Parameters`**: A dictionary to hold any runtime parameters for the pipeline. These are typically set during pipeline initialization and remain constant throughout execution.
*   **`Items`**: A dictionary for sharing state between pipeline nodes. This is used for transient data that needs to be accessible by multiple nodes during execution.
*   **`Properties`**: A dictionary for storing properties that can be used by extensions and plugins. This provides a way to extend PipelineContext without modifying its core structure.
*   **`LoggerFactory`**: The logger factory for this pipeline run, providing consistent logging across all nodes.
*   **`Tracer`**: The tracer for this pipeline run, enabling distributed tracing.
*   **`PipelineErrorHandler`**: The error handler for the entire pipeline.
*   **`DeadLetterSink`**: The sink for items that have failed processing and have been redirected.
*   **`ErrorHandlerFactory`**: The factory for creating error handlers and dead-letter sinks.
*   **`LineageFactory`**: The factory for creating lineage sinks and resolving lineage collectors.
*   **`ObservabilityFactory`**: The factory for resolving observability collectors.
*   **`RetryOptions`**: Execution / retry configuration for this pipeline run. Values here override builder defaults when provided.
*   **`CurrentNodeId`**: The ID of the node currently being executed. This is automatically managed by the pipeline runner.
*   **`ExecutionObserver`**: Optional execution observer for instrumentation (node lifecycle, retries, queue/backpressure events).
*   **`StateManager`**: Gets the state manager for this pipeline run, if available. This is accessed through the Properties dictionary.
*   **`StatefulRegistry`**: Gets the stateful registry for this pipeline run, if available. This is accessed through the Properties dictionary.
*   **Logging/Metrics Interfaces**: References to logging or metrics services (e.g., `ILogger`, `IMetricsRecorder`) that nodes can use to report events or data.

## State Management Capabilities

`PipelineContext` provides several methods for managing state and resources:

*   **`RegisterForDisposal(IAsyncDisposable disposable)`**: Registers an `IAsyncDisposable` resource to be disposed when the pipeline context is disposed. This ensures proper cleanup of resources created during pipeline execution. The disposal list is **lazily initialized** only when the first disposable is registered, optimizing performance for stateless pipelines that don't require resource cleanup.
*   **`ScopedNode(string nodeId)`**: Sets the `CurrentNodeId` for the duration of the returned disposable scope. This is used internally by the pipeline runner to track which node is currently executing.
*   **`TryGetStatefulRegistry(out IStatefulRegistry? registry)`**: Attempts to get the stateful registry for this pipeline run. Returns true if a stateful registry is available, false otherwise.

### Performance Note

The `RegisterForDisposal` method uses lazy initialization for its internal disposal list. This means that for stateless pipelines (those that never register any disposables), there is no allocation or disposal overhead. When disposables are registered, the list is initialized with a capacity of 8 items, which is sufficient for most typical pipelines (averaging 3-5 disposables per execution).

## Difference Between Parameters, Items, and Properties

*   **Parameters**: These are typically set during pipeline initialization and are meant to be configuration values that remain constant throughout the pipeline execution. They are used to pass configuration to the pipeline as a whole.
*   **Items**: These are used for sharing transient state between pipeline nodes during execution. Items can be modified by nodes and are meant for data that needs to be shared between different parts of the pipeline during a single run.
*   **Properties**: These are used for storing properties that can be used by extensions and plugins. They provide a way to extend PipelineContext without modifying its core structure and are typically used by framework components rather than user code.

## Example: Using `PipelineContext` for Logging

Consider a scenario where you want to log the processing of each item within a transform node, including a correlation ID for tracing.

```csharp
using NPipeline;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

// Assume ILogger is an interface for logging
public interface ILogger
{
    void LogInformation(string message, params object[] args);
}

// Assume a simple Logger implementation
public sealed class ConsoleLogger : ILogger
{
    public void LogInformation(string message, params object[] args)
    {
        Console.WriteLine($"[INFO] {string.Format(message, args)}");
    }
}

public sealed record DataItem(Guid Id, string Payload);

public sealed class MyTransformWithContext : ITransformNode<DataItem, DataItem>
{
    public async IAsyncEnumerable<DataItem> ExecuteAsync(
        IAsyncEnumerable<DataItem> input,
        PipelineContext context,
        CancellationToken cancellationToken = default)
    {
        // Retrieve logger from context (or get a default if not present)
        var logger = context.GetItem<ILogger>("Logger") ?? new ConsoleLogger();
        var correlationId = context.GetItem<Guid>("CorrelationId");

        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            if (cancellationToken.IsCancellationRequested) yield break;

            logger.LogInformation(
                "[{CorrelationId}] Processing item {ItemId} with payload: {Payload}",
                correlationId,
                item.Id,
                item.Payload);

            // Simulate some transformation
            var transformedItem = item with { Payload = item.Payload.ToUpperInvariant() };
            yield return transformedItem;
        }
    }
}

public sealed class MySource : SourceNode<DataItem>
{
    public override IDataPipe<DataItem> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken = default)
    {
        static IAsyncEnumerable<DataItem> Stream()
        {
            return Generate();

            async IAsyncEnumerable<DataItem> Generate()
            {
                for (int i = 0; i < 3; i++)
                {
                    yield return new DataItem(Guid.NewGuid(), $"item-{i}");
                }
            }
        }

        return new StreamingDataPipe<DataItem>(Stream(), "Source Data Stream");
    }
}

public sealed class MySink : SinkNode<DataItem>
{
    public override async Task ExecuteAsync(
        IDataPipe<DataItem> input,
        PipelineContext context,
        CancellationToken cancellationToken = default)
    {
        var logger = context.Items.TryGetValue("Logger", out var loggerObj) && loggerObj is ILogger loggerInstance
            ? loggerInstance
            : new ConsoleLogger();
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            logger.LogInformation("Sink consumed: {ItemId} - {Payload}", item.Id, item.Payload);
        }
    }
}

public sealed class MyPipelineDefinition : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var sourceHandle = builder.AddSource<MySource, DataItem>();
        var transformHandle = builder.AddTransform<MyTransformWithContext, DataItem, DataItem>();
        var sinkHandle = builder.AddSink<MySink, DataItem>();

        builder.Connect(sourceHandle, transformHandle);
        builder.Connect(transformHandle, sinkHandle);
    }
}

public static class Program
{
    public static async Task Main(string[] args)
    {
        var logger = new ConsoleLogger();
        var correlationId = Guid.NewGuid();

        // Create a custom PipelineContext and add shared resources
        var context = PipelineContext.Default;
        context.Items["Logger"] = logger;
        context.Items["CorrelationId"] = correlationId;

        var runner = PipelineRunner.Create();

        Console.WriteLine($"Starting pipeline with CorrelationId: {correlationId}");
        await runner.RunAsync<MyPipelineDefinition>(context); // Pass the custom context
        Console.WriteLine("Pipeline finished.");
    }
}
```

In this example, a `ConsoleLogger` and a `CorrelationId` are added to the `PipelineContext`. The `MyTransformWithContext` and `MySink` nodes then retrieve these items from the context to perform logging with the shared correlation ID.

## Example: Using `IContextAwareNode`

Let's imagine a scenario where you want to pass a configuration setting (e.g., a batch size) to multiple nodes, or log information with a unique pipeline run ID.

First, define a custom context object or simply use `PipelineContext` directly:

```csharp
using NPipeline;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;

public sealed class MyConfig
{
    public int BatchSize { get; set; } = 10;
    public Guid RunId { get; } = Guid.NewGuid();
}

public sealed class ConfigurableSource : SourceNode<string>, IContextAwareNode
{
    private PipelineContext _context;

    public void SetContext(PipelineContext context)
    {
        _context = context;
    }

    public override IDataPipe<string> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken = default)
    {
        var config = _context.GetOrAdd("MyConfig", () => new MyConfig());
        Console.WriteLine($"Source (RunId: {config.RunId}): Producing up to {config.BatchSize} items.");

        static IAsyncEnumerable<string> Stream(int batchSize)
        {
            return Generate();

            async IAsyncEnumerable<string> Generate()
            {
                for (int i = 0; i < batchSize; i++)
                {
                    yield return $"Item {i}";
                }
            }
        }

        return new StreamingDataPipe<string>(Stream(config.BatchSize), "Configurable Source Stream");
    }
}

public sealed class ContextAwareTransform : TransformNode<string, string>, IContextAwareNode
{
    private PipelineContext _context;

    public void SetContext(PipelineContext context)
    {
        _context = context;
    }

    public override async Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken = default)
    {
        var config = _context.GetOrAdd("MyConfig", () => new MyConfig()); // Retrieve the same config
        Console.WriteLine($"Transform (RunId: {config.RunId}): Transforming {item}");
        return item.ToUpperInvariant();
    }
}

public sealed class ContextAwareSink : SinkNode<string>, IContextAwareNode
{
    private PipelineContext _context;

    public void SetContext(PipelineContext context)
    {
        _context = context;
    }

    public override async Task ExecuteAsync(
        IDataPipe<string> input,
        PipelineContext context,
        CancellationToken cancellationToken = default)
    {
        var config = _context.GetOrAdd("MyConfig", () => new MyConfig()); // Retrieve the same config
        Console.WriteLine($"Sink (RunId: {config.RunId}): Consuming items.");
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            Console.WriteLine($"Sink (RunId: {config.RunId}): Received {item}");
        }
    }
}

public sealed class ContextAwarePipelineDefinition : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var sourceHandle = builder.AddSource<ConfigurableSource, string>();
        var transformHandle = builder.AddTransform<ContextAwareTransform, string, string>();
        var sinkHandle = builder.AddSink<ContextAwareSink, string>();

        builder.Connect(sourceHandle, transformHandle);
        builder.Connect(transformHandle, sinkHandle);
    }
}

public static class Program
{
    public static async Task Main(string[] args)
    {
        var runner = PipelineRunner.Create();
        await runner.RunAsync<ContextAwarePipelineDefinition>();
    }
}
```

In this example, `MyConfig` is stored in `PipelineContext` using a key ("MyConfig"). Each `IContextAwareNode` can then retrieve this shared configuration. The `GetOrAdd` method ensures that the object is created only once per pipeline run if it doesn't already exist for that key.

## Best Practices

*   **Keep it Lightweight**: Avoid adding large or frequently changing data structures to the context. For complex state management, consider dedicated state management nodes or services.
*   **Transient State:** Use `PipelineContext` for state that is specific to a single pipeline run. Avoid using it for global application state.
*   **Immutability (where possible)**: While the context itself is mutable, consider making items retrieved from it immutable or thread-safe if they are to be shared and modified across multiple concurrent nodes. Where possible, store immutable objects in context to prevent unexpected side effects from nodes modifying shared state.
*   **Clarity**: Use meaningful keys for items stored in the context to improve readability and prevent naming collisions. Consider defining static string constants for your keys.
*   **Optional Access**: Design nodes to gracefully handle cases where an expected item might not be present in the context (e.g., provide default values or log warnings).
*   **Cancellation:** Always respect the `CancellationToken` provided by the `PipelineContext` (or directly via method parameters) to ensure your nodes can respond to cancellation requests.

## Related Topics

*   **[Streaming vs. Buffering](streaming-vs-buffering.md)**: Understand how NPipeline handles data flow and memory management.
*   **[Error Handling](resilience/error-handling.md)**: Learn about NPipeline's error handling mechanisms.


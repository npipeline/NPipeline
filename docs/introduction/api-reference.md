---
title: API Quick Reference
description: Quick lookup for NPipeline's main APIs.
sidebar_position: 13
---

# API Quick Reference

## Core Interfaces

### IPipelineDefinition

**Purpose:** Define pipeline structure and connections.

```csharp
public interface IPipelineDefinition
{
    void Define(PipelineBuilder builder, PipelineContext context);
}
```

**Implement to:**

- Create custom pipelines
- Define node connections
- Configure pipeline topology

**Example:**

```csharp
using NPipeline.Pipeline;

public class MyPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<MySource, MyData>();
        var transform = builder.AddTransform<MyTransform, MyData, ProcessedData>();
        var sink = builder.AddSink<MySink, ProcessedData>();

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}
```

### PipelineBuilder

**Purpose:** Build and connect nodes.

| Method | Returns | Purpose |
|--------|---------|---------|
| `AddSource<TNode, TOut>()` | `SourceNodeHandle<TOut>` | Create and register a source node |
| `AddTransform<TNode, TIn, TOut>()` | `TransformNodeHandle<TIn, TOut>` | Create and register a transform node |
| `AddSink<TNode, TIn>()` | `SinkNodeHandle<TIn>` | Create and register a sink node |
| `Connect<TData>(SourceNodeHandle<TData>, TransformNodeHandle<TData, TOut>)` | `PipelineBuilder` | Connect two nodes |

**Example:**

```csharp
var source = builder.AddSource<OrderSource, Order>();
var sink = builder.AddSink<OrderSink, Order>();
builder.Connect(source, sink);
```

### INode

**Purpose:** Base interface for all pipeline nodes.

All nodes implement `INode`. Use specific base classes instead:

- `SourceNode<T>` - Data source
- `TransformNode<TIn, TOut>` - Data transform
- `SinkNode<T>` - Data consumer

## Base Node Classes

### `SourceNode<T>`

**Purpose:** Produce data to pipeline.

```csharp
public abstract class SourceNode<T> : INode
{
    public abstract IDataPipe<T> ExecuteAsync(
        PipelineContext context,
        CancellationToken cancellationToken);
}
```

**Implement by:** Override `ExecuteAsync()` to return data synchronously.

**Example:**

```csharp
public class CsvSource : SourceNode<Customer>
{
    private readonly string _filePath;

    public override IDataPipe<Customer> ExecuteAsync(
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        static IAsyncEnumerable<Customer> ReadAsync(string path, CancellationToken ct)
        {
            return Read();

            async IAsyncEnumerable<Customer> Read()
            {
                using var reader = new StreamReader(path);
                string? line;
                while ((line = await reader.ReadLineAsync(ct)) != null)
                {
                    ct.ThrowIfCancellationRequested();
                    yield return ParseCsv(line);
                }
            }
        }

        return new StreamingDataPipe<Customer>(ReadAsync(_filePath, cancellationToken));
    }
}
```

### `TransformNode<TIn, TOut>`

**Purpose:** Transform data item-by-item.

```csharp
public abstract class TransformNode<TIn, TOut> : INode
{
    public abstract Task<TOut> ExecuteAsync(
        TIn item,
        PipelineContext context,
        CancellationToken cancellationToken);
}
```

**Implement by:** Override `ExecuteAsync()` for each item.

**Example:**

```csharp
public class OrderValidator : TransformNode<Order, ValidatedOrder>
{
    public override Task<ValidatedOrder> ExecuteAsync(
        Order item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        if (item.Amount <= 0)
            throw new ValidationException("Amount must be positive");

        var validated = new ValidatedOrder(item.Id, item.Amount);
        return Task.FromResult(validated);
    }
}
```

### `SinkNode<T>`

**Purpose:** Consume final data.

```csharp
using NPipeline.Nodes;
using NPipeline.DataFlow;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;

public abstract class SinkNode<T> : ISinkNode<T>
{
    public abstract Task ExecuteAsync(
        IDataPipe<T> input,
        PipelineContext context,
        CancellationToken cancellationToken);
}
```

**Implement by:** Override `ExecuteAsync()` to consume the data pipe.

**Example:**

```csharp
public class OrderSink : SinkNode<ValidatedOrder>
{
    private readonly IOrderRepository _repository;

    public override async Task ExecuteAsync(
        IDataPipe<ValidatedOrder> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        await foreach (var order in input.WithCancellation(cancellationToken))
        {
            await _repository.SaveAsync(order, cancellationToken);
        }
    }
}
```

## Core Types

### PipelineContext

**Purpose:** Shared runtime context for pipeline.

```csharp
using NPipeline.Pipeline;
using NPipeline.Observability.Logging;
using NPipeline.Observability.Tracing;

public class PipelineContext
{
    public Dictionary<string, object> Items { get; }
    public Dictionary<string, object> Parameters { get; }
    public CancellationToken CancellationToken { get; }
    public IPipelineLoggerFactory LoggerFactory { get; }
    public IPipelineTracer Tracer { get; }
    public IDeadLetterSink? DeadLetterSink { get; }
    public IPipelineErrorHandler? PipelineErrorHandler { get; }
    public Properties Properties { get; }
    public IErrorHandlerFactory ErrorHandlerFactory { get; }
    public ILineageFactory LineageFactory { get; }
    public IObservabilityFactory ObservabilityFactory { get; }
    public RetryOptions RetryOptions { get; }
    public StateManager StateManager { get; }
    public IExecutionObserver ExecutionObserver { get; }
}
```

**Key Members:**

- `Items` - Store and retrieve shared state between nodes
- `Parameters` - Input parameters for the pipeline
- `CancellationToken` - For graceful cancellation
- `LoggerFactory` - Factory to create loggers for nodes
- `Tracer` - For distributed tracing
- `DeadLetterSink` - Sink for failed items
- `PipelineErrorHandler` - Error handler for pipeline-level errors
- `Properties` - Pipeline properties
- `ErrorHandlerFactory` - Factory for creating error handlers and dead-letter sinks
- `LineageFactory` - Factory for creating lineage sinks and resolving lineage collectors
- `ObservabilityFactory` - Factory for resolving observability collectors
- `RetryOptions` - Configuration for retry behavior
- `StateManager` - Manages pipeline state
- `ExecutionObserver` - Observer for execution events

**Example:**

```csharp
var context = new PipelineContextBuilder()
    .WithCancellation(cts.Token)
    .Build();

// In a node:
var logger = context.LoggerFactory.CreateLogger("MyNode");
logger.Log(LogLevel.Information, "Processing item");

// Access shared state
if (context.Items.TryGetValue("cache", out var cacheObj))
{
    var sharedState = cacheObj as MyCache;
}
```

### IDataPipe

**Purpose:** Represents streaming data flow.

```csharp
using NPipeline.DataFlow;

public interface IDataPipe<out T> : IAsyncEnumerable<T>
{
    string StreamName { get; }
    // IDataPipe<T> implements IAsyncEnumerable<T> directly
    // Iterate using: await foreach (var item in dataPipe)
}
```

**Usage:**

```csharp
var dataPipe = sourceNode.ExecuteAsync(context, cancellationToken);
await foreach (var item in dataPipe.WithCancellation(cancellationToken))
{
    // Process item
}
```

### StreamingDataPipe

**Purpose:** Default implementation of IDataPipe.

```csharp
using NPipeline.DataFlow.DataPipes;

public class StreamingDataPipe<T> : IDataPipe<T>
{
    public StreamingDataPipe(IAsyncEnumerable<T> data, string streamName = "");
    // Implements IAsyncEnumerable<T> directly
}
```

**Create in source nodes:**

```csharp
static IAsyncEnumerable<Item> ReadAsync()
{
    return Read();

    async IAsyncEnumerable<Item> Read()
    {
        // Yield items
        yield return new Item();
    }
}
return new StreamingDataPipe<Item>(ReadAsync());
```

## Execution

### PipelineRunner

**Purpose:** Execute pipelines.

```csharp
using NPipeline.Execution;

public class PipelineRunner
{
    public Task RunAsync<TDefinition>(
        PipelineContext context,
        CancellationToken cancellationToken = default)
        where TDefinition : IPipelineDefinition, new();

    public Task RunAsync<TDefinition>(
        CancellationToken cancellationToken = default)
        where TDefinition : IPipelineDefinition, new();
}
```

**Usage:**

```csharp
var runner = PipelineRunner.Create();
var context = new PipelineContextBuilder()
    .WithCancellation(cancellationToken)
    .Build();

await runner.RunAsync<MyPipeline>(context, cancellationToken);
```

## Dependency Injection

### AddNPipeline Extension (Assembly Scanning)

**Purpose:** Register nodes and services with automatic discovery.

```csharp
public static IServiceCollection AddNPipeline(
    this IServiceCollection services,
    params Assembly[] assembliesToScan);
```

**Usage:**

```csharp
services.AddNPipeline(
    Assembly.GetExecutingAssembly(),
    typeof(ConnectorAssembly).Assembly
);
```

**Registers:**

- All `INode` implementations as Transient
- All `IPipelineDefinition` implementations
- All `IPipelineErrorHandler` and `INodeErrorHandler` implementations
- All `IDeadLetterSink`, `ILineageSink`, and `IPipelineLineageSink` implementations
- All `IPipelineLineageSinkProvider` implementations
- Pipeline infrastructure services (IPipelineFactory, IPipelineRunner, etc.)

### AddNPipeline Extension (Fluent Configuration)

**Purpose:** Register nodes and services with fine-grained control using a fluent API.

```csharp
public static IServiceCollection AddNPipeline(
    this IServiceCollection services,
    Action<NPipelineServiceBuilder> configure);
```

**Usage:**

```csharp
services.AddNPipeline(builder => builder
    .AddNode<MySourceNode>()
    .AddNode<MyTransformNode>()
    .AddNode<MySinkNode>(ServiceLifetime.Singleton)
    .AddPipeline<MyPipelineDefinition>()
    .AddErrorHandler<MyErrorHandler>()
    .AddLineageSink<MyLineageSink>()
);
```

**Benefits:**

- No reflection overhead - explicit component registration
- Clear, discoverable API via IntelliSense
- Type-safe registration with compile-time verification
- Flexible `ServiceLifetime` control per component (Transient, Scoped, Singleton)
- Can mix manual registration with assembly scanning via `ScanAssemblies()`

**NPipelineServiceBuilder Methods:**

```csharp
// Register nodes
AddNode<TNode>()
AddNode<TNode>(ServiceLifetime lifetime)

// Register pipelines
AddPipeline<TPipeline>()
AddPipeline<TPipeline>(ServiceLifetime lifetime)

// Register error handlers
AddErrorHandler<THandler>()
AddErrorHandler<THandler>(ServiceLifetime lifetime)
AddPipelineErrorHandler<THandler>()
AddPipelineErrorHandler<THandler>(ServiceLifetime lifetime)

// Register sinks
AddDeadLetterSink<TSink>()
AddDeadLetterSink<TSink>(ServiceLifetime lifetime)
AddLineageSink<TSink>()
AddLineageSink<TSink>(ServiceLifetime lifetime)
AddPipelineLineageSink<TSink>()
AddPipelineLineageSink<TSink>(ServiceLifetime lifetime)

// Register providers
AddLineageSinkProvider<TProvider>()
AddLineageSinkProvider<TProvider>(ServiceLifetime lifetime)

// Scan assemblies for implementations
ScanAssemblies(params Assembly[] assemblies)
```

**Example - Mixed Approach:**

```csharp
services.AddNPipeline(builder => builder
    // Register high-performance or singleton components manually
    .AddNode<SpecialCachedNode>(ServiceLifetime.Singleton)
    .AddPipeline<MyPipeline>()
    
    // Then scan remaining components from assemblies
    .ScanAssemblies(
        Assembly.GetExecutingAssembly(),
        typeof(PluginProvider).Assembly
    )
);

## Extensions

### Parallelism Extension

**Package:** `NPipeline.Extensions.Parallelism`

**Configure Parallel Execution:**

```csharp
using NPipeline.Extensions.Parallelism;
using NPipeline.Pipeline;

public class MyPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<MySource, MyData>();
        var transform = builder.AddTransform<MyTransform, MyData, ProcessedData>();
        var sink = builder.AddSink<MySink, ProcessedData>();

        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        // Configure parallel execution for the transform
        builder.WithParallelOptions(
            new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }
        );
    }
}
```

**Method:**

```csharp
public static PipelineBuilder WithParallelOptions(this PipelineBuilder builder, ParallelOptions options)
```

### Testing Extension

**Package:** `NPipeline.Extensions.Testing`

#### InMemorySourceNode

```csharp
using NPipeline.Extensions.Testing;

public class InMemorySourceNode<T> : SourceNode<T>
{
    public T[] Data { get; set; }
    public override IDataPipe<T> ExecuteAsync(
        PipelineContext context,
        CancellationToken cancellationToken);
}
```

**Usage:**

```csharp
var source = new InMemorySourceNode<int> { Data = new[] { 1, 2, 3 } };
```

#### `InMemorySinkNode<T>`

```csharp
public class InMemorySinkNode<T> : SinkNode<T>
{
    public Task<List<T>> Completion { get; }
    public override Task ExecuteAsync(
        IDataPipe<T> input,
        PipelineContext context,
        CancellationToken cancellationToken);
}
```

**Usage:**

```csharp
var sink = new InMemorySinkNode<int>();
// ... run pipeline ...
var results = await sink.Completion;
```

### Connectors

**CSV Connector:** `NPipeline.Connectors.Csv` (Available in initial release)

- `CsvSourceNode<T>` - Read CSV files
- `CsvSinkNode<T>` - Write CSV files

**Example:**

```csharp
var source = new CsvSourceNode<Customer>("customers.csv");
var sink = new CsvSinkNode<ProcessedCustomer>("output.csv");
```

**Excel Connector:** `NPipeline.Connectors.Excel` (Planned for future release)

- `ExcelSourceNode<T>` - Read Excel files
- `ExcelSinkNode<T>` - Write Excel files

**AWS Connector:** `NPipeline.Connectors.Aws` (Planned for future release)

- `S3SourceNode<T>` - Read from S3
- `SqsSourceNode<T>` - Read from SQS
- `S3SinkNode<T>` - Write to S3

## Common Patterns

### Create Pipeline and Run

```csharp
using NPipeline.Execution;
using NPipeline.Pipeline;

public class MyPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<MySource, MyData>();
        var transform = builder.AddTransform<MyTransform, MyData, ProcessedData>();
        var sink = builder.AddSink<MySink, ProcessedData>();

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}

// Execute
var runner = PipelineRunner.Create();
var context = new PipelineContext();
await runner.RunAsync<MyPipeline>(context);
```

### Use with Dependency Injection

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;

// Approach 1: Assembly Scanning (automatic discovery)
var services = new ServiceCollection();
services.AddLogging();
services.AddNPipeline(Assembly.GetExecutingAssembly());

var provider = services.BuildServiceProvider();
var runner = provider.GetRequiredService<IPipelineRunner>();
await runner.RunAsync<MyPipeline>();

// Approach 2: Fluent Configuration (explicit registration)
var services = new ServiceCollection();
services.AddLogging();
services.AddNPipeline(builder => builder
    .AddNode<MySourceNode>()
    .AddNode<MyTransformNode>()
    .AddNode<MySinkNode>()
    .AddPipeline<MyPipeline>()
);

var provider = services.BuildServiceProvider();
var runner = provider.GetRequiredService<IPipelineRunner>();
await runner.RunAsync<MyPipeline>();
```

### Handle Cancellation

```csharp
using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));

var context = new PipelineContext
{
    CancellationToken = cts.Token
};

try
{
    await runner.RunAsync<MyPipeline>(context, cts.Token);
}
catch (OperationCanceledException)
{
    Console.WriteLine("Pipeline was cancelled");
}
```

### Share State Between Nodes

```csharp
var context = new PipelineContext();
context.Items["cache"] = new Dictionary<int, Customer>();
context.Items["stats"] = new ProcessingStats();

// In nodes:
if (context.Items.TryGetValue("cache", out var cacheObj))
{
    var cache = cacheObj as Dictionary<int, Customer>;
}
if (context.Items.TryGetValue("stats", out var statsObj))
{
    var stats = statsObj as ProcessingStats;
}
```

### Custom Error Handling

```csharp
using NPipeline.Observability.Logging;

public override async Task<Item> ExecuteAsync(
    Item item,
    PipelineContext context,
    CancellationToken cancellationToken)
{
    var logger = context.LoggerFactory.CreateLogger("MyTransform");

    try
    {
        return await ProcessAsync(item, cancellationToken);
    }
    catch (ValidationException ex)
    {
        logger.Log(LogLevel.Warning, $"Validation failed: {ex.Message}");
        throw;
    }
    catch (Exception ex)
    {
        logger.Log(LogLevel.Error, ex, "Unexpected error");
        throw;
    }
}
```

## Quick Lookup Table

| Need | Class/Method | Package | Availability |
|------|-------------|---------|--------------|
| Define pipeline | `IPipelineDefinition` | Core | Initial Release |
| Create source | `SourceNode<T>` | Core | Initial Release |
| Create transform | `TransformNode<TIn, TOut>` | Core | Initial Release |
| Create sink | `SinkNode<T>` | Core | Initial Release |
| Run pipeline | `PipelineRunner.RunAsync<T>()` | Core | Initial Release |
| Share state | `PipelineContext.Items` | Core | Initial Release |
| Register nodes | `AddNPipeline()` | Core | Initial Release |
| Parallel execution | `WithParallelOptions()` | Extensions.Parallelism | Initial Release |
| Unit testing | `InMemorySourceNode<T>`, `InMemorySinkNode<T>` | Extensions.Testing | Initial Release |
| Read CSV | `CsvSourceNode<T>` | Connectors.Csv | Initial Release |
| Write CSV | `CsvSinkNode<T>` | Connectors.Csv | Initial Release |

## Next Steps

- **[Common Patterns](../core-concepts/common-patterns.md)** - Practical examples
- **[Architecture](../architecture/index.md)** - Deep dive into how NPipeline works
- **[Best Practices](../core-concepts/best-practices.md)** - Design guidelines
- **[Getting Started](../getting-started/quick-start.md)** - Installation and first pipeline


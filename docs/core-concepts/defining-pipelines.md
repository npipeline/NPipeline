---
title: Defining Pipelines
description: Learn how to define and construct your data pipelines using both the fluent PipelineBuilder API and class-based IPipelineDefinition approach.
sidebar_position: 5
---

# Defining Pipelines

Defining a pipeline in NPipeline involves specifying the sequence of nodes and how they connect. NPipeline provides two complementary approaches: the fluent **`PipelineBuilder`** API for direct, expressive construction, and the **`IPipelineDefinition`** interface for class-based, reusable definitions.

## The PipelineBuilder: Fluent API (Recommended for Most Cases)

The `PipelineBuilder` is a fluent API that provides a simple and expressive way to define the structure of your data pipeline. It is the primary tool for adding nodes, connecting them, and compiling the final, runnable `IPipeline` instance.

### The Core Workflow

Building a pipeline with `PipelineBuilder` involves three main steps:

1. **Add Nodes**: Use methods like `AddSource`, `AddTransform`, and `AddSink` to register the processing units of your pipeline.
2. **Connect Nodes**: Use the `Connect` method to define the flow of data between the nodes you have added.
3. **Build the Pipeline**: Call the `Build` method to validate your configuration and create an executable `IPipeline` instance.

### Key Methods

* `new PipelineBuilder()`: Creates a new pipeline builder instance.
* `AddSource<TNode, TOut>(name)`: Adds a source node to the pipeline and returns a handle.
* `AddTransform<TNode, TIn, TOut>(name)`: Adds a transform node and returns a handle. The input type `TIn` must match the output type of the connected source.
* `AddSink<TNode, TIn>(name)`: Adds a sink node and returns a handle. The input type `TIn` must match the output type of the connected node.
* `Connect(handle1, handle2)`: Connects two node handles in the pipeline.
* `Build()`: Finalizes the pipeline definition and returns a `Pipeline` instance ready for execution.

### Basic Example

Let's walk through a complete example using PipelineBuilder within an `IPipelineDefinition`:

```csharp
using NPipeline;
using NPipeline.Nodes;
using NPipeline.Pipeline;

// Define your custom nodes
public sealed class HelloWorldSource : SourceNode<string>
{
    public override IDataPipe<string> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        return new StreamingDataPipe<string>(GenerateMessages());

        static async IAsyncEnumerable<string> GenerateMessages()
        {
            string[] messages = { "Hello", "World", "from", "NPipeline" };
            
            foreach (var message in messages)
            {
                yield return message;
                await Task.Delay(100, cancellationToken);
            }
        }
    }
}

public sealed class UppercaseTransform : ITransformNode<string, string>
{
    public Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
    {
        var uppercase = item.ToUpperInvariant();
        return Task.FromResult(uppercase);
    }
}

public sealed class ConsoleSink : ISinkNode<string>
{
    public async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
    {
        await foreach (var message in input.WithCancellation(cancellationToken))
        {
            Console.WriteLine(message);
        }
    }
}

// Define the pipeline using the fluent PipelineBuilder API
public sealed class HelloWorldPipelineDefinition : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Step 1: Add nodes and store their handles
        var sourceHandle = builder.AddSource<HelloWorldSource, string>("message_source");
        var transformHandle = builder.AddTransform<UppercaseTransform, string, string>("uppercase_transform");
        var sinkHandle = builder.AddSink<ConsoleSink, string>("console_sink");

        // Step 2: Connect nodes to define data flow
        builder.Connect(sourceHandle, transformHandle);
        builder.Connect(transformHandle, sinkHandle);

        // Step 3: Build pipeline (implicit when RunAsync is called by PipelineRunner)
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

### When to Use PipelineBuilder

* **Simple to moderate pipelines**: For straightforward data flows with a manageable number of nodes
* **Quick prototyping**: When you want to get a pipeline running quickly
* **Fluent, expressive API**: When you prefer readable, method-chaining style code
* **Ad-hoc definitions**: When the pipeline structure is unlikely to be reused

---

## The IPipelineDefinition: Class-Based Approach (For Reusable, Complex Pipelines)

For more complex or reusable pipeline structures, you can define your pipeline by implementing the `IPipelineDefinition` interface. This allows you to encapsulate the pipeline's structure within a dedicated class, making it easier to manage, test, and integrate with dependency injection frameworks.

### Interface Definition

```csharp
public interface IPipelineDefinition
{
    void Define(PipelineBuilder builder, PipelineContext context);
}
```

* **`Define`**: This method is where you add your sources, transforms, and sinks to the provided `builder`. The `context` parameter allows for dynamic pipeline construction based on runtime parameters or injected dependencies.

### Advantages of Class-Based Definitions

1. **Separation of Concerns**: Keeps pipeline logic separate from execution code
2. **Reusability**: Define once, execute multiple times with different configurations
3. **Testability**: Easier to unit test pipeline structure independently
4. **Dependency Injection**: Seamlessly integrates with DI containers for injecting node dependencies
5. **Complex Pipelines**: Better organization for pipelines with many branches, joins, or conditional logic

### Example with Dependency Injection

Here's how you can use `IPipelineDefinition` with dependency injection:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline;
using NPipeline.Pipeline;

public sealed class MyPipelineDefinition : IPipelineDefinition
{
    private readonly ILogger<MyPipelineDefinition> _logger;
    private readonly IDataService _dataService;

    // Inject dependencies
    public MyPipelineDefinition(ILogger<MyPipelineDefinition> logger, IDataService dataService)
    {
        _logger = logger;
        _dataService = dataService;
    }

    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        _logger.LogInformation("Defining pipeline with injected dependencies");

        // Use injected services to configure nodes
        var sourceHandle = builder.AddSource<ConfiguredSource, Data>();
        var transformHandle = builder.AddTransform<DependentTransform, Data, ProcessedData>();
        var sinkHandle = builder.AddSink<DatabaseSink, ProcessedData>();

        builder.Connect(sourceHandle, transformHandle);
        builder.Connect(transformHandle, sinkHandle);
    }
}

public static class Program
{
    public static async Task Main(string[] args)
    {
        var services = new ServiceCollection();
        
        // Register pipeline and its dependencies
        services.AddLogging();
        services.AddScoped<IDataService, DataService>();
        services.AddScoped<IPipelineDefinition, MyPipelineDefinition>();

        var provider = services.BuildServiceProvider();
        
        // Create runner and execute
        var runner = PipelineRunner.Create();
        await runner.RunAsync<MyPipelineDefinition>();
    }
}
```

---

## Choosing Your Approach

| Aspect | PipelineBuilder | IPipelineDefinition |
|--------|-----------------|-------------------|
| **Use When** | Simple pipelines, quick prototyping | Complex pipelines, reusable definitions, Dependency Injection (DI) needed |
| **Code Style** | Fluent, expressive | Class-based, organized |
| **Testability** | Moderate | Excellent (isolated test fixtures) |
| **Dependency Injection** | Possible but awkward | Natural, seamless integration |
| **Reusability** | Limited | Excellent |
| **Learning Curve** | Gentle | Moderate |

### Decision Guide

**Use `PipelineBuilder` directly if:**
- Your pipeline is simple or moderate in complexity
- You're building a one-off data processing task
- You prefer fluent, method-chaining style code
- You want to get started quickly

**Use `IPipelineDefinition` if:**
- Your pipeline will be used in multiple places
- You have complex branching, joining, or conditional logic
- You want to inject dependencies (loggers, services, configuration)
- You plan to test the pipeline structure independently
- You want better separation of concerns in your codebase

---

## Executing Your Pipelines

Regardless of which approach you use, pipelines are executed using the `PipelineRunner`:

```csharp
var runner = PipelineRunner.Create();
await runner.RunAsync<MyPipelineDefinition>();
```

The `PipelineRunner` handles:
- Instantiating your `IPipelineDefinition`
- Calling the `Define` method with a builder and context
- Building the pipeline graph
- Validating the configuration
- Executing the pipeline

---

## Related Topics

* **[Pipeline Execution](pipeline-execution/index.md)** - Learn how to run your defined pipelines
* **[Pipeline Context](pipeline-context.md)** - Understand how to pass state and configuration to your pipeline nodes
* **[Nodes Overview](nodes/index.md)** - Learn about source, transform, and sink nodes
* **[Execution Strategies](pipeline-execution/execution-strategies.md)** - Control how nodes process data
* **[Error Handling Guide](resilience/error-handling.md)** - Add resilience to your pipelines
* **[Dependency Injection](../extensions/dependency-injection.md)** - Using DI with pipelines

## Next Steps

- **[Pipeline Context](pipeline-context.md)** - Learn about the final component that carries state across your pipeline
- **[Execution Strategies](pipeline-execution/execution-strategies.md)** - Control how nodes process data
- **[Error Handling Guide](resilience/error-handling.md)** - Add resilience to your pipelines

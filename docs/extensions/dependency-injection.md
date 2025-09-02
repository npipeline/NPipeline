---
title: Dependency Injection
description: Integrate NPipeline with .NET's dependency injection framework to manage dependencies in your nodes and pipelines.
sidebar_position: 1
---

# Dependency Injection

Managing dependencies in complex data pipelines can be challenging. The `NPipeline.Extensions.DependencyInjection` package provides seamless integration with the standard `Microsoft.Extensions.DependencyInjection` framework, allowing you to register your pipelines and nodes with a service container and have their dependencies automatically resolved.

## Installation

First, add the NuGet package to your project:

```bash
dotnet add package NPipeline.Extensions.DependencyInjection
```

## Registering NPipeline Services

The `AddNPipeline` extension method on `IServiceCollection` registers all necessary NPipeline services, including `IPipelineRunner`, node factories, and optionally any `IPipelineDefinition` or `INode` implementations found in specified assemblies.

NPipeline supports two registration approaches:

### Assembly Scanning (Automatic Discovery)

Use this approach when you want automatic discovery of pipeline components in your assemblies. This leverages reflection to find and register all implementations.

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Observability.Tracing;

public class Program
{
    public static async Task Main(string[] args)
    {
        var services = new ServiceCollection();

        // Register NPipeline services from the current assembly
        // This will discover and register:
        // - IPipelineRunner
        // - Any IPipelineDefinition implementations
        // - Any INode implementations
        // - Any INodeErrorHandler implementations
        // - Any IPipelineErrorHandler implementations
        // - Any IDeadLetterSink implementations
        // - Any ILineageSink implementations
        // - Any IPipelineLineageSink implementations
        // - Any IPipelineLineageSinkProvider implementations
        services.AddNPipeline(Assembly.GetExecutingAssembly());

        // You can also add other services as needed
        // services.AddSingleton<IMyCustomService, MyCustomService>();

        var serviceProvider = services.BuildServiceProvider();

        // Resolve the pipeline runner
        var runner = serviceProvider.GetRequiredService<IPipelineRunner>();

        // Now you can run your pipeline
        await runner.RunAsync<MyPipelineDefinition>();
    }
}
```

**Benefits of Assembly Scanning:**

- Automatic discovery - no need to manually register each component
- Convenient for larger projects with many components
- Works with components in multiple assemblies


### Fluent Configuration (Manual Registration)

Use this approach for explicit, fine-grained control over service registration. This is ideal when:

- You want to avoid reflection overhead
- You have a specific set of components to register
- You need custom `ServiceLifetime` control for certain components


```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.DataFlow;
using NPipeline.Nodes;

public class Program
{
    public static async Task Main(string[] args)
    {
        var services = new ServiceCollection();

        // Register NPipeline services using fluent configuration
        services.AddNPipeline(builder => builder
            // Register nodes with default transient lifetime
            .AddNode<MySourceNode>()
            .AddNode<MyTransformNode>()
            .AddNode<MySinkNode>()
            
            // Register pipeline definition
            .AddPipeline<MyPipelineDefinition>()
            
            // Register error handlers
            .AddErrorHandler<MyErrorHandler>()
            
            // Register lineage sinks
            .AddLineageSink<MyLineageSink>()
        );

        // You can also add other services as needed
        services.AddSingleton<IMyCustomService, MyCustomService>();

        var serviceProvider = services.BuildServiceProvider();

        var runner = serviceProvider.GetRequiredService<IPipelineRunner>();
        await runner.RunAsync<MyPipelineDefinition>();
    }
}
```

**Benefits of Fluent Configuration:**

- No reflection overhead - explicit component registration
- Clear, discoverable API via IntelliSense
- Type-safe registration with compile-time verification
- Flexible `ServiceLifetime` control per component
- Better for small/medium projects or performance-critical scenarios
- Can mix manual registration with selective assembly scanning


### Mixed Approach (Manual + Assembly Scanning)

You can combine both approaches by registering some components manually and scanning for others:

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
```

### NPipelineServiceBuilder API

The fluent builder provides the following methods:

```csharp
// Register nodes (default: Transient)
AddNode<TNode>()
AddNode<TNode>(ServiceLifetime lifetime)

// Register pipelines (default: Transient)
AddPipeline<TPipeline>()
AddPipeline<TPipeline>(ServiceLifetime lifetime)

// Register error handlers (default: Transient)
AddErrorHandler<THandler>()
AddErrorHandler<THandler>(ServiceLifetime lifetime)

// Register pipeline error handlers (default: Transient)
AddPipelineErrorHandler<THandler>()
AddPipelineErrorHandler<THandler>(ServiceLifetime lifetime)

// Register sinks (default: Transient)
AddDeadLetterSink<TSink>()
AddDeadLetterSink<TSink>(ServiceLifetime lifetime)

AddLineageSink<TSink>()
AddLineageSink<TSink>(ServiceLifetime lifetime)

AddPipelineLineageSink<TSink>()
AddPipelineLineageSink<TSink>(ServiceLifetime lifetime)

// Register providers (default: Transient)
AddLineageSinkProvider<TProvider>()
AddLineageSinkProvider<TProvider>(ServiceLifetime lifetime)

// Scan assemblies for implementations
ScanAssemblies(params Assembly[] assemblies)
```

### Automatic Registration

`AddNPipeline` automatically scans the provided assemblies for:

* **`IPipelineDefinition`**: Your pipeline definitions are registered as transient services, meaning a new instance is created for each pipeline run.
* **`INode`**: Your custom node implementations (sources, transforms, sinks, join nodes, aggregation nodes) are registered as transient services by default. This ensures node isolation between pipeline runs.
* **`INodeErrorHandler` / `IPipelineErrorHandler`**: Custom error handlers are also registered.
* **`IDeadLetterSink`**: Dead letter sink implementations for handling failed items.
* **`ILineageSink` / `IPipelineLineageSink`**: Lineage tracking sink implementations.
* **`IPipelineLineageSinkProvider`**: Providers for creating lineage sinks dynamically.

### Core Services Registered

In addition to scanning for your implementations, `AddNPipeline` registers these core NPipeline services:

* **`IPipelineFactory`**: Factory for creating pipeline instances
* **`IPipelineRunner`**: Main pipeline execution service
* **`INodeFactory`**: DI-aware node factory (replaces default factory)
* **`IErrorHandlerFactory`**: Factory for creating error handlers and dead-letter sinks
* **`ILineageFactory`**: Factory for creating lineage sinks and resolving lineage collectors
* **`IObservabilityFactory`**: Factory for resolving observability collectors
* **Execution services**: CountingService, MergeStrategySelector, PipeMergeService, etc.
* **Observability services**: LineageService, BranchService, NodeExecutor, etc.
* **Error handling**: ErrorHandlingService
* **Persistence**: PersistenceService

### Overriding Default Registrations

You can override or customize default registrations if needed. For example, if you want a singleton instance of a particular node:

```csharp
services.AddNPipeline(Assembly.GetExecutingAssembly());
services.AddSingleton<MySingletonNode>(); // Overrides the default transient registration
```

## Running Pipelines with DI

The dependency injection extension provides convenient extension methods for running pipelines directly from the service provider.

### Simple Pipeline Execution

```csharp
// Run pipeline without parameters
await serviceProvider.RunPipelineAsync<MyPipelineDefinition>();
```

### Pipeline Execution with Parameters

```csharp
// Run pipeline with parameters
var parameters = new Dictionary<string, object>
{
    ["InputPath"] = "/data/input.csv",
    ["OutputPath"] = "/data/output.csv",
    ["BatchSize"] = 1000
};

await serviceProvider.RunPipelineAsync<MyPipelineDefinition>(parameters);
```

## Registering Pipelines and Nodes with Dependencies

The extension provides a set of `IServiceCollection` extension methods to register your pipeline components. The primary method is `AddNPipeline()`, which sets up the core services required to run pipelines.

You can then register your pipeline definitions and the nodes they depend on.

Let's consider a pipeline where a transform node relies on an external service, `IEmailService`.

**1. Define your services and nodes:**

```csharp
using NPipeline;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;

// A service your node depends on
public interface IEmailService
{
    Task SendEmailAsync(string to, string subject, string body);
}

public class EmailService : IEmailService
{
    public Task SendEmailAsync(string to, string subject, string body)
    {
        Console.WriteLine($"Sending email to {to}: {subject}");
        return Task.CompletedTask;
    }
}

// A transform that uses the service
public sealed class NotificationTransform : TransformNode<string, string>
{
    private readonly IEmailService _emailService;

    public NotificationTransform(IEmailService emailService)
    {
        _emailService = emailService;
    }

    public override Task<string> ExecuteAsync(
        string item,
        PipelineContext context,
        CancellationToken cancellationToken = default)
    {
        _emailService.SendEmailAsync(
            "admin@example.com",
            "Processing Item",
            $"Item '{item}' was processed.");
        return Task.FromResult($"Notified for {item}");
    }
}
```

**2. Define your pipeline:**

```csharp
using NPipeline;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Obersability.Tracing;
using NPipeline.Pipeline;

public sealed class TestStringSource : SourceNode<string>
{
    public IDataPipe<string> ExecuteAsync(
        PipelineContext context,
        CancellationToken cancellationToken = default)
    {
        static IAsyncEnumerable<string> Stream()
        {
            return Generate();

            async IAsyncEnumerable<string> Generate()
            {
                var data = new[] { "A", "B", "C" };
                foreach (var item in data)
                {
                    yield return item;
                }
            }
        }

        return new StreamingDataPipe<string>(Stream());
    }
}

public sealed class TestStringSink : SinkNode<string>
{
    public async Task ExecuteAsync(
        IDataPipe<string> input,
        PipelineContext context,
        IPipelineActivity parentActivity,
        CancellationToken cancellationToken = default)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            Console.WriteLine($"Result: {item}");
        }
    }
}

public class MyPipelineDefinition : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var sourceHandle = builder.AddSource<TestStringSource, string>();
        var transformHandle = builder.AddTransform<NotificationTransform, string, string>();
        var sinkHandle = builder.AddSink<TestStringSink, string>();

        builder.Connect(sourceHandle, transformHandle);
        builder.Connect(transformHandle, sinkHandle);
    }
}
```

**3. Register everything with the DI container:**

In your `Program.cs` or `Startup.cs`, use the `AddNPipeline` extension method.

```csharp
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

public class Program
{
    public static async Task Main(string[] args)
    {
        var services = new ServiceCollection();

        // 1. Add required NPipeline services, scanning this assembly for pipeline components
        services.AddNPipeline(Assembly.GetExecutingAssembly());

        // 2. Register your application's services
        services.AddSingleton<IEmailService, EmailService>();

        var serviceProvider = services.BuildServiceProvider();

        // 3. Run the pipeline using the service provider
        await serviceProvider.RunPipelineAsync<MyPipelineDefinition>();
    }
}
```

### How It Works

* `AddNPipeline(Assembly.GetExecutingAssembly())`: Registers core NPipeline services and automatically scans the provided assembly for pipeline definitions and nodes.
* When nodes are instantiated (like `NotificationTransform`), the service provider automatically resolves their dependencies.
* This ensures that `NotificationTransform` receives its `IEmailService` dependency from the container.
* The `DiContainerNodeFactory` is used instead of the default `DefaultNodeFactory`, enabling constructor injection for nodes.

## Resolving Services within Nodes

Nodes can receive dependencies through constructor injection, just like any other service in your DI container.

```csharp
using NPipeline;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

public interface IDataService
{
    Task<string> FetchDataAsync(int id);
}

public class MyDataService : IDataService
{
    public Task<string> FetchDataAsync(int id) => Task.FromResult($"Data for {id}");
}

public class MyDependentTransform(IDataService dataService) : TransformNode<int, string>
{
    public override Task<string> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
    {
        var data = dataService.FetchDataAsync(item).Result;
        return Task.FromResult($"Transformed: {data}");
    }
}

// Register services and node
// services.AddSingleton<IDataService, MyDataService>();
// services.AddNPipeline(Assembly.GetExecutingAssembly());
```

## DI-Specific Node Factory Behavior

When using dependency injection, the `DiContainerNodeFactory` is registered instead of the default `DefaultNodeFactory`. This provides several benefits:

1. **Constructor Injection**: Nodes can receive dependencies through their constructors
2. **Service Lifetime Management**: Nodes respect the service lifetimes configured in the DI container
3. **Scoped Services**: Nodes can receive scoped services that are shared within a pipeline execution
4. **Automatic Disposal**: The DI container manages disposal of nodes and their dependencies

### Service Lifetimes in Pipelines

* **Transient**: New instance created for each node (default for nodes)
* **Singleton**: Single instance shared across all pipeline runs
* **Scoped**: Instance shared within a single pipeline execution

## Benefits of Using Dependency Injection

* **Decoupling**: Your nodes no longer need to create their own dependencies. They simply declare what they need in their constructors.
* **Lifecycle Management**: The DI container manages the lifetime of your services (singleton, scoped, transient).
* **Testability**: It becomes much easier to test your nodes by providing mock implementations of their dependencies.
* **Configuration**: You can easily inject configuration objects (`IOptions<T>`) into your nodes to change their behavior without modifying code.
* **Service Discovery**: The DI container automatically discovers and registers your pipeline components.

## Related Topics

* **[Parallelism](parallelism.md)**: Learn how to execute parts of your pipeline in parallel to improve performance.
* **[Testing Extensions](./testing/index.md)**: Discover utilities for testing your pipelines and nodes effectively.
* **[NPipeline Extensions Index](../index.md)**: Return to the extensions overview.

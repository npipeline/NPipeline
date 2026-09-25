---
title: "Dependency Injection"
description: "Register pipelines and nodes with Microsoft.Extensions.DependencyInjection."
order: 11
---

# Dependency Injection

> **Prerequisites:** [Defining Pipelines](defining-pipelines.md)

The `NPipeline.Extensions.DependencyInjection` package integrates NPipeline with `Microsoft.Extensions.DependencyInjection`. It handles [node](../reference/glossary.md#node) resolution, service lifetimes, and pipeline execution through the DI container.

## Installation

```bash
dotnet add package NPipeline.Extensions.DependencyInjection
```

## Quick Setup

The simplest approach scans assemblies for all NPipeline types:

```csharp
var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        services.AddNPipeline(Assembly.GetExecutingAssembly());
    })
    .Build();

await host.Services.RunPipelineAsync<OrderPipeline>();
```

`AddNPipeline(Assembly[])` auto-discovers and registers:

- All `INode` implementations
- All `IPipelineDefinition` implementations
- All `IResiliencePolicy` implementations
- All `IDeadLetterSink` implementations
- All lineage sink implementations

## Manual Registration

For explicit control, use the builder API:

```csharp
services.AddNPipeline(builder =>
{
    builder.AddNode<OrderSource>();
    builder.AddNode<ValidateOrder>();
    builder.AddNode<OrderSink>();
    builder.AddPipeline<OrderPipeline>();
    builder.AddResiliencePolicy<RetryOnTransientErrors>();
    builder.AddDeadLetterSink<FileDeadLetterSink>();
});
```

Mix both approaches - scan assemblies, then add individual registrations:

```csharp
services.AddNPipeline(builder =>
{
    builder.ScanAssemblies(Assembly.GetExecutingAssembly());
    builder.AddNode<ExternalNode>(ServiceLifetime.Singleton);
});
```

## Service Lifetimes

All types default to `Transient` registration. Override the lifetime per type:

```csharp
builder.AddNode<ExpensiveNode>(ServiceLifetime.Singleton);
builder.AddNode<CachedLookup>(ServiceLifetime.Scoped);
```

| Lifetime | When to Use |
|----------|-------------|
| Transient (default) | Stateless nodes, new instance per use |
| Scoped | Nodes that share state within a pipeline run |
| Singleton | Thread-safe nodes with expensive initialization |

> ⚠️ **Warning:** Singleton nodes must be thread-safe. If your node holds mutable state, use `Transient` or `Scoped`.

## Running Pipelines

### From IServiceProvider

```csharp
await host.Services.RunPipelineAsync<MyPipeline>();

// With parameters
await host.Services.RunPipelineAsync<MyPipeline>(
    new Dictionary<string, object> { ["date"] = DateTime.Today });
```

`RunPipelineAsync` creates a DI scope, resolves the runner and all dependencies, and executes the pipeline. The pipeline determines ownership per instance: the container manages nodes it resolves, while an instance the run creates or the builder hands over (for example through `AddTap`, `AddBatcher` or `AddSink(instance)`) is disposed at the end of the run that owns it.

### From an Injected Runner

Inject `IPipelineRunner` into your own services:

```csharp
public class OrderService(IPipelineRunner runner)
{
    public async Task ProcessOrdersAsync(CancellationToken ct)
    {
        var context = PipelineContext.CreateDefault();
        await runner.RunAsync<OrderPipeline>(context, ct);
    }
}
```

## Constructor Injection in Nodes

When nodes are resolved through DI, they can take constructor dependencies:

```csharp
public class EnrichOrder : TransformNode<Order, EnrichedOrder>
{
    private readonly IHttpClientFactory _httpFactory;
    private readonly ILogger<EnrichOrder> _logger;

    public EnrichOrder(IHttpClientFactory httpFactory, ILogger<EnrichOrder> logger)
    {
        _httpFactory = httpFactory;
        _logger = logger;
    }

    public override async ValueTask<EnrichedOrder> TransformAsync(
        Order item, PipelineContext context, CancellationToken ct)
    {
        var client = _httpFactory.CreateClient();
        _logger.LogInformation("Enriching order {OrderId}", item.Id);
        var details = await client.GetFromJsonAsync<Details>($"/orders/{item.Id}", ct);
        return new EnrichedOrder(item, details!);
    }
}
```

> 📝 **Note:** Without DI, nodes must have a parameterless constructor. The `NodeParameterlessConstructorAnalyzer` (NP9403) warns about this at build time.

## Custom Factories and Ownership

The pipeline disposes what it creates, and leaves alone what a container owns. A run resolves nodes, dead-letter sinks, and lineage sinks through `INodeFactory`, `IErrorHandlerFactory`, and `ILineageFactory`, then asks each factory whether the instance it handed over belongs to the run:

```csharp
public bool IsOwnedByRun(NodeDefinition nodeDefinition, INode instance) => true;
public bool CallerOwnsCreatedInstance(object instance) => true;
```

The default answer is `true`, which suits a factory that always builds fresh instances. A factory backed by a container overrides the method and reports `false` for the instances it resolved:

```csharp
public sealed class MyNodeFactory(IServiceProvider serviceProvider) : INodeFactory
{
    private readonly HashSet<INode> _containerOwned = new(ReferenceEqualityComparer.Instance);

    public INode Create(NodeDefinition nodeDefinition, PipelineGraph graph) { /* ... */ }

    public bool IsOwnedByRun(NodeDefinition nodeDefinition, INode instance) => !_containerOwned.Contains(instance);
}
```

> [!WARNING]
> Returning `true` for a container-owned instance disposes it twice: once by the run, once when the scope ends. Because a run instantiates nodes concurrently, back the collection with a thread-safe type or guard it with a lock.

A single factory can mix both. Track only the instances the container resolves and let everything the factory constructs itself be disposed by the run.

## What Gets Registered

`AddNPipeline` registers these core services automatically:

| Service | Lifetime |
|---------|----------|
| `IPipelineRunner` | Scoped |
| `PipelineBuilder` | Transient |
| `IPipelineFactory` | Singleton |
| `INodeFactory` | Scoped |
| `INodeExecutor` | Scoped |
| `ITopologyService` | Scoped |
| `IErrorHandlingService` | Transient |

## Next Steps

- [DI Extension Reference](../extensions/dependency-injection.md) - registered services, node factory, and registry details
- [Pipeline Context](pipeline-context.md) - pass runtime parameters and state
- [Defining Pipelines](defining-pipelines.md) - the builder API
- [Parallel Execution](parallel-execution.md) - thread safety with DI-resolved nodes

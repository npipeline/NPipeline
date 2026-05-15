---
title: "Dependency Injection"
description: "Microsoft.Extensions.DependencyInjection integration for automatic node resolution, assembly scanning, and pipeline lifecycle."
order: 3
---

# Dependency Injection

The `NPipeline.Extensions.DependencyInjection` package integrates NPipeline with `Microsoft.Extensions.DependencyInjection`. It provides assembly scanning, fluent registration, compiled expression-based node creation, and pipeline lifecycle management.

## Installation

```bash
dotnet add package NPipeline.Extensions.DependencyInjection
```

## Quick Start

```csharp
var services = new ServiceCollection();

services.AddNPipeline(builder =>
{
    builder.AddPipeline<OrderPipeline>();
    builder.AddNode<OrderTransformNode>();
});

var provider = services.BuildServiceProvider();
await provider.RunPipelineAsync<OrderPipeline>();
```

## NPipelineServiceBuilder API

The builder passed to `AddNPipeline` exposes the following registration methods:

| Method | Default Lifetime | Description |
|--------|-----------------|-------------|
| `AddNode<T>()` | Transient | Register a node type |
| `AddNode<T>(ServiceLifetime)` | Custom | Register with explicit lifetime |
| `AddPipeline<T>()` | Transient | Register a pipeline definition |
| `AddPipeline<T>(ServiceLifetime)` | Custom | Register with explicit lifetime |
| `AddErrorHandler<T>()` | Scoped | Register an error handler |
| `AddResiliencePolicy<T>()` | Scoped | Register a resilience policy |
| `AddDeadLetterSink<T>()` | Scoped | Register a dead letter sink |
| `AddLineageSink<T>()` | Scoped | Register a lineage sink |
| `AddPipelineLineageSink<T>()` | Scoped | Register a pipeline lineage sink |
| `AddLineageSinkProvider<T>()` | Scoped | Register a lineage sink provider |
| `ScanAssemblies(params Assembly[])` | — | Discover and register all implementations |

### Fluent Registration

```csharp
services.AddNPipeline(builder =>
{
    // Nodes
    builder.AddNode<OrderTransformNode>();
    builder.AddNode<OrderTransformNode>(ServiceLifetime.Scoped);

    // Pipeline definitions
    builder.AddPipeline<OrderPipeline>();
    builder.AddPipeline<OrderPipeline>(ServiceLifetime.Transient);

    // Resilience policies (resolved via DI)
    builder.AddResiliencePolicy<MyRetryPolicy>();

    // Dead letter sinks
    builder.AddDeadLetterSink<FileDeadLetterSink>();

    // Lineage sinks
    builder.AddLineageSink<DatabaseLineageSink>();
    builder.AddPipelineLineageSink<LoggingPipelineLineageSink>();
    builder.AddLineageSinkProvider<DefaultPipelineLineageSinkProvider>();
});
```

### Assembly Scanning

Automatically discovers and registers implementations of:

- `IPipelineDefinition` — pipeline definitions
- `INode` derivatives — source, transform, sink, and custom nodes
- `IResiliencePolicy` — resilience policies
- `IErrorHandler` — error handlers
- `IDeadLetterSink` — dead letter sinks
- `ILineageSink` / `IPipelineLineageSink` — lineage sinks

```csharp
services.AddNPipeline(typeof(Program).Assembly);

// Multiple assemblies
services.AddNPipeline(
    typeof(Program).Assembly,
    typeof(SharedNodes).Assembly);
```

Assembly scanning handles `ReflectionTypeLoadException` gracefully — types that fail to load are skipped.

### Mixed Registration

Combine explicit registration with assembly scanning:

```csharp
services.AddNPipeline(builder =>
{
    // Explicit — use specific lifetime or override defaults
    builder.AddNode<CustomTransformNode>(ServiceLifetime.Singleton);

    // Scan — discover everything else
    builder.ScanAssemblies(typeof(Program).Assembly);
});
```

Explicit registrations take precedence over scanned registrations for the same type.

## Running Pipelines

```csharp
// Run a specific pipeline
await provider.RunPipelineAsync<OrderPipeline>();

// Run with parameters
await provider.RunPipelineAsync<OrderPipeline>(new Dictionary<string, object>
{
    ["date"] = DateTime.Today,
    ["batchSize"] = 1000
});
```

## Resolving Services in Nodes

Nodes participate in DI — inject dependencies via constructor:

```csharp
public class EmailNotificationNode : TransformNode<Order, Order>
{
    private readonly IEmailService _emailService;
    private readonly ILogger<EmailNotificationNode> _logger;

    public EmailNotificationNode(IEmailService emailService, ILogger<EmailNotificationNode> logger)
    {
        _emailService = emailService;
        _logger = logger;
    }

    protected override async Task<Order> TransformAsync(Order input, PipelineContext ctx, CancellationToken ct)
    {
        await _emailService.SendAsync(input.CustomerEmail, $"Order {input.Id} received", ct);
        _logger.LogInformation("Sent notification for order {OrderId}", input.Id);
        return input;
    }
}
```

## Service Lifetimes

| Lifetime | Behavior in Pipelines | Use For |
|----------|----------------------|---------|
| **Transient** | New instance per resolution (per node creation) | Stateless nodes, lightweight services |
| **Scoped** | One instance per pipeline run (via `IServiceScope`) | Per-run state, DB connections, unit-of-work |
| **Singleton** | One instance for the application | Thread-safe caches, configuration, factories |

**Recommendation**: Use **Transient** for nodes (default) and **Scoped** for services that hold per-run state. Avoid injecting Scoped services into Singleton nodes.

## Node Factory

`DiContainerNodeFactory` creates node instances using **compiled expression trees** for constructor invocation — no runtime reflection in the hot path. This provides near-native constructor performance.

Falls back to `ActivatorUtilities.CreateInstance` when:

- The constructor has complex parameter patterns
- Expression compilation fails at startup

The fallback is transparent and functionally identical — only performance differs slightly.

## Registered Services

`AddNPipeline` registers these core services automatically:

| Service | Implementation | Lifetime |
|---------|---------------|----------|
| `IPipelineFactory` | `PipelineFactory` | Singleton |
| `PipelineBuilder` | — | Transient |
| `INodeFactory` | `DiContainerNodeFactory` | Scoped |
| `IPipelineRunner` | `PipelineRunner` | Scoped |
| `IErrorHandlerFactory` | `DiHandlerFactory` | Scoped |
| `ILineageFactory` | `DiHandlerFactory` | Scoped |
| `IObservabilityFactory` | `DiHandlerFactory` | Scoped |
| `ILineage` | `NullLineage` | Scoped |
| `PipelineDefinitionRegistry` | — | Singleton |

## Pipeline Definition Registry

`PipelineDefinitionRegistry` tracks all discovered `IPipelineDefinition` types. It is:

- Populated during `AddNPipeline` / `ScanAssemblies` calls
- Thread-safe (backed by `ConcurrentDictionary`)
- Used by tooling (e.g., NPipeline.Studio) for pipeline discovery
- Queryable at runtime to list available pipelines

## Overriding Default Registrations

Replace built-in services by registering after `AddNPipeline`:

```csharp
services.AddNPipeline(builder => { ... });

// Override the node factory
services.AddScoped<INodeFactory, CustomNodeFactory>();

// Override the pipeline runner
services.AddScoped<IPipelineRunner, InstrumentedPipelineRunner>();
```

Later registrations replace earlier ones for the same service type.

## See Also

- [Dependency Injection Guide](../guides/dependency-injection.md) — step-by-step walkthrough with examples
- [Extensions Overview](index.md)

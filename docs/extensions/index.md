---
title: NPipeline Extensions
description: Discover how NPipeline extensions enhance functionality, providing specialized nodes, integrations, and advanced features.
sidebar_position: 1
slug: /extensions
---

# NPipeline Extensions

NPipeline's modular design allows its core functionality to be extended through separate NuGet packages. These extensions provide specialized nodes, integration points, and advanced features that cater to specific use cases without bloating the core library.

This section details the officially supported extensions and how to leverage them in your pipelines.

## Available Extensions

* **[Nodes](nodes/index.md)**: Pre-built, production-ready nodes for common data processing operations. Includes cleansing, validation, filtering, and type conversion nodes for string, numeric, datetime, and collection data.

* **[Dependency Injection](dependency-injection.md)**: Seamlessly integrate NPipeline with your favorite Dependency Injection container. Learn how to use constructor injection in your nodes and run pipelines with the `RunPipelineAsync<TDefinition>()` extension method.

* **[Parallelism](parallelism.md)**: Execute pipeline nodes in parallel for improved performance. Discover how to use `ParallelExecutionStrategy` with `WithParallelOptions()` to configure parallel processing, queue policies, and ordering behavior.

* **[Testing](testing/index.md)**: Utilities and helpers for writing comprehensive and efficient tests for your pipelines. Includes in-memory source/sink nodes, pipeline builder extensions, and assertion libraries.

* **[Connectors](../connectors/index.md)**: Pre-built source and sink nodes for common data sources and destinations (e.g., CSV files).

## Extension Packages

### Core Extensions

| Package | Description | Key Features |
|----------|-------------|---------------|
| [`NPipeline.Extensions.Nodes`](../../src/NPipeline.Extensions.Nodes/NPipeline.Extensions.Nodes.csproj) | Pre-built data processing nodes | String/numeric/datetime cleansing, validation, filtering, type conversion |
| [`NPipeline.Extensions.DependencyInjection`](../../../src/NPipeline.Extensions.DependencyInjection/NPipeline.Extensions.DependencyInjection.csproj) | DI container integration | Constructor injection, service lifetime management, `RunPipelineAsync()` extension |
| [`NPipeline.Extensions.Parallelism`](../../../src/NPipeline.Extensions.Parallelism/NPipeline.Extensions.Parallelism.csproj) | Parallel processing capabilities | `ParallelExecutionStrategy`, `WithParallelOptions()`, queue policies |
| [`NPipeline.Extensions.Testing`](../../../src/NPipeline.Extensions.Testing/NPipeline.Extensions.Testing.csproj) | Testing utilities | In-memory nodes, pipeline builder extensions, test context helpers |

### Assertion Libraries

| Package | Description | Integration |
|----------|-------------|--------------|
| [`NPipeline.Extensions.Testing.AwesomeAssertions`](../../../src/NPipeline.Extensions.Testing.AwesomeAssertions/NPipeline.Extensions.Testing.AwesomeAssertions.csproj) | AwesomeAssertions integration | `ShouldHaveReceived()`, `ShouldContain()`, `ShouldOnlyContain()` |
| [`NPipeline.Extensions.Testing.FluentAssertions`](../../../src/NPipeline.Extensions.Testing.FluentAssertions/NPipeline.Extensions.Testing.FluentAssertions.csproj) | FluentAssertions integration | `ShouldHaveReceived()`, `ShouldContain()`, `ShouldNotContain()` |

## Getting Started with Extensions

### Installation

Each extension is available as a separate NuGet package. Install only what you need:

```bash
# Nodes (data processing)
dotnet add package NPipeline.Extensions.Nodes

# Dependency Injection
dotnet add package NPipeline.Extensions.DependencyInjection

# Parallelism
dotnet add package NPipeline.Extensions.Parallelism

# Testing
dotnet add package NPipeline.Extensions.Testing

# Testing with AwesomeAssertions
dotnet add package NPipeline.Extensions.Testing.AwesomeAssertions
dotnet add package AwesomeAssertions

# Testing with FluentAssertions
dotnet add package NPipeline.Extensions.Testing.FluentAssertions
dotnet add package FluentAssertions
```

> **Complete Installation Guide**
>
> For the core NPipeline package and comprehensive installation instructions for all packages, see the [Installation Guide](../getting-started/installation.md).

### Basic Usage Pattern

Most extensions follow a consistent pattern:

1. **Install the NuGet package**
2. **Add using statements** for the extension namespace
3. **Use extension methods** on `PipelineBuilder`, `IServiceCollection`, or test classes
4. **Configure options** as needed for the extension

```csharp
using NPipeline;
using NPipeline.Extensions.Nodes;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Parallelism;

// Nodes setup
var builder = new PipelineBuilder();
builder.AddStringCleansing<User>(x => x.Email)
    .Trim()
    .ToLower();

builder.AddStringValidation<User>(x => x.Email)
    .IsEmail();

// DI setup
var services = new ServiceCollection();
services.AddNPipeline(Assembly.GetExecutingAssembly());

// Parallel pipeline definition
public class MyParallelPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddInMemorySource<int>();
        var transform = builder.AddTransform<MyTransform, int, int>();
        var sink = builder.AddInMemorySink<int>();

        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        // Configure parallel execution
        builder.WithParallelOptions(transform,
            new ParallelOptions { MaxDegreeOfParallelism = 4 });

        transform.ExecutionStrategy = new ParallelExecutionStrategy();
    }
}
```

## Best Practices

* **Install only what you need**: Each extension is a separate package to keep your dependencies minimal
* **Use the right tool for the job**: Leverage testing extensions for unit tests, parallelism for performance, DI for enterprise applications
* **Combine extensions**: Extensions are designed to work together seamlessly
* **Check compatibility**: Ensure extension versions are compatible with your NPipeline core version

## Next Steps

* **[Dependency Injection](dependency-injection.md)**: Learn about constructor injection and service lifetime management
* **[Parallelism](parallelism.md)**: Explore parallel processing capabilities and configuration options
* **[Testing](testing/index.md)**: Discover comprehensive testing utilities and assertion libraries
* **[Connectors](../connectors/index.md)**: Explore pre-built connectors for external systems

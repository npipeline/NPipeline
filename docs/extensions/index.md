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

* **[Composition](composition/index.md)**: Create hierarchical, modular pipelines by treating entire pipelines as reusable transform nodes. Enables breaking complex workflows into smaller, well-tested building blocks with full type safety and context control.

* **[Connectors](../connectors/index.md)**: Pre-built source and sink nodes for common data sources and destinations (e.g., CSV files).

* **[Dependency Injection](dependency-injection.md)**: Seamlessly integrate NPipeline with your favorite Dependency Injection container. Learn how to use constructor injection in your nodes and run pipelines with the `RunPipelineAsync<TDefinition>()` extension method.

* **[Lineage](lineage/index.md)**: Comprehensive data lineage tracking and provenance capabilities. Track the complete journey of each data item from source to destination, enabling data governance, debugging, audit trails, and data discovery.

* **[Nodes](nodes/index.md)**: Pre-built, production-ready nodes for common data processing operations. Includes cleansing, validation, filtering, and type conversion nodes for string, numeric, datetime, and collection data.

* **[Observability](observability/index.md)**: Comprehensive metrics collection and monitoring capabilities for NPipeline pipelines. Track node and pipeline performance, throughput, memory usage, retries, and errors with built-in logging sinks and custom metrics sink support.

* **[Parallelism](parallelism/index.md)**: Execute pipeline nodes in parallel for improved performance. Discover how to use `ParallelExecutionStrategy` with `WithParallelOptions()` to configure parallel processing, queue policies, and ordering behavior.

* **[Testing](testing/index.md)**: Utilities and helpers for writing comprehensive and efficient tests for your pipelines. Includes in-memory source/sink nodes, pipeline builder extensions, and assertion libraries.

## Extension Packages

### Core Extensions

| Package | Description | Key Features |
|----------|-------------|---------------|
| [`NPipeline.Extensions.Composition`](../../../src/NPipeline.Extensions.Composition/NPipeline.Extensions.Composition.csproj) | Hierarchical pipeline composition | Sub-pipelines as nodes, modular design, context control, unlimited nesting |
| [`NPipeline.Extensions.DependencyInjection`](../../../src/NPipeline.Extensions.DependencyInjection/NPipeline.Extensions.DependencyInjection.csproj) | DI container integration | Constructor injection, service lifetime management, `RunPipelineAsync()` extension |
| [`NPipeline.Extensions.Lineage`](../../../src/NPipeline.Extensions.Lineage/NPipeline.Extensions.Lineage.csproj) | Data lineage tracking | Item-level lineage, pipeline reports, configurable sampling, custom sinks |
| [`NPipeline.Extensions.Nodes`](../../src/NPipeline.Extensions.Nodes/NPipeline.Extensions.Nodes.csproj) | Pre-built data processing nodes | String/numeric/datetime cleansing, validation, filtering, type conversion |
| [`NPipeline.Extensions.Observability`](../../../src/NPipeline.Extensions.Observability/NPipeline.Extensions.Observability.csproj) | Metrics collection and monitoring | Node/pipeline metrics, throughput tracking, memory/CPU monitoring, custom sinks |
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
# Composition
dotnet add package NPipeline.Extensions.Composition

# Dependency Injection
dotnet add package NPipeline.Extensions.DependencyInjection

# Lineage
dotnet add package NPipeline.Extensions.Lineage

# Nodes
dotnet add package NPipeline.Extensions.Nodes

# Observability
dotnet add package NPipeline.Extensions.Observability

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

## Best Practices

* **Install only what you need**: Each extension is a separate package to keep your dependencies minimal
* **Use the right tool for the job**: Leverage testing extensions for unit tests, parallelism for performance, DI for enterprise applications, observability for production monitoring, lineage for data governance
* **Combine extensions**: Extensions are designed to work together seamlessly
* **Check compatibility**: Ensure extension versions are compatible with your NPipeline core version

## Next Steps

* **[Dependency Injection](dependency-injection.md)**: Learn about constructor injection and service lifetime management
* **[Lineage](lineage/index.md)**: Explore data lineage tracking for governance and debugging
* **[Parallelism](parallelism/index.md)**: Explore parallel processing capabilities and configuration options
* **[Testing](testing/index.md)**: Discover comprehensive testing utilities and assertion libraries
* **[Observability](observability/index.md)**: Learn about metrics collection and monitoring for production pipelines
* **[Connectors](../connectors/index.md)**: Explore pre-built connectors for external systems

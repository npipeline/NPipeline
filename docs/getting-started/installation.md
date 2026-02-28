---
title: Installation
description: A quick guide to installing NPipeline and its essential extensions via NuGet.
sidebar_position: 1
---

# Installation

Getting started with NPipeline is straightforward. This guide will walk you through adding the necessary NuGet packages to your .NET project and setting up dependency injection.

## Version Compatibility

All NPipeline packages target .NET 8.0, 9.0, and 10.0, ensuring compatibility with the latest .NET releases.

## Installing NuGet Packages

You can install NPipeline and its extensions using the NuGet Package Manager in Visual Studio, the .NET CLI, or by directly editing your project file.

### Using the .NET CLI

Open your terminal or command prompt, navigate to your project directory, and run the following commands:

**1. NPipeline (Core Library)**
The core library provides the fundamental building blocks for creating pipelines.

```bash
dotnet add package NPipeline
```

**2. NPipeline.Extensions.DependencyInjection (Optional, but Recommended)**
For easier integration with .NET's dependency injection container.

```bash
dotnet add package NPipeline.Extensions.DependencyInjection
```

**3. NPipeline.Extensions.Parallelism (Optional)**
For advanced control over parallel execution within your pipelines.

```bash
dotnet add package NPipeline.Extensions.Parallelism
```

**4. NPipeline.Extensions.Testing (Optional)**
Provides utilities for testing your pipelines.

```bash
dotnet add package NPipeline.Extensions.Testing
```

**5. NPipeline.Extensions.Testing.AwesomeAssertions (Optional)**
Integrates NPipeline testing with AwesomeAssertions.

```bash
dotnet add package NPipeline.Extensions.Testing.AwesomeAssertions
```

**6. NPipeline.Extensions.Testing.FluentAssertions (Optional)**
Integrates NPipeline testing with FluentAssertions.

```bash
dotnet add package NPipeline.Extensions.Testing.FluentAssertions
```

**7. NPipeline.Connectors (Optional)**
Base package for various data connectors.

```bash
dotnet add package NPipeline.Connectors
```

**8. NPipeline.Connectors.Csv (Optional)**
Provides CSV specific connectors.

```bash
dotnet add package NPipeline.Connectors.Csv
```

**9. NPipeline.Connectors.Excel (Optional)**
Provides Excel specific connectors for reading and writing XLS and XLSX files.

```bash
dotnet add package NPipeline.Connectors.Excel
```

**10. NPipeline.Connectors.Json (Optional)**
Provides JSON specific connectors for reading and writing JSON array and NDJSON files.

```bash
dotnet add package NPipeline.Connectors.Json
```

**11. NPipeline.Connectors.DuckDB (Optional)**
Provides an embedded analytical database connector for SQL-over-files (Parquet/CSV/JSON) and local zero-setup pipelines.

```bash
dotnet add package NPipeline.Connectors.DuckDB
```

### Using Visual Studio NuGet Package Manager

1. Right-click on your project in the Solution Explorer and select "Manage NuGet Packages...".
2. Go to the "Browse" tab.
3. Search for each package name (e.g., `NPipeline`, `NPipeline.Extensions.DependencyInjection`).
4. Select the desired package and click "Install".

## Package Dependencies

The following table shows the dependencies for each NPipeline package:

| Package | Dependencies |
| --- | --- |
| NPipeline.Extensions.DependencyInjection | Microsoft.Extensions.DependencyInjection.Abstractions |
| NPipeline.Connectors | Microsoft.Extensions.DependencyInjection.Abstractions |
| NPipeline.Connectors.Csv | CsvHelper |
| NPipeline.Connectors.Excel | ExcelDataReader, DocumentFormat.OpenXml |
| NPipeline.Connectors.Json | System.Text.Json |
| NPipeline.Connectors.DuckDB | DuckDB.NET.Data.Full |
| NPipeline.Extensions.Parallelism | System.Threading.Tasks.DataFlow |

## Setting Up Dependency Injection

After installing the necessary packages, you need to configure NPipeline in your dependency injection container. NPipeline supports Microsoft's IServiceContainer (the standard .NET dependency injection container).

### Microsoft.Extensions.DependencyInjection

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;

// In your application startup code (e.g., Program.cs)
var services = new ServiceCollection();

// Add NPipeline services and scan assemblies for pipeline components
// This registers all nodes, pipeline definitions, error handlers, and other components
services.AddNPipeline(Assembly.GetExecutingAssembly());

// You can also specify multiple assemblies to scan
services.AddNPipeline(
    Assembly.GetExecutingAssembly(),
    Assembly.LoadFrom("MyPipelineComponents.dll")
);

// Build the service provider
var serviceProvider = services.BuildServiceProvider();

// Now you can run pipelines
await serviceProvider.RunPipelineAsync<MyPipelineDefinition>(cancellationToken);
```

### In ASP.NET Core Applications

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;

var builder = WebApplication.CreateBuilder(args);

// Add NPipeline services to the ASP.NET Core DI container
builder.Services.AddNPipeline(Assembly.GetExecutingAssembly());

var app = builder.Build();

// NPipeline services are now available through dependency injection
// You can inject IPipelineRunner, INodeFactory, etc. into your services
```

### Running Pipelines with Dependency Injection

Once configured, you can run pipelines in several ways:

```csharp
// Method 1: Using the extension method on IServiceProvider
await serviceProvider.RunPipelineAsync<MyPipelineDefinition>(cancellationToken);

// Method 2: With parameters
var parameters = new Dictionary<string, object>
{
    ["InputPath"] = "data/input.csv",
    ["OutputPath"] = "data/output.json"
};
await serviceProvider.RunPipelineAsync<MyPipelineDefinition>(parameters, cancellationToken);

// Method 3: Manually resolving services
using var scope = serviceProvider.CreateScope();
var runner = scope.ServiceProvider.GetRequiredService<IPipelineRunner>();

var context = new PipelineContext(PipelineContextConfiguration.WithCancellation(cancellationToken));
await runner.RunAsync<MyPipelineDefinition>(context);
```

## Next Steps

* **[Quick Start](quick-start.md)**: Build your first NPipeline with a simple "Hello World" example
* **[Core Concepts](../core-concepts/index.md)**: Understand the fundamental building blocks of NPipeline
* **[Dependency Injection](../extensions/dependency-injection.md)**: Learn how to integrate NPipeline with .NET's dependency injection

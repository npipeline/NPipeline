---
title: "Installation"
description: "Install NPipeline and verify your setup in under 2 minutes."
order: 2
---

# Installation

This page gets NPipeline into your .NET project. You need one package to start; everything else is optional.

## Requirements

- .NET 8.0, 9.0, or 10.0
- A C# project (console app, ASP.NET Core, worker service, etc.)

## Install the Core Package

```bash
dotnet add package NPipeline
```

This gives you the pipeline builder, all base node types, data streams, and the pipeline runner. It's everything you need to define and execute pipelines.

## Add Dependency Injection (Recommended)

Most projects benefit from automatic node discovery and DI integration:

```bash
dotnet add package NPipeline.Extensions.DependencyInjection
```

With this package, NPipeline scans your assemblies for nodes and pipeline definitions, then registers them in your DI container automatically.

## Verify Your Setup

Create a new console project and confirm everything works:

```bash
dotnet new console -n MyPipeline
cd MyPipeline
dotnet add package NPipeline
dotnet add package NPipeline.Extensions.DependencyInjection
dotnet build
```

If the build succeeds, you're ready to write your first pipeline.

## Optional Packages

Install these as you need them. You don't need any of these to get started.

| Package | Purpose |
|---------|---------|
| `NPipeline.Extensions.Parallelism` | Run nodes in parallel with thread-safety controls |
| `NPipeline.Extensions.Testing` | In-memory sources/sinks and test harness utilities |
| `NPipeline.Extensions.Testing.AwesomeAssertions` | AwesomeAssertions integration for pipeline tests |
| `NPipeline.Extensions.Testing.FluentAssertions` | FluentAssertions integration for pipeline tests |
| `NPipeline.Extensions.Lineage` | Data lineage tracking and provenance |
| `NPipeline.Extensions.Observability` | Metrics collection and monitoring |
| `NPipeline.Extensions.Composition` | Nested sub-pipelines as reusable nodes |
| `NPipeline.Connectors.Csv` | Read/write CSV files |
| `NPipeline.Connectors.Json` | Read/write JSON and NDJSON files |
| `NPipeline.Connectors.Excel` | Read/write XLS and XLSX files |
| `NPipeline.Connectors.DuckDB` | Embedded analytical SQL over Parquet/CSV/JSON |
| `NPipeline.Connectors.Parquet` | Read/write Apache Parquet files |

> 🔗 **See also:** [Extensions](../extensions/index.md) for the complete list of all available packages.

## Next Steps

- [Your First Pipeline](your-first-pipeline.md) - build and run a working pipeline
- [Key Concepts](key-concepts.md) - understand the mental model behind NPipeline

---
title: "Installation"
description: "Install NPipeline and verify your setup."
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

This gives you the pipeline builder, all base node types, data streams, and the pipeline runner.

## Add Dependency Injection (Recommended)

Most projects benefit from automatic node discovery and DI integration:

```bash
dotnet add package NPipeline.Extensions.DependencyInjection
```

Use this package to discover nodes and pipeline definitions in your assemblies and register them in your DI container.

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
| `NPipeline.Connectors.Excel` | Read XLS and XLSX files; write XLSX files |
| `NPipeline.Connectors.DuckDB` | Embedded analytical SQL over Parquet/CSV/JSON |
| `NPipeline.Connectors.Parquet` | Read/write Apache Parquet files |

> [!NOTE]
> For the complete package list, see [Extensions](../extensions/index.md).

## Next Steps

- [Your First Pipeline](your-first-pipeline.md) - build and run a working pipeline
- [Key Concepts](key-concepts.md) - understand the mental model behind NPipeline

---
title: "Extensions"
description: "All NPipeline packages at a glance — extensions, connectors, storage providers, and tools."
order: 6
---

# Extensions

NPipeline is distributed as a set of NuGet packages. The core `NPipeline` package provides the pipeline runtime, node abstractions, error handling, and configuration. Everything else is an optional extension you install as needed.

## Core Packages

| Package | Description |
|---------|-------------|
| `NPipeline` | Pipeline runtime, node base classes, resilience, error handling, configuration |
| `NPipeline.Analyzers` | Roslyn analyzers and code fixes for pipeline configuration issues |

## Extension Packages

These packages add capabilities to the core pipeline runtime.

| Package | Description | Docs |
|---------|-------------|------|
| `NPipeline.Extensions.DependencyInjection` | Microsoft.Extensions.DependencyInjection integration for automatic node resolution | [Reference](dependency-injection.md) · [Guide](../guides/dependency-injection.md) |
| `NPipeline.Extensions.Nodes` | Pre-built utility nodes: cleansing, validation, filtering, conversion, enrichment | [Reference](utility-nodes.md) |
| `NPipeline.Extensions.Composition` | Treat entire pipelines as nodes within larger pipelines | [Reference](composition.md) · [Guide](../guides/pipeline-composition.md) |
| `NPipeline.Extensions.Parallelism` | Parallel execution strategies with configurable backpressure and queue policies | [Reference](parallelism.md) · [Guide](../guides/parallel-execution.md) |
| `NPipeline.Extensions.Observability` | Observability infrastructure for metrics collection and monitoring | [Reference](observability.md) · [Guide](../observability/metrics-and-monitoring.md) |
| `NPipeline.Extensions.Observability.OpenTelemetry` | OpenTelemetry integration for distributed tracing | [Reference](opentelemetry.md) · [Guide](../observability/opentelemetry.md) |
| `NPipeline.Extensions.Lineage` | Data lineage tracking and provenance capabilities | [Reference](lineage.md) · [Guide](../observability/data-lineage.md) |
| `NPipeline.Extensions.Testing` | Testing utilities: in-memory nodes, test harness, assertions | [Reference](testing.md) · [Guide](../testing/testing-pipelines.md) |
| `NPipeline.Extensions.Testing.FluentAssertions` | FluentAssertions extensions for pipeline execution results | [Reference](testing.md#fluentassertions) · [Guide](../testing/test-utilities.md) |
| `NPipeline.Extensions.Testing.AwesomeAssertions` | AwesomeAssertions extensions for pipeline execution results | [Reference](testing.md#awesomeassertions) · [Guide](../testing/test-utilities.md) |

## Connector Packages

Connectors provide pre-built source and sink nodes for reading from and writing to external systems.

### File Formats

| Package | Description |
|---------|-------------|
| `NPipeline.Connectors.Csv` | CSV source/sink using CsvHelper |
| `NPipeline.Connectors.Json` | JSON source/sink using System.Text.Json |
| `NPipeline.Connectors.Parquet` | Parquet source/sink using Parquet.Net |
| `NPipeline.Connectors.Excel` | Excel source/sink (XLS, XLSX) |

### Databases

| Package | Description |
|---------|-------------|
| `NPipeline.Connectors.Postgres` | PostgreSQL source/sink using Npgsql |
| `NPipeline.Connectors.SqlServer` | SQL Server source/sink using Microsoft.Data.SqlClient |
| `NPipeline.Connectors.MySql` | MySQL/MariaDB source/sink with streaming and bulk-load writes |
| `NPipeline.Connectors.MongoDB` | MongoDB source/sink |
| `NPipeline.Connectors.CosmosDb` | Azure Cosmos DB source/sink with SQL queries and Change Feed |
| `NPipeline.Connectors.Snowflake` | Snowflake source/sink with streaming, batching, staged COPY loading |
| `NPipeline.Connectors.DuckDB` | DuckDB embedded analytics with Parquet/CSV/JSON support and SQL queries |

### Message Queues

| Package | Description |
|---------|-------------|
| `NPipeline.Connectors.Kafka` | Apache Kafka source/sink with Avro/Protobuf support |
| `NPipeline.Connectors.RabbitMQ` | RabbitMQ streaming source/sink (async API) |
| `NPipeline.Connectors.Azure.ServiceBus` | Azure Service Bus source/sink with queues and topics |
| `NPipeline.Connectors.Aws.Sqs` | AWS SQS source/sink for reliable message queuing |

### Specialized

| Package | Description |
|---------|-------------|
| `NPipeline.Connectors.Http` | HTTP/REST API source/sink with pagination, auth, retry, rate limiting |
| `NPipeline.Connectors.DataLake` | Data Lake table abstractions with partitioning, manifests, snapshots, time travel |

### Connector Analyzers

| Package | Description |
|---------|-------------|
| `NPipeline.Connectors.Postgres.Analyzers` | PostgreSQL-specific analyzers (checkpointing validation) |
| `NPipeline.Connectors.SqlServer.Analyzers` | SQL Server-specific analyzers (checkpointing validation) |

## Storage Provider Packages

Storage providers implement the `IStorageProvider` abstraction used by file-based connectors (CSV, JSON, Parquet, Excel) to read and write from different storage backends.

| Package | Description |
|---------|-------------|
| `NPipeline.StorageProviders.S3` | AWS S3 storage provider |
| `NPipeline.StorageProviders.S3.Compatible` | S3-compatible storage (MinIO, DigitalOcean Spaces, etc.) |
| `NPipeline.StorageProviders.Azure` | Azure Blob Storage provider |
| `NPipeline.StorageProviders.Adls` | Azure Data Lake Storage Gen2 provider |
| `NPipeline.StorageProviders.Gcp` | Google Cloud Storage provider |
| `NPipeline.StorageProviders.Sftp` | SFTP storage provider |

## Installing Extensions

All packages are available on NuGet. Install with the .NET CLI:

```bash
dotnet add package NPipeline.Extensions.DependencyInjection
dotnet add package NPipeline.Connectors.Csv
dotnet add package NPipeline.StorageProviders.S3
```

Or add multiple packages to your `.csproj`:

```xml
<ItemGroup>
    <PackageReference Include="NPipeline" />
    <PackageReference Include="NPipeline.Extensions.DependencyInjection" />
    <PackageReference Include="NPipeline.Connectors.Csv" />
    <PackageReference Include="NPipeline.StorageProviders.S3" />
</ItemGroup>
```

## Next Steps

- [Dependency Injection](dependency-injection.md) — DI integration reference
- [Utility Nodes](utility-nodes.md) — pre-built nodes for common data operations
- [Composition](composition.md) — hierarchical pipeline reference
- [Parallelism](parallelism.md) — parallel execution strategies
- [Observability](observability.md) — metrics and monitoring
- [OpenTelemetry](opentelemetry.md) — distributed tracing
- [Lineage](lineage.md) — data provenance tracking
- [Testing](testing.md) — test harness and assertions
- [Connectors](../connectors/index.md) — choosing the right connector for your data source
- [Storage Providers](../storage-providers/index.md) — choosing a storage backend
- [Getting Started](../getting-started/installation.md) — install your first packages

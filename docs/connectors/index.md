---
title: Connectors Overview
description: An overview of the available source and sink connectors for integrating NPipelines with external systems.
sidebar_position: 1
slug: /connectors
---

## Connectors Overview

Connectors are pre-built nodes that make it easy to read data from and write data to external systems. They are specialized `ISourceNode` and `ISinkNode` implementations that handle the specifics of communicating with systems like databases, file formats, message queues, and cloud services.

Using connectors, you can quickly assemble pipelines that integrate with your existing infrastructure without having to write boilerplate code for file I/O or network communication.

## Core Concepts

### Storage Abstraction

All connectors work through the `IStorageProvider` abstraction from the `NPipeline.StorageProviders` project, which enables them to work with multiple backend systems:

- **[Storage Provider Interface](../storage-providers/storage-provider.md)** - Learn about the abstraction layer that powers connectors
  - Works with filesystems, cloud storage (S3, Azure), databases, and custom backends
  - Unified API for read, write, delete, list, and metadata operations
  - Built-in support for filesystem with resilient directory traversal

> **Note:** Connectors depend on `NPipeline.StorageProviders` for storage abstractions. The storage provider interfaces and implementations have been extracted from `NPipeline.Connectors` into a separate `NPipeline.StorageProviders` project.

### Common Attributes

All connectors support common attributes from `NPipeline.Connectors.Attributes` that provide a unified way to map properties across different data sources:

- **`[Column]`**: Specify column names and control property mapping
- **`[IgnoreColumn]`**: Exclude properties from mapping

These common attributes work across all connectors (CSV, Excel, PostgreSQL, SQL Server, etc.) and are recommended for new code. Each connector also provides connector-specific attributes for backward compatibility and advanced features. See individual connector documentation for details and examples.

## Available Connectors

The following connectors are available:

- **[CSV](./csv.md)**: Read from and write to Comma-Separated Values (CSV) files.
  - Works with any storage backend via the `IStorageProvider` abstraction from `NPipeline.StorageProviders`
- **[Excel](./excel.md)**: Read from and write to Excel files (XLS and XLSX formats).
  - Supports both legacy XLS (binary) and modern XLSX (Open XML) formats
  - Configurable sheet selection, header handling, and type detection
  - Works with any storage backend via the `IStorageProvider` abstraction from `NPipeline.StorageProviders`
- **[JSON](./json.md)**: Read from and write to JSON files (Array and NDJSON formats).
  - Supports both JSON array and newline-delimited JSON (NDJSON) formats
  - Configurable property naming policies, indentation, and error handling
  - Uses System.Text.Json for efficient streaming with minimal dependencies
  - Works with any storage backend via the `IStorageProvider` abstraction from `NPipeline.StorageProviders`
- **[PostgreSQL](./postgres.md)**: Read from and write to PostgreSQL databases.
  - Supports streaming reads, per-row and batched writes, and in-memory checkpointing
  - Uses Npgsql library for reliable database operations
- **[SQL Server](./sqlserver.md)**: Read from and write to Microsoft SQL Server databases.
  - Supports streaming reads, per-row and batched writes, and in-memory checkpointing
  - Uses Microsoft.Data.SqlClient for reliable database operations
  - Supports Windows Authentication and SQL Server Authentication
- **[Azure Cosmos DB](./cosmos.md)**: Read from and write to Azure Cosmos DB databases.
  - Supports SQL API with parameterized queries and change feed streaming
  - Multi-API support for Mongo and Cassandra APIs
  - Multiple write strategies (PerRow, Batch, TransactionalBatch, Bulk)
  - Azure AD authentication and connection pooling
  - Uses Azure.Cosmos SDK for reliable operations
- **[AWS SQS](./aws-sqs.md)**: Read from and write to Amazon Simple Queue Service (SQS).
  - Supports multiple acknowledgment strategies (AutoOnSinkSuccess, Manual, Delayed, None)
  - Includes batch acknowledgment for performance optimization
  - Configurable long polling, parallel processing, and retry logic
  - Uses AWSSDK.SQS for reliable SQS operations
- **[Kafka](./kafka.md)**: Read from and write to Apache Kafka topics.
  - Supports multiple delivery semantics (at-least-once, exactly-once)
  - Configurable batching, retry strategies, and error handling
  - Multiple serialization formats (JSON, Avro, Protobuf)
  - Transaction support with proper offset management
  - Uses Confluent.Kafka for reliable Kafka operations

## General Usage Pattern

Most source connectors are added to a pipeline using `AddSource()` and sink connectors are added using `AddSink()`.
When you need to pass configuration (file path, resolver, etc.), instantiate the connector and register it with the builder using the overloads that accept a preconfigured node instance. These helpers automatically call `AddPreconfiguredNodeInstance()` and track disposal for you.

```csharp
// Example of using a source and sink connector
var pipeline = new PipelineBuilder()
  // Read data from a source connector
    .AddSource(new CsvSourceNode<User>(
        StorageUri.FromFilePath("users.csv"),
        row => new User(
            row.Get<int>("Id") ?? 0,
            row.Get<string>("Name") ?? string.Empty,
            row.Get<string>("Email") ?? string.Empty)), "user_source")

    // ... add transforms ...

    // Write data to a sink connector
    .AddSink(new CsvSinkNode<UserSummary>(StorageUri.FromFilePath("summaries.csv")), "summary_sink")
  .Build();
```

> **Note:** NPipeline uses a storage abstraction layer from `NPipeline.StorageProviders` that requires `StorageUri` objects instead of plain file paths. Use `StorageUri.FromFilePath()` for local files or `StorageUri.Parse()` for absolute URIs (e.g., "s3://bucket/key"). For local files, the resolver is optional. For custom providers or cloud storage, create a resolver via `StorageProviderFactory.CreateResolver()` and pass it explicitly.

Explore the documentation for each specific connector to learn about its installation, configuration options, and usage examples.

## Next Steps

- **[CSV Connector](csv.md)**: Learn how to read from and write to CSV files
- **[Excel Connector](excel.md)**: Learn how to read from and write to Excel files (XLS and XLSX)
- **[JSON Connector](json.md)**: Learn how to read from and write to JSON files (Array and NDJSON)
- **[PostgreSQL Connector](postgres.md)**: Learn how to read from and write to PostgreSQL databases
- **[SQL Server Connector](sqlserver.md)**: Learn how to read from and write to Microsoft SQL Server databases
- **[Azure Cosmos DB Connector](cosmos.md)**: Learn how to read from and write to Azure Cosmos DB
- **[AWS SQS Connector](aws-sqs.md)**: Learn how to read from and write to Amazon SQS queues
- **[Kafka Connector](kafka.md)**: Learn how to read from and write to Apache Kafka topics
- **[Common Patterns](../core-concepts/common-patterns.md)**: See connectors in practical examples
- **[Installation](../getting-started/installation.md)**: Review installation options for connector packages

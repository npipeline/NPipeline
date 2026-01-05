---
title: Connectors Overview
description: An overview of the available source and sink connectors for integrating NPipelines with external systems.
sidebar_position: 1
slug: /connectors
---

# Connectors Overview

Connectors are pre-built nodes that make it easy to read data from and write data to external systems. They are specialized `ISourceNode` and `ISinkNode` implementations that handle the specifics of communicating with systems like databases, file formats, message queues, and cloud services.

Using connectors, you can quickly assemble pipelines that integrate with your existing infrastructure without having to write boilerplate code for file I/O or network communication.

## Core Concepts

### Storage Abstraction

All connectors work through the `IStorageProvider` abstraction, which enables them to work with multiple backend systems:

- **[Storage Provider Interface](./storage-provider.md)** - Learn about the abstraction layer that powers connectors
  - Works with filesystems, cloud storage (S3, Azure), databases, and custom backends
  - Unified API for read, write, delete, list, and metadata operations
  - Built-in support for filesystem with resilient directory traversal

## Available Connectors

The following connectors are available:

- **[CSV](./csv.md)**: Read from and write to Comma-Separated Values (CSV) files.
  - Works with any storage backend via the `IStorageProvider` abstraction
- **[Excel](./excel.md)**: Read from and write to Excel files (XLS and XLSX formats).
  - Supports both legacy XLS (binary) and modern XLSX (Open XML) formats
  - Configurable sheet selection, header handling, and type detection
  - Works with any storage backend via the `IStorageProvider` abstraction

## General Usage Pattern

Most source connectors are added to a pipeline using `AddSource()`, and sink connectors are added using `AddSink()`. They require some configuration, such as a file path and a storage resolver, which are passed to their constructor.

```csharp
// Example of using a source and sink connector
var resolver = StorageProviderFactory.CreateResolver().Resolver;

var pipeline = new PipelineBuilder()
    // Read data from a source connector
    .AddSource("user_source", new CsvSourceNode<User>(StorageUri.FromFilePath("users.csv"), resolver))

    // ... add transforms ...

    // Write data to a sink connector
    .AddSink("summary_sink", new CsvSinkNode<UserSummary>(StorageUri.FromFilePath("summaries.csv"), resolver), "summarizer")
    .Build();
```

> **Note:** NPipeline uses a storage abstraction layer that requires `StorageUri` objects instead of plain file paths. Use `StorageUri.FromFilePath()` for local files or `StorageUri.Parse()` for absolute URIs (e.g., "s3://bucket/key"). Always provide a resolver created via `StorageProviderFactory.CreateResolver().Resolver`.

Explore the documentation for each specific connector to learn about its installation, configuration options, and usage examples.

## Next Steps

- **[CSV Connector](csv.md)**: Learn how to read from and write to CSV files
- **[Excel Connector](excel.md)**: Learn how to read from and write to Excel files (XLS and XLSX)
- **[Common Patterns](../core-concepts/common-patterns.md)**: See connectors in practical examples
- **[Installation](../getting-started/installation.md)**: Review installation options for connector packages

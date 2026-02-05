---
title: Storage Providers Overview
description: An overview of the available storage providers for NPipeline.
sidebar_position: 1
slug: /storage-providers
---

## Storage Providers Overview

Storage providers are the underlying abstraction layer that enables NPipeline connectors to work with different storage backends. All connectors (CSV, Excel, PostgreSQL, SQL Server, etc.) use the `IStorageProvider` interface from the `NPipeline.StorageProviders` namespace to read from and write to various storage systems.

> **Note:** Storage provider abstractions are now located in the `NPipeline.StorageProviders` namespace/assembly. Connectors depend on this project for storage operations.

This abstraction allows you to:

- Use the same connector code with different storage backends
- Switch between local files, cloud storage, and databases without changing connector logic
- Implement custom storage providers for specialized systems

## Core Concepts

### IStorageProvider Interface

The `IStorageProvider` interface (from `NPipeline.StorageProviders`) defines a unified API for storage operations:

- **Read Operations**: Open streams for reading data
- **Write Operations**: Open streams for writing data
- **List Operations**: Enumerate files and directories
- **Metadata Operations**: Retrieve file metadata and check existence
- **Delete Operations**: Remove files (optional, not all providers support this)

### StorageUri

The `StorageUri` class (from `NPipeline.StorageProviders`) represents a normalized storage location URI. It supports:

- **Local files**: `file:///path/to/file` or via `StorageUri.FromFilePath()`
- **Cloud storage**: `s3://bucket/key`, `azure://container/blob`, etc.
- **Custom schemes**: Any scheme supported by registered providers

### IStorageResolver

The `IStorageResolver` interface (from `NPipeline.StorageProviders`) is responsible for discovering and resolving storage providers capable of handling a given `StorageUri`. The resolver:

- Examines the URI scheme
- Returns the appropriate provider instance
- Caches providers for performance

## Available Storage Providers

The following storage providers are available:

- **[Storage Provider Interface](./storage-provider.md)**: Learn about the storage abstraction layer that powers connectors
  - Works with filesystems, cloud storage (S3, Azure), databases, and custom backends
  - Unified API for read, write, delete, list, and metadata operations
  - Built-in support for filesystem with resilient directory traversal
- **[AWS S3](./aws-s3.md)**: Read from and write to Amazon S3 and S3-compatible storage services.
  - Supports AWS S3, MinIO, LocalStack, and other S3-compatible services
  - Stream-based I/O for efficient handling of large files
  - Flexible authentication via AWS credential chain or explicit credentials
  - Multipart upload for large files (configurable threshold)

## Usage Pattern

Most connectors automatically create a default resolver configured with the file system provider when no resolver is provided. This is ideal for most use cases involving local files.

For cloud storage or custom providers, you need to create a custom resolver:

```csharp
using NPipeline.Connectors;
using NPipeline.StorageProviders.Aws.S3;

// Create a resolver with S3 support
var resolver = StorageProviderFactory.CreateResolver(
    new StorageResolverOptions
    {
        IncludeFileSystem = true,
        AdditionalProviders = new[] { new S3StorageProvider() }
    }
);

// Use the resolver with a connector
var source = new CsvSourceNode<User>(
    StorageUri.Parse("s3://my-bucket/users.csv"),
    row => new User(
        row.Get<int>("Id") ?? 0,
        row.Get<string>("Name") ?? string.Empty,
        row.Get<string>("Email") ?? string.Empty),
    resolver: resolver
);
```

## Configuration with Dependency Injection

Storage providers can be configured through dependency injection for cleaner application setup:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders.Aws.S3;

var services = new ServiceCollection();

services.AddS3StorageProvider(options =>
{
    options.DefaultRegion = RegionEndpoint.USEast1;
    options.UseDefaultCredentialChain = true;
});

var serviceProvider = services.BuildServiceProvider();
var provider = serviceProvider.GetRequiredService<S3StorageProvider>();
```

## Creating Custom Storage Providers

You can implement custom storage providers by implementing the `IStorageProvider` interface from `NPipeline.StorageProviders`:

```csharp
using NPipeline.StorageProviders;

public class CustomStorageProvider : IStorageProvider
{
    public async Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        // Implement read logic
    }

    public async Task<Stream> OpenWriteAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        // Implement write logic
    }

    // Implement other required methods...
}
```

## Next Steps

- **[Storage Provider Interface](./storage-provider.md)**: Learn about the storage abstraction layer
- **[AWS S3 Storage Provider](./aws-s3.md)**: Learn how to use the S3 storage provider
- **[CSV Connector](../connectors/csv.md)**: See storage providers in action with CSV files
- **[Installation](../getting-started/installation.md)**: Review installation options for storage provider packages

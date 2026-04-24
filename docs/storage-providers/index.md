---
title: Storage Providers
description: An overview of the available storage providers for NPipeline.
order: 5
---

# Storage Providers


Storage providers are the underlying abstraction layer that enables NPipeline connectors to work with different storage backends. All connectors (CSV, Excel, PostgreSQL, SQL Server, etc.) use the `IStorageProvider` interface from the `NPipeline.StorageProviders` namespace to read from and write to various storage systems.

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

- **[AWS S3](./aws-s3.md)**: Read from and write to Amazon S3 buckets.
  - AWS-native authentication via credential chain, IAM roles, or explicit credentials
  - Region-aware endpoint selection for all AWS regions
  - Stream-based I/O for efficient handling of large files
  - Multipart upload for large files (configurable threshold)
- **[S3-Compatible Storage](./s3-compatible.md)**: Read from and write to S3-compatible storage services.
  - Supports MinIO, DigitalOcean Spaces, Cloudflare R2, Wasabi, and other S3-compatible platforms
  - Static credential authentication (no AWS IAM required)
  - Purpose-built for custom endpoints and non-AWS services
  - Stream-based I/O for efficient handling of large files
  - Multipart upload for large files (configurable threshold)
- **[Azure Blob Storage](./azure-blob.md)**: Read from and write to Azure Blob Storage.
  - Supports Azure Blob Storage and Azurite emulator for local development
  - Stream-based I/O for efficient handling of large files
  - Flexible authentication via Azure credential chain, connection string, SAS token, or explicit credentials
  - Block blob upload for large files (configurable threshold)
- **[Azure Data Lake Storage Gen2](./adls-gen2.md)**: Read from and write to Azure Data Lake Storage Gen2.
  - Supports ADLS Gen2 and Azurite emulator for local development
  - True hierarchical namespace with POSIX-like directory structure
  - Atomic move/rename operations via O(1) server-side rename
  - Stream-based I/O for efficient handling of large files
  - Flexible authentication via Azure credential chain, connection string, SAS token, or explicit credentials
- **[Google Cloud Storage](./gcs-storage-provider.md)**: Read from and write to Google Cloud Storage.
  - Supports Google Cloud Storage and fake-gcs-server emulator for local development
  - Stream-based I/O for efficient handling of large files
  - Flexible authentication via Application Default Credentials, service account JSON, or access tokens
  - Resumable uploads for large files (configurable chunk size)
- **[SFTP](./sftp.md)**: Read from and write to files on SFTP servers.
  - Secure file transfer over SSH with key and password authentication
  - Stream-based I/O for efficient handling of large files
  - Configurable connection pooling and timeout handling
- **[MongoDB Connector Storage URI Support](../connectors/mongodb.md)**: Use `mongodb://` and `mongodb+srv://` URIs through the storage abstraction.
  - Supports `StorageUri`-based source and sink configuration for MongoDB connectors
  - Enables resolver-driven connection string handling for MongoDB-backed pipelines
- **[Storage Provider Interface](./storage-provider.md)**: Learn about the storage abstraction layer that powers connectors
  - Works with filesystems, cloud storage (S3, Azure), databases, and custom backends
  - Unified API for read, write, delete, list, and metadata operations
  - Built-in support for filesystem with resilient directory traversal

## Usage Pattern

Most connectors automatically create a default resolver configured with the file system provider when no resolver is provided. This is ideal for most use cases involving local files.

For cloud storage or custom providers, you need to create a custom resolver:

```csharp
using NPipeline.Connectors;
using NPipeline.StorageProviders.S3.Aws;
using Microsoft.Extensions.DependencyInjection;

// Create a resolver with AWS S3 support
var services = new ServiceCollection();
services.AddAwsS3StorageProvider(options =>
{
    options.UseDefaultCredentialChain = true;
});

var resolver = StorageProviderFactory.CreateResolver(
    new StorageResolverOptions
    {
        IncludeFileSystem = true,
        AdditionalProviders = new[] { services.BuildServiceProvider().GetRequiredService<AwsS3StorageProvider>() }
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
using NPipeline.StorageProviders.S3.Aws;
using Amazon;

var services = new ServiceCollection();

services.AddAwsS3StorageProvider(options =>
{
    options.DefaultRegion = RegionEndpoint.USEast1;
    options.UseDefaultCredentialChain = true;
});

var serviceProvider = services.BuildServiceProvider();
var provider = serviceProvider.GetRequiredService<AwsS3StorageProvider>();
```

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders.S3.Compatible;

var services = new ServiceCollection();

services.AddS3CompatibleStorageProvider(new S3CompatibleStorageProviderOptions
{
    ServiceUrl = new Uri("https://nyc3.digitaloceanspaces.com"),
    AccessKey = "your-access-key",
    SecretKey = "your-secret-key",
});

var serviceProvider = services.BuildServiceProvider();
var provider = serviceProvider.GetRequiredService<S3CompatibleStorageProvider>();
```

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders.Azure;

var services = new ServiceCollection();

services.AddAzureBlobStorageProvider(options =>
{
    options.UseDefaultCredentialChain = true;
    options.BlockBlobUploadThresholdBytes = 64 * 1024 * 1024; // 64 MB
});

var serviceProvider = services.BuildServiceProvider();
var provider = serviceProvider.GetRequiredService<AzureBlobStorageProvider>();
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

- **[AWS S3 Storage Provider](./aws-s3.md)**: Learn how to use the AWS S3 storage provider
- **[Azure Blob Storage Provider](./azure-blob.md)**: Learn how to use the Azure Blob Storage provider
- **[Azure Data Lake Storage Gen2 Provider](./adls-gen2.md)**: Learn how to use the Azure Data Lake Storage Gen2 provider
- **[Google Cloud Storage Provider](./gcs-storage-provider.md)**: Learn how to use the Google Cloud Storage provider
- **[S3-Compatible Storage Provider](./s3-compatible.md)**: Learn how to use MinIO, DigitalOcean Spaces, Cloudflare R2, and other S3-compatible services
- **[SFTP Storage Provider](./sftp.md)**: Learn how to use the SFTP storage provider
- **[MongoDB Connector](../connectors/mongodb.md)**: Learn how MongoDB connectors use `StorageUri` with `mongodb` and `mongodb+srv` schemes
- **[Storage Provider Interface](./storage-provider.md)**: Learn about the storage abstraction layer
- **[CSV Connector](../connectors/csv.md)**: See storage providers in action with CSV files
- **[Installation](../getting-started/installation.md)**: Review installation options for storage provider packages

---
title: "Storage Providers"
description: "Abstract file storage so connectors work with local files, S3, Azure Blob, GCS, and SFTP."
order: 5
---

# Storage Providers

> **Prerequisites:** [Connectors Overview](../connectors/index.md)

A [storage provider](../reference/glossary.md#storage-provider) implements `IStorageProvider` — a unified interface for reading and writing files regardless of where they're stored. File-based [connectors](../connectors/index.md) (CSV, JSON, Parquet, Excel) use storage providers so the same pipeline code works with local files, cloud storage, and SFTP.

## The IStorageProvider Interface

```csharp
public interface IStorageProvider
{
    StorageScheme Scheme { get; }
    bool CanHandle(StorageUri uri);
    Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken ct = default);
    Task<Stream> OpenWriteAsync(StorageUri uri, CancellationToken ct = default);
    Task<bool> ExistsAsync(StorageUri uri, CancellationToken ct = default);
    IAsyncEnumerable<StorageItem> ListAsync(StorageUri prefix, bool recursive = false, CancellationToken ct = default);
}
```

Extended interfaces add optional capabilities: `IDeletableStorageProvider` (delete files), `IMoveableStorageProvider` (move/rename), `IConfigurableStorageProvider` (runtime configuration).

## Choosing a Provider

| Provider | System | Package |
|----------|--------|---------|
| Built-in | Local file system | `NPipeline.StorageProviders` |
| [AWS S3](aws-s3.md) | Amazon S3 | `NPipeline.StorageProviders.S3.Aws` |
| [Azure Blob](azure-blob.md) | Azure Blob Storage | `NPipeline.StorageProviders.Azure` |
| [ADLS Gen2](adls-gen2.md) | Azure Data Lake Storage Gen2 | `NPipeline.StorageProviders.Adls` |
| [Google Cloud Storage](gcs.md) | Google Cloud Storage | `NPipeline.StorageProviders.Gcp` |
| [SFTP](sftp.md) | SFTP servers | `NPipeline.StorageProviders.Sftp` |
| [S3-Compatible](s3-compatible.md) | MinIO, DigitalOcean Spaces, Cloudflare R2 | `NPipeline.StorageProviders.S3.Compatible` |

## Usage with Connectors

Pass a storage provider to any file-based connector:

```csharp
var storage = new AwsS3StorageProvider(new AwsS3StorageProviderOptions
{
    DefaultRegion = RegionEndpoint.USEast1
});

var config = new CsvConfiguration { HasHeaderRecord = true };
var uri = new StorageUri("s3://my-bucket/data/orders.csv");
var source = new CsvSourceNode<Order>(config, storage, uri);
```

Switch storage without changing pipeline logic:

```csharp
// Local development
var storage = new FileSystemStorageProvider();
var uri = new StorageUri("file:///data/orders.csv");

// Production (same connector, different storage)
var storage = new AwsS3StorageProvider(s3Options);
var uri = new StorageUri("s3://prod-bucket/data/orders.csv");
```

## DI Registration

Each provider package includes `IServiceCollection` extensions:

```csharp
services.AddNPipelineS3Storage(options => { /* configure */ });
services.AddNPipelineAzureBlobStorage(options => { /* configure */ });
services.AddNPipelineGcsStorage(options => { /* configure */ });
```

## Storage Resolver

`StorageResolver` automatically selects the right provider based on URI scheme:

```csharp
var resolver = new StorageResolver(new IStorageProvider[]
{
    new FileSystemStorageProvider(),
    new AwsS3StorageProvider(s3Options),
    new AzureBlobStorageProvider(blobOptions)
});

// Resolver picks the right provider based on URI scheme
var stream = await resolver.OpenReadAsync(new StorageUri("s3://bucket/file.csv"));
```

## Custom Provider

See [Implementing a Custom Provider](custom-provider.md) if you need to support a storage system that isn't covered.

## StorageUri

`StorageUri` is the address type used by all storage providers. It supports local files, cloud storage, and SFTP — with optional query parameters for per-request configuration:

```csharp
// Local file
var local = StorageUri.FromFilePath("/data/orders.csv");

// S3
var s3 = StorageUri.Parse("s3://my-bucket/data/orders.csv?region=us-west-2");

// Azure Blob
var azure = StorageUri.Parse("azure://my-container/data/orders.csv");

// ADLS Gen2
var adls = StorageUri.Parse("adls://my-filesystem/data/orders.parquet");

// GCS
var gcs = StorageUri.Parse("gs://my-bucket/data/orders.csv");

// SFTP
var sftp = StorageUri.Parse("sftp://server.example.com/data/orders.csv");
```

Properties: `Scheme`, `Host`, `Path`, `Port`, `UserInfo`, `Parameters`.

## Built-in FileSystem Provider

The `FileSystemStorageProvider` handles `file://` URIs and local paths. It's included in `NPipeline.StorageProviders` with no additional dependencies:

```csharp
var provider = new FileSystemStorageProvider();
var uri = StorageUri.FromFilePath("data/orders.csv");

using var stream = await provider.OpenReadAsync(uri);
```

## Common Operations

All providers expose the same operations:

```csharp
// Read
using var readStream = await provider.OpenReadAsync(uri);

// Write
using var writeStream = await provider.OpenWriteAsync(uri);

// Exists
bool exists = await provider.ExistsAsync(uri);

// List (recursive or non-recursive)
await foreach (var item in provider.ListAsync(prefix, recursive: true))
{
    Console.WriteLine($"{item.Uri} — {item.Size} bytes — {item.LastModified}");
}

// Metadata (via IStorageProviderMetadataProvider)
var metadata = await provider.GetMetadataAsync(uri);
```

## Next Steps

- Pick a provider from the table above to see configuration details
- [Custom Provider](custom-provider.md) — implement your own storage provider
- [Connectors](../connectors/index.md) — use storage providers with file-based connectors

---
title: "Azure Blob Storage Provider"
description: "Read and write files in Azure Blob Storage with managed identity, block blob optimization, and client caching."
order: 3
---

# Azure Blob Storage Provider

> **Prerequisites:** [Storage Providers Overview](index.md)

The `NPipeline.StorageProviders.Azure` package implements `IStorageProvider` for [Azure Blob Storage](https://learn.microsoft.com/en-us/azure/storage/blobs/). Supports `DefaultAzureCredential`, connection strings, chunked block blob uploads, client caching with LRU eviction, and the Azurite emulator.

## Installation

```bash
dotnet add package NPipeline.StorageProviders.Azure
```

**Dependencies:** [Azure.Storage.Blobs](https://www.nuget.org/packages/Azure.Storage.Blobs) 12.x, [Azure.Identity](https://www.nuget.org/packages/Azure.Identity) 1.x

## Quick Start

```csharp
var options = new AzureBlobStorageProviderOptions
{
    DefaultConnectionString = "DefaultEndpointsProtocol=https;AccountName=mystorageaccount;AccountKey=..."
};
var factory = new AzureBlobClientFactory(options);
var provider = new AzureBlobStorageProvider(factory, options);

var stream = await provider.OpenReadAsync(
    StorageUri.Parse("azure://my-container/data/orders.csv"));
```

## URI Format

```
azure://container-name/blob/path
```

| Component | Description |
|-----------|-------------|
| `container-name` | Blob container (URI host) |
| `blob/path` | Blob name (URI path) |

## Authentication

Credentials are resolved in this order:

1. **Connection string** — `DefaultConnectionString` (takes precedence when set)
2. **Explicit credential** — `DefaultCredential` (any `TokenCredential`)
3. **Default credential chain** — `DefaultAzureCredential` (environment → managed identity → Azure CLI)

```csharp
// Connection string
var options = new AzureBlobStorageProviderOptions
{
    DefaultConnectionString = "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=..."
};

// Managed identity (recommended for production)
var options = new AzureBlobStorageProviderOptions
{
    UseDefaultCredentialChain = true    // default
};

// Explicit credential
var options = new AzureBlobStorageProviderOptions
{
    DefaultCredential = new ManagedIdentityCredential()
};
```

## Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `DefaultConnectionString` | `string?` | `null` | Azure Storage connection string |
| `DefaultCredential` | `TokenCredential?` | `null` | Azure `TokenCredential` |
| `UseDefaultCredentialChain` | `bool` | `true` | Use `DefaultAzureCredential` |
| `ServiceUrl` | `Uri?` | `null` | Custom Blob service URL (Azurite) |
| `ServiceVersion` | `BlobClientOptions.ServiceVersion?` | `null` | API version override |
| `BlockBlobUploadThresholdBytes` | `long` | `64 MB` | Switch to staged upload above this size |
| `UploadMaximumConcurrency` | `int?` | `null` | Parallel upload threads |
| `UploadMaximumTransferSizeBytes` | `int?` | `null` | Block size for staged uploads |
| `ClientCacheSizeLimit` | `int` | `100` | Max cached `BlobContainerClient` instances |

## Dependency Injection

```csharp
// Default options
services.AddAzureBlobStorageProvider();

// Configure inline
services.AddAzureBlobStorageProvider(options =>
{
    options.DefaultConnectionString = "...";
});
```

Registers: `IStorageProvider`, `IStorageProviderMetadataProvider`

## Features

- **Chunked uploads** — files above `BlockBlobUploadThresholdBytes` use the staged block upload API
- **Client caching** — `BlobContainerClient` instances are cached per container; LRU eviction when `ClientCacheSizeLimit` is reached
- **Azurite emulator** — set `ServiceUrl` for local development
- **Metadata** — implements `IStorageProviderMetadataProvider` for `Size`, `LastModified`, `ContentType`, `ETag`

## URI Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `accountName` | Storage account name | `accountName=mystorageaccount` |
| `accountKey` | Account key (dev only) | `accountKey=mykey` |
| `sasToken` | SAS token (URL-encoded) | `sasToken=sp%3Dr%26st%3D2023-01-01` |
| `connectionString` | Full connection string | `connectionString=DefaultEndpointsProtocol=...` |
| `serviceUrl` | Custom service URL (Azurite) | `serviceUrl=http://localhost:10000/devstoreaccount1` |
| `contentType` | Content type on write | `contentType=application/json` |

```csharp
// With SAS token
var uri = StorageUri.Parse("azure://my-container/data/output.json?sasToken=sp%3Dr%26st%3D2023-01-01");

// Azurite endpoint
var uri = StorageUri.Parse("azure://my-container/data/file.csv?serviceUrl=http://localhost:10000/devstoreaccount1");
```

> **Security:** Avoid credentials in URIs for production. Use the credential chain or connection strings via DI.

## Configuration Examples

### Azurite (Local Development)

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.ServiceUrl = new Uri("http://localhost:10000/devstoreaccount1");
    options.DefaultConnectionString = "UseDevelopmentStorage=true";
});
```

### Custom Upload Settings

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.BlockBlobUploadThresholdBytes = 128 * 1024 * 1024; // 128 MB
    options.UploadMaximumConcurrency = 8;
    options.UploadMaximumTransferSizeBytes = 8 * 1024 * 1024;  // 8 MB blocks
});
```

## Examples

### Reading

```csharp
var uri = StorageUri.Parse("azure://my-container/data.csv");

using var stream = await provider.OpenReadAsync(uri);
using var reader = new StreamReader(stream);
var content = await reader.ReadToEndAsync();
```

### Writing

```csharp
var uri = StorageUri.Parse("azure://my-container/output.csv");

using var stream = await provider.OpenWriteAsync(uri);
using var writer = new StreamWriter(stream);
await writer.WriteLineAsync("id,name,value");
```

### Listing

```csharp
var prefix = StorageUri.Parse("azure://my-container/data/");

await foreach (var item in provider.ListAsync(prefix, recursive: true))
{
    Console.WriteLine($"{item.Uri} — {item.Size} bytes");
}
```

### Metadata

```csharp
var metadata = await provider.GetMetadataAsync(uri);
if (metadata is not null)
{
    Console.WriteLine($"Size: {metadata.Size}, ContentType: {metadata.ContentType}");
    Console.WriteLine($"ETag: {metadata.ETag}");
}
```

## Error Handling

| Azure Error | HTTP Status | .NET Exception |
|-------------|-------------|----------------|
| `AuthenticationFailed` | 401 | `UnauthorizedAccessException` |
| `AuthorizationFailed` | 403 | `UnauthorizedAccessException` |
| `ContainerNotFound`, `BlobNotFound` | 404 | `FileNotFoundException` |
| `InvalidResourceName` | 400 | `ArgumentException` |
| Other `RequestFailedException` | Various | `IOException` |

## Azure Permissions

| Operation | Required Permission |
|-----------|---------------------|
| `OpenReadAsync` | `Storage Blob Data Reader` |
| `OpenWriteAsync` | `Storage Blob Data Contributor` |
| `ListAsync` | `Storage Blob Data Reader` |
| `ExistsAsync` | `Storage Blob Data Reader` |

Assign `Storage Blob Data Contributor` for full read/write/delete access. For read-only pipelines, `Storage Blob Data Reader` is sufficient.

## Limitations

- **Flat storage** — Blob Storage has no directories; prefix-based hierarchy is simulated
- **Concurrent writes** — writing to the same blob from multiple threads may race; use locking or versioning
- **Block blob size** — maximum 190.7 TiB per blob; each block max 4,000 MiB

## Next Steps

- [ADLS Gen2 Provider](adls-gen2.md) — hierarchical namespace on Azure
- [AWS S3 Provider](aws-s3.md) — Amazon alternative
- [Storage Providers Overview](index.md) — choosing between providers

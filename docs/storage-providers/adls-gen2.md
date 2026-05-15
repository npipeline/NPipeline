---
title: "ADLS Gen2 Storage Provider"
description: "Read and write files in Azure Data Lake Storage Gen2 with hierarchical namespace, atomic rename, and directory-level ACLs."
order: 4
---

# ADLS Gen2 Storage Provider

> **Prerequisites:** [Storage Providers Overview](index.md)

The `NPipeline.StorageProviders.Adls` package implements `IStorageProvider` for [Azure Data Lake Storage Gen2](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction). In addition to standard read/write, it implements `IDeletableStorageProvider` and `IMoveableStorageProvider` for atomic rename/move operations — critical for partitioned table writes.

## When to Use ADLS Gen2 vs Azure Blob

| Feature | Azure Blob | ADLS Gen2 |
|---------|------------|----------|
| Flat namespace | Yes | Yes |
| Hierarchical namespace | No | Yes |
| Directory-level ACLs | No | Yes |
| Atomic rename/move | No | Yes |
| Data Lake connector | Limited | Full support |

Use ADLS Gen2 when you need hierarchical namespaces, directory-level ACLs, or the [Data Lake connector's](../connectors/datalake.md) partitioned table features.

## Installation

```bash
dotnet add package NPipeline.StorageProviders.Adls
```

**Dependencies:** [Azure.Storage.Blobs](https://www.nuget.org/packages/Azure.Storage.Blobs) 12.x, [Azure.Storage.Files.DataLake](https://www.nuget.org/packages/Azure.Storage.Files.DataLake) 12.x, [Azure.Identity](https://www.nuget.org/packages/Azure.Identity) 1.x

## Quick Start

```csharp
var options = new AdlsGen2StorageProviderOptions
{
    DefaultConnectionString = "DefaultEndpointsProtocol=https;AccountName=mydatalake;AccountKey=..."
};
var factory = new AdlsGen2ClientFactory(options);
var provider = new AdlsGen2StorageProvider(factory, options);

var stream = await provider.OpenReadAsync(
    StorageUri.Parse("adls://my-filesystem/data/orders.parquet"));
```

## URI Format

```
adls://filesystem-name/path/to/file
```

| Component | Description |
|-----------|-------------|
| `filesystem-name` | ADLS Gen2 filesystem (URI host) |
| `path/to/file` | File path within the filesystem |

## Authentication

Same precedence as the Azure Blob provider:

1. **Connection string** — `DefaultConnectionString` (takes precedence)
2. **Explicit credential** — `DefaultCredential` (any `TokenCredential`)
3. **Default credential chain** — `DefaultAzureCredential`

```csharp
// Managed identity (recommended)
var options = new AdlsGen2StorageProviderOptions
{
    UseDefaultCredentialChain = true
};

// Connection string
var options = new AdlsGen2StorageProviderOptions
{
    DefaultConnectionString = "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=..."
};
```

## Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `DefaultConnectionString` | `string?` | `null` | ADLS connection string |
| `DefaultCredential` | `TokenCredential?` | `null` | Azure `TokenCredential` |
| `UseDefaultCredentialChain` | `bool` | `true` | Use `DefaultAzureCredential` |
| `ServiceUrl` | `Uri?` | `null` | Custom service URL (Azurite) |
| `ServiceVersion` | `DataLakeClientOptions.ServiceVersion?` | `null` | API version override |
| `UploadThresholdBytes` | `long` | `64 MB` | Staged upload threshold |
| `UploadMaximumConcurrency` | `int?` | `null` | Parallel upload threads |
| `UploadMaximumTransferSizeBytes` | `int?` | `null` | Block size for staged uploads |
| `ClientCacheSizeLimit` | `int` | `100` | Max cached client instances |

## Dependency Injection

```csharp
services.AddAdlsGen2StorageProvider();

services.AddAdlsGen2StorageProvider(options =>
{
    options.UseDefaultCredentialChain = true;
});
```

Registers: `IStorageProvider`, `IDeletableStorageProvider`, `IMoveableStorageProvider`, `IStorageProviderMetadataProvider`

## Features

- **Atomic rename/move** — `IMoveableStorageProvider` uses the Data Lake rename API; falls back to copy + delete on failure
- **Idempotent delete** — `IDeletableStorageProvider` treats 404 as success
- **Dual client caches** — separate LRU caches for Blob and DataLake clients
- **Metadata** — `Size`, `LastModified`, `ContentType`, `ETag`

## URI Parameters

| Parameter | Description |
|-----------|-------------|
| `accountName` | Storage account name (overrides default) |
| `accountKey` | Shared-key credential (base64-encoded) |
| `sasToken` | SAS token |
| `connectionString` | Full connection string |
| `contentType` | MIME type set on write |

### Naming Constraints

- **Filesystem name**: 3–63 characters; lowercase letters, digits, and hyphens; no leading/trailing hyphen
- **Path**: 1–2,048 characters; no backslash (`\`); no `?`

## Examples

### Reading

```csharp
var uri = StorageUri.Parse("adls://my-filesystem/data/records.csv");
await using var stream = await provider.OpenReadAsync(uri);
```

### Writing

```csharp
var uri = StorageUri.Parse("adls://my-filesystem/data/output.csv?contentType=text/csv");
await using var stream = await provider.OpenWriteAsync(uri);
```

Data is buffered to a local temporary file and uploaded atomically when the stream is disposed.

### Listing

```csharp
var dirUri = StorageUri.Parse("adls://my-filesystem/data/");

// Non-recursive (immediate children only)
await foreach (var item in provider.ListAsync(dirUri, recursive: false))
{
    var type = item.IsDirectory ? "[dir]" : $"{item.Size,12} bytes";
    Console.WriteLine($"  {type}  {item.Uri}");
}

// Recursive
await foreach (var item in provider.ListAsync(dirUri, recursive: true))
    Console.WriteLine(item.Uri);
```

### Metadata

```csharp
var metadata = await provider.GetMetadataAsync(uri);
if (metadata is not null)
    Console.WriteLine($"Size: {metadata.Size}, ContentType: {metadata.ContentType}, IsDirectory: {metadata.IsDirectory}");
```

### Deleting (Idempotent)

```csharp
if (provider is IDeletableStorageProvider del)
    await del.DeleteAsync(uri);   // succeeds even if path doesn't exist
```

### Moving / Renaming (Atomic)

ADLS Gen2's O(1) server-side rename is the primary differentiator over Azure Blob:

```csharp
if (provider is IMoveableStorageProvider mov)
{
    var src = StorageUri.Parse("adls://my-filesystem/staging/records.csv");
    var dest = StorageUri.Parse("adls://my-filesystem/processed/records.csv");
    await mov.MoveAsync(src, dest);
}
```

> Cross-account moves are not supported. Both source and destination must be within the same storage account.

## Error Handling

| HTTP Status / Error | .NET Exception |
|---------------------|----------------|
| `AuthenticationFailed`, `AuthorizationFailed`, 401, 403 | `UnauthorizedAccessException` |
| `FilesystemNotFound`, `PathNotFound`, 404 | `FileNotFoundException` |
| `InvalidResourceName`, 400 | `ArgumentException` |
| `PathAlreadyExists`, 409 | `IOException` |
| 429 / 5xx (transient) | `IOException` (preserves retryable context) |

`AdlsStorageException` (inherits `ConnectorException`) carries `Filesystem` and `Path` properties for structured diagnostics.

## Provider Capabilities

Via `IStorageProviderMetadataProvider.GetMetadata()`:

| Capability | Value |
|------------|-------|
| `SupportsRead` | `true` |
| `SupportsWrite` | `true` |
| `SupportsListing` | `true` |
| `SupportsMetadata` | `true` |
| `SupportsHierarchy` | `true` |
| `supportsAtomicMove` | `true` |
| `supportsNativeDelete` | `true` |

## Azurite (Local Development)

```csharp
services.AddAdlsGen2StorageProvider(options =>
{
    options.DefaultConnectionString = "UseDevelopmentStorage=true";
    options.ServiceUrl = new Uri("http://127.0.0.1:10000/devstoreaccount1/");
});
```

```bash
docker run -p 10000:10000 \
    mcr.microsoft.com/azure-storage/azurite \
    azurite --blobHost 0.0.0.0 --skipApiVersionCheck --inMemoryPersistence
```

> Azurite has partial ADLS Gen2 fidelity — ACL and some HNS behaviors may differ. Validate against a real ADLS Gen2 account for production.

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `InvalidOperationException: Account name must be provided` | No credential resolved | Provide `connectionString`, `accountName + accountKey`, or `DefaultConnectionString` |
| `FileNotFoundException` on write | Filesystem doesn't exist | Ensure filesystem exists; `UploadAsync` calls `CreateIfNotExistsAsync` automatically |
| `UnauthorizedAccessException` | Missing permissions | Assign `Storage Blob Data Contributor` (or equivalent ADLS Gen2 role) |

## Next Steps

- [Azure Blob Provider](azure-blob.md) — flat namespace alternative
- [Data Lake Connector](../connectors/datalake.md) — partitioned tables on ADLS
- [Storage Providers Overview](index.md) — choosing between providers

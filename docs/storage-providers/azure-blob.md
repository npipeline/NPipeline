---
title: "Azure Blob Storage Provider"
description: "Read and write files in Azure Blob Storage with managed identity, streaming block blob uploads, and client caching."
order: 3
---

# Azure Blob Storage Provider

> **Prerequisites:** [Storage Providers Overview](index.md)

The `NPipeline.StorageProviders.Azure` package implements `IStorageProvider` for [Azure Blob Storage](https://learn.microsoft.com/en-us/azure/storage/blobs/). Supports `DefaultAzureCredential`, connection strings, account keys, SAS tokens, streaming block blob uploads, client caching with LRU eviction, and the Azurite emulator.

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

You set credentials in the provider options. A URI never carries a credential, because URIs are logged and compared. The provider uses the first of these that you set:

1. **Connection string** - `DefaultConnectionString`. A connection string names its own endpoint, so combining it with `ServiceUrl` or a `serviceUrl` URI parameter throws `ArgumentException`.
2. **SAS token** - `DefaultSasToken`. The provider sends the token as you set it.
3. **Account key** - `DefaultAccountKey`, with `AccountName` or an `accountName` URI parameter.
4. **Explicit credential** - `DefaultCredential` (any `TokenCredential`)
5. **Default credential chain** - `DefaultAzureCredential` (environment, managed identity, Azure CLI)

If none of these is available and `AllowAnonymousAccess` is `false`, creating a client throws `InvalidOperationException`.

```csharp
// Connection string
var options = new AzureBlobStorageProviderOptions
{
    DefaultConnectionString = "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=..."
};

// Account key
var options = new AzureBlobStorageProviderOptions
{
    AccountName = "mystorageaccount",
    DefaultAccountKey = "..."
};

// SAS token
var options = new AzureBlobStorageProviderOptions
{
    AccountName = "mystorageaccount",
    DefaultSasToken = "sv=2022-11-02&sp=rw&sig=..."
};

// Managed identity (recommended for production)
var options = new AzureBlobStorageProviderOptions
{
    AccountName = "mystorageaccount",
    UseDefaultCredentialChain = true    // default
};

// Explicit credential
var options = new AzureBlobStorageProviderOptions
{
    AccountName = "mystorageaccount",
    DefaultCredential = new ManagedIdentityCredential()
};
```

> **Breaking change:** The `connectionString`, `sasToken`, and `accountKey` URI parameters are no longer supported. A URI that carries one throws `ArgumentException` that names the option to set instead.

## Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `AccountName` | `string?` | `null` | Default storage account name. An `accountName` URI parameter overrides it. Required with `DefaultAccountKey`. |
| `DefaultConnectionString` | `string?` | `null` | Azure Storage connection string |
| `DefaultSasToken` | `string?` | `null` | Shared access signature token |
| `DefaultAccountKey` | `string?` | `null` | Storage account key |
| `DefaultCredential` | `TokenCredential?` | `null` | Azure `TokenCredential` |
| `UseDefaultCredentialChain` | `bool` | `true` | Use `DefaultAzureCredential` |
| `AllowAnonymousAccess` | `bool` | `false` | Connect without credentials when none are configured, for public containers. When `false`, a missing credential throws `InvalidOperationException`. |
| `ServiceUrl` | `Uri?` | `null` | Custom Blob service URL (Azurite). A `serviceUrl` URI parameter overrides it. |
| `ServiceVersion` | `BlobClientOptions.ServiceVersion?` | `null` | API version override |
| `Retry` | `AzureRetryOptions` | see below | Retry settings for the Azure SDK clients |
| `PartSizeBytes` | `int` | `8 MiB` | Size of each upload block. An object that fits in one block uploads in one request. |
| `MaxConcurrency` | `int` | `4` | Most blocks of one object that upload at the same time |
| `CreateContainerIfMissing` | `bool` | `false` | Create the container on the first write to it |
| `ClientCacheSizeLimit` | `int` | `100` | Most `BlobServiceClient` instances kept in the cache, one per endpoint |

### Retry options

`AzureRetryOptions` sets the retry policy of every client the provider creates. The defaults match the Azure SDK's own.

| Property | Default | Description |
|----------|---------|-------------|
| `Mode` | `Exponential` | `RetryMode.Exponential` or `RetryMode.Fixed` |
| `MaxRetries` | `5` | Most retries. `0` turns retries off. |
| `Delay` | `800 ms` | Base delay between retries |
| `MaxDelay` | `8 s` | Longest delay between retries |
| `NetworkTimeout` | `100 s` | Timeout of one network operation |

```csharp
options.Retry = new AzureRetryOptions
{
    Mode = RetryMode.Fixed,
    MaxRetries = 3,
    Delay = TimeSpan.FromMilliseconds(500)
};
```

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

Registers: `IStorageProvider`

## Features

- **Streaming uploads** - the provider uploads blocks while you write. It doesn't buffer the object in a local temporary file. Memory use is about `PartSizeBytes × (MaxConcurrency + 1)`.
- **Single-request fast path** - an object that fits in one block uploads with one `Put Blob` request.
- **Client caching** - one `BlobServiceClient` is cached for each endpoint (account name and service URL). The cache never uses a credential as a key. When the cache holds `ClientCacheSizeLimit` clients, the least recently used client leaves it.
- **Azurite emulator** - set `ServiceUrl` for local development
- **Metadata** - `GetMetadataAsync` returns `Size`, `LastModified`, `ContentType`, `ETag`

## URI Parameters

A URI selects an endpoint and a blob. It doesn't carry credentials.

| Parameter | Description | Example |
|-----------|-------------|---------|
| `accountName` | Storage account name. Overrides `AccountName`. | `accountName=mystorageaccount` |
| `serviceUrl` | Custom service URL (Azurite). Overrides `ServiceUrl`. | `serviceUrl=http://localhost:10000/devstoreaccount1` |
| `contentType` | Content type on write | `contentType=application/json` |

```csharp
// Another account, using the credentials in the options
var uri = StorageUri.Parse("azure://my-container/data/output.json?accountName=otheraccount");

// Azurite endpoint
var uri = StorageUri.Parse("azure://my-container/data/file.csv?serviceUrl=http://localhost:10000/devstoreaccount1");
```

> **Security:** The `connectionString`, `sasToken`, and `accountKey` URI parameters throw `ArgumentException`. Set `DefaultConnectionString`, `DefaultSasToken`, or `DefaultAccountKey` in the options instead.

## Configuration Examples

### Azurite (Local Development)

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.ServiceUrl = new Uri("http://127.0.0.1:10000/devstoreaccount1");
    options.AccountName = "devstoreaccount1";
    options.DefaultAccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    options.CreateContainerIfMissing = true;
});
```

Azurite's account name and key are published, well-known development values.

### Creating containers

By default, the provider doesn't create containers. Creating a container needs permissions that a least-privilege token usually lacks, and costs a request. A write to a missing container fails with `FileNotFoundException` when you call `CommitAsync`. To create containers on demand, set `CreateContainerIfMissing`. The provider then creates each container at most once for each endpoint.

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.CreateContainerIfMissing = true;
});
```

### Custom Upload Settings

```csharp
services.AddAzureBlobStorageProvider(options =>
{
    options.PartSizeBytes = 16 * 1024 * 1024; // 16 MiB blocks
    options.MaxConcurrency = 8;
});
```

A block blob holds at most 50,000 blocks, which is about 390 GiB at the default 8 MiB block size. For a larger object, pass its expected size in `StorageWriteOptions.LengthHint`, and the provider raises the block size to stay within the limit. Without a hint, the provider enlarges the blocks after the first 10,000.

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

await using var stream = await provider.OpenWriteAsync(uri, cancellationToken: ct);
await using (var writer = new StreamWriter(stream, leaveOpen: true))
{
    await writer.WriteLineAsync("id,name,value");
}

// The object appears only after CommitAsync. Disposing the stream without it discards the data.
await stream.CommitAsync(ct);
```

Call `CommitAsync` once, after the last write. The provider uploads blocks while you write and doesn't use a local temporary file. A block fills, and its upload starts while you keep writing; when `MaxConcurrency` blocks are in flight, `WriteAsync` waits. `CommitAsync` uploads the last block and then commits the block list, and only then does the blob appear. If the whole object fits in one block, `CommitAsync` uploads it in a single request. Disposing the stream without committing leaves an existing blob as it was; the blocks you staged stay uncommitted, and Azure discards them after a week. A failed block upload surfaces from the next `WriteAsync` or from `CommitAsync`.

The provider declares `ConditionalWrite`. Pass `new StorageWriteOptions { Overwrite = false }` to fail the commit if the blob exists, or `new StorageWriteOptions { IfMatch = etag }` (an ETag from `GetMetadataAsync`) to commit only if the blob is unchanged. A refused condition throws `StoragePreconditionFailedException` from `CommitAsync`. A condition applies to the final request: the single upload, or the commit of the block list. Set `StorageWriteOptions.ContentType` to set the content type.

### Listing

```csharp
var prefix = StorageUri.Parse("azure://my-container/data/");

await foreach (var item in provider.ListAsync(prefix, recursive: true))
{
    Console.WriteLine($"{item.Uri} - {item.Size} bytes");
}
```

Listing makes no request to check that the container exists, and it requests no blob metadata. Listing a container that doesn't exist yields no items.

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
| `ConditionNotMet`, `BlobAlreadyExists` (conditional write) | 412, 409 | `StoragePreconditionFailedException` |
| Other `RequestFailedException` | Various | `IOException` |

## Azure Permissions

| Operation | Required Permission |
|-----------|---------------------|
| `OpenReadAsync` | `Storage Blob Data Reader` |
| `OpenWriteAsync` | `Storage Blob Data Contributor` |
| `ListAsync` | `Storage Blob Data Reader` |
| `ExistsAsync` | `Storage Blob Data Reader` |
| `DeleteAsync`, `MoveAsync` | `Storage Blob Data Contributor` |
| `OpenWriteAsync` with `CreateContainerIfMissing` | `Storage Blob Data Contributor` and permission to create containers |

Assign `Storage Blob Data Contributor` for full read/write/delete access. For read-only pipelines, `Storage Blob Data Reader` is sufficient.

## Limitations

- **Flat storage** - Blob Storage has no directories; prefix-based hierarchy is simulated
- **Concurrent writes** - writing to the same blob from multiple threads may race; use locking or versioning
- **Block blob size** - maximum 190.7 TiB per blob, in at most 50,000 blocks of up to 4,000 MiB each
- **Move isn't atomic** - `MoveAsync` copies the blob within the account and then deletes the source. Moving between accounts throws `ArgumentException`.

## Next Steps

- [ADLS Gen2 Provider](adls-gen2.md) - hierarchical namespace on Azure
- [AWS S3 Provider](aws-s3.md) - Amazon alternative
- [Storage Providers Overview](index.md) - choosing between providers

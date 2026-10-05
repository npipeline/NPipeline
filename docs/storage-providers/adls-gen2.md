---
title: "ADLS Gen2 Storage Provider"
description: "Read and write files in Azure Data Lake Storage Gen2 with streaming uploads, hierarchical namespace listing, and atomic rename."
order: 4
---

# ADLS Gen2 Storage Provider

> **Prerequisites:** [Storage Providers Overview](index.md)

The `NPipeline.StorageProviders.Adls` package implements `IStorageProvider` for [Azure Data Lake Storage Gen2](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction). In addition to standard read/write, it supports delete and move. On an account with a hierarchical namespace, a move is an atomic rename, which is critical for partitioned table writes. The package builds on `NPipeline.StorageProviders.Azure`, so it shares that package's options, client builder, retry settings, and upload engine.

## When to Use ADLS Gen2 vs Azure Blob

| Feature | Azure Blob | ADLS Gen2 |
|---------|------------|----------|
| Flat namespace | Yes | Yes |
| Hierarchical namespace | No | Yes |
| Directory-level ACLs | No | Yes |
| Atomic rename/move | No | Yes (with a hierarchical namespace) |
| Data Lake connector | Limited | Full support |

Use ADLS Gen2 when you need hierarchical namespaces, directory-level ACLs, or the [Data Lake connector's](../connectors/datalake.md) partitioned table features.

## Installation

```bash
dotnet add package NPipeline.StorageProviders.Adls
```

**Dependencies:** `NPipeline.StorageProviders.Azure` (installed with the package), [Azure.Storage.Blobs](https://www.nuget.org/packages/Azure.Storage.Blobs) 12.x, [Azure.Storage.Files.DataLake](https://www.nuget.org/packages/Azure.Storage.Files.DataLake) 12.x, [Azure.Identity](https://www.nuget.org/packages/Azure.Identity) 1.x

## Quick Start

```csharp
var options = new AdlsGen2StorageProviderOptions
{
    DefaultConnectionString = "DefaultEndpointsProtocol=https;AccountName=mydatalake;AccountKey=..."
};
using var factory = new AdlsGen2ClientFactory(options);
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

`AdlsGen2StorageProviderOptions` derives from `AzureAccountOptions`, so authentication works as it does in the [Azure Blob provider](azure-blob.md#authentication). You set credentials in the options. A URI never carries a credential. The provider uses the first of these that you set:

1. **Connection string** - `DefaultConnectionString`. A connection string names its own endpoint, so combining it with `ServiceUrl` or a `serviceUrl` URI parameter throws `ArgumentException`.
2. **SAS token** - `DefaultSasToken`
3. **Account key** - `DefaultAccountKey`, with `AccountName` or an `accountName` URI parameter
4. **Explicit credential** - `DefaultCredential` (any `TokenCredential`)
5. **Default credential chain** - `DefaultAzureCredential`

```csharp
// Managed identity (recommended)
var options = new AdlsGen2StorageProviderOptions
{
    AccountName = "mydatalake",
    UseDefaultCredentialChain = true
};

// Connection string
var options = new AdlsGen2StorageProviderOptions
{
    DefaultConnectionString = "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=..."
};

// Account key
var options = new AdlsGen2StorageProviderOptions
{
    AccountName = "mydatalake",
    DefaultAccountKey = "..."
};
```

> **Breaking change:** The `connectionString`, `sasToken`, and `accountKey` URI parameters are no longer supported. A URI that carries one throws `ArgumentException` that names the option to set instead.

## Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `AccountName` | `string?` | `null` | Default storage account name. An `accountName` URI parameter overrides it. Required with `DefaultAccountKey`. |
| `DefaultConnectionString` | `string?` | `null` | ADLS connection string |
| `DefaultSasToken` | `string?` | `null` | Shared access signature token |
| `DefaultAccountKey` | `string?` | `null` | Storage account key |
| `DefaultCredential` | `TokenCredential?` | `null` | Azure `TokenCredential` |
| `UseDefaultCredentialChain` | `bool` | `true` | Use `DefaultAzureCredential` |
| `AllowAnonymousAccess` | `bool` | `false` | Connect without credentials when none are configured, for public filesystems. When `false`, a missing credential throws `InvalidOperationException`. |
| `ServiceUrl` | `Uri?` | `null` | Custom service URL (Azurite). A `serviceUrl` URI parameter overrides it. |
| `ServiceVersion` | `DataLakeClientOptions.ServiceVersion?` | `null` | API version override |
| `Retry` | `AzureRetryOptions` | see below | Azure SDK retry settings |
| `PartSizeBytes` | `int` | `8 MiB` | Size of each upload block. A file that fits in one block uploads in one request. |
| `MaxConcurrency` | `int` | `4` | Most blocks of one file that upload at the same time |
| `CreateContainerIfMissing` | `bool` | `false` | Create the filesystem on the first write to it |
| `ClientCacheSizeLimit` | `int` | `100` | Most clients kept in each cache, one per endpoint |

### Resilience

The Azure SDK retries each request natively (429, 5xx, request timeouts, and network failures, honoring
`Retry-After`), and NPipeline adds no retry layer on top. `Retry` configures the SDK's `RetryOptions` on both the
Data Lake and Blob clients the provider creates. The type is `AzureRetryOptions`, in the `NPipeline.StorageProviders.Azure` namespace. It was `AdlsGen2RetryOptions` in earlier releases.

| Property | Default | Description |
|----------|---------|-------------|
| `Mode` | `RetryMode.Exponential` | Delay growth (`Exponential` or `Fixed`) |
| `MaxRetries` | `5` | Retries after the first attempt; `0` disables retries |
| `Delay` | `800 ms` | First retry delay (exponential base) |
| `MaxDelay` | `8 s` | Longest delay between retries |
| `NetworkTimeout` | `100 s` | Timeout for each network operation |

```csharp
services.AddAdlsGen2StorageProvider(options =>
{
    options.Retry = new AzureRetryOptions
    {
        MaxRetries = 3,
        NetworkTimeout = TimeSpan.FromSeconds(30)
    };
});
```

## Dependency Injection

```csharp
services.AddAdlsGen2StorageProvider();

services.AddAdlsGen2StorageProvider(options =>
{
    options.UseDefaultCredentialChain = true;
});
```

Registers: `IStorageProvider`

## Features

- **Streaming uploads** - the provider uploads blocks while you write. It doesn't buffer the file in a local temporary file. Writes use the Blob API, which works on every account and with the emulator, and produce block blobs.
- **Atomic rename/move** - on an account with a hierarchical namespace, `MoveAsync` uses the Data Lake rename API only
- **Hierarchical listing** - on an account with a hierarchical namespace, `ListAsync` uses the Data Lake path API and returns real directories
- **Idempotent delete** - `DeleteAsync` deletes a file and treats 404 as success
- **Endpoint-keyed client caches** - separate LRU caches for Blob and Data Lake clients, one client per endpoint. The cache never uses a credential as a key.
- **Metadata** - `Size`, `LastModified`, `ContentType`, `ETag`

### Hierarchical namespace detection

Moves and listing depend on whether the account has a hierarchical namespace (HNS). The provider asks the account once for each endpoint, with `GetAccountInfo`, and remembers the answer. If the account information isn't available, for example because the token lacks permission, the provider assumes the account has a hierarchical namespace.

| Operation | With HNS | Without HNS (a plain blob account, or Azurite) |
|-----------|----------|------------------------------------------------|
| `MoveAsync` | Data Lake rename. A rejected rename throws and never falls back to copy. | Blob copy, then delete the source. Not atomic. |
| `ListAsync`, non-recursive | `GetPaths`. Directories, including empty ones, appear as directory items. | Blob listing by prefix. A directory exists only as a name prefix. |
| `ListAsync`, recursive | `GetPaths`. Files only. | Blob listing by prefix. Files only. |

Previously, a failed rename silently fell back to copy and delete, which could leave both the source and the destination in place. Now a rename that the service rejects throws the translated exception.

## URI Parameters

A URI selects an endpoint and a path. It doesn't carry credentials.

| Parameter | Description |
|-----------|-------------|
| `accountName` | Storage account name (overrides `AccountName`) |
| `serviceUrl` | Custom service URL (overrides `ServiceUrl`) |
| `contentType` | MIME type set on write |

> **Security:** The `connectionString`, `sasToken`, and `accountKey` URI parameters throw `ArgumentException`. Set `DefaultConnectionString`, `DefaultSasToken`, or `DefaultAccountKey` in the options instead.

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
await using var stream = await provider.OpenWriteAsync(uri, cancellationToken: ct);
await using (var writer = new StreamWriter(stream, leaveOpen: true))
{
    await writer.WriteLineAsync("id,name,value");
}

// The file appears only after CommitAsync. Disposing the stream without it discards the data.
await stream.CommitAsync(ct);
```

Call `CommitAsync` once, after the last write. The provider uploads blocks while you write and doesn't use a local temporary file. A block fills, and its upload starts while you keep writing; when `MaxConcurrency` blocks are in flight, `WriteAsync` waits. `CommitAsync` uploads the last block and then commits the block list, and only then does the file appear. If the whole file fits in one block, `CommitAsync` uploads it in a single request. Disposing the stream without committing leaves an existing file as it was; the blocks you staged stay uncommitted, and Azure discards them after a week. A failed block upload surfaces from the next `WriteAsync` or from `CommitAsync`.

By default, the provider doesn't create the filesystem, so a write to a missing filesystem fails with `FileNotFoundException` when you call `CommitAsync`. Set `CreateContainerIfMissing` to create each filesystem on first use, once for each endpoint. Creating a filesystem needs permissions that a least-privilege token usually lacks.

The provider declares `ConditionalWrite`. Pass `new StorageWriteOptions { Overwrite = false }` to fail the commit if the file exists, or `new StorageWriteOptions { IfMatch = etag }` (an ETag from `GetMetadataAsync`) to commit only if the file is unchanged. A refused condition throws `StoragePreconditionFailedException` from `CommitAsync`. Set `StorageWriteOptions.ContentType` to set the content type.

### Listing

```csharp
var dirUri = StorageUri.Parse("adls://my-filesystem/data/");

// Non-recursive (immediate children only). Directories appear as items with IsDirectory = true.
await foreach (var item in provider.ListAsync(dirUri, recursive: false))
{
    var type = item.IsDirectory ? "[dir]" : $"{item.Size,12} bytes";
    Console.WriteLine($"  {type}  {item.Uri}");
}

// Recursive (files only)
await foreach (var item in provider.ListAsync(dirUri, recursive: true))
    Console.WriteLine(item.Uri);
```

Listing makes no request to check that the filesystem exists. Listing a filesystem or directory that doesn't exist yields no items. Names are case-sensitive, so `Data/` and `data/` are two directories.

### Metadata

```csharp
var metadata = await provider.GetMetadataAsync(uri);
if (metadata is not null)
    Console.WriteLine($"Size: {metadata.Size}, ContentType: {metadata.ContentType}, IsDirectory: {metadata.IsDirectory}");
```

### Deleting (Idempotent)

`DeleteAsync` deletes a file. It treats a path that doesn't exist as success.

```csharp
await provider.DeleteAsync(uri);   // succeeds even if path doesn't exist
```

### Moving / Renaming

On an account with a hierarchical namespace, ADLS Gen2's O(1) server-side rename is the primary differentiator over Azure Blob. Source and destination can be in different filesystems of the same account:

```csharp
var src = StorageUri.Parse("adls://my-filesystem/staging/records.csv");
var dest = StorageUri.Parse("adls://my-filesystem/processed/records.csv");
await provider.MoveAsync(src, dest);
```

> Cross-account moves throw `NotSupportedException`. The provider compares the endpoints (account name and service URL) of the source and destination. Both must be in the same storage account.

## Error Handling

| HTTP Status / Error | .NET Exception |
|---------------------|----------------|
| `AuthenticationFailed`, `AuthorizationFailed`, 401, 403 | `UnauthorizedAccessException` |
| `FilesystemNotFound`, `PathNotFound`, 404 | `FileNotFoundException` |
| `InvalidResourceName`, 400 | `ArgumentException` |
| `PathAlreadyExists`, `BlobAlreadyExists` (409), 412 | `StoragePreconditionFailedException` |
| 429 / 5xx (transient) | `IOException` (preserves retryable context) |

## Provider Capabilities

`provider.Capabilities` is `Read | Write | List | Delete | Move | Hierarchy | ConditionalWrite`. The provider doesn't declare `AtomicMove`, because whether a move is a single atomic rename depends on the account.

## Azurite (Local Development)

Azurite has no hierarchical namespace, so the provider moves by copy and delete and lists by blob prefix. Use the full connection string, which carries the Blob endpoint. Don't also set `ServiceUrl`, because a connection string and a service URL can't be combined.

```csharp
services.AddAdlsGen2StorageProvider(options =>
{
    options.DefaultConnectionString =
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;" +
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
        "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";
    options.CreateContainerIfMissing = true;
});
```

```bash
docker run -p 10000:10000 \
    mcr.microsoft.com/azure-storage/azurite \
    azurite --blobHost 0.0.0.0 --skipApiVersionCheck --inMemoryPersistence
```

> Azurite has partial ADLS Gen2 fidelity. It has no hierarchical namespace, so the rename and `GetPaths` code paths don't run against it. Validate against a real ADLS Gen2 account for production.

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `InvalidOperationException: Account name must be provided` | No account name or service URL resolved | Set `AccountName` (or an `accountName` URI parameter), `ServiceUrl`, or `DefaultConnectionString` |
| `ArgumentException` naming a URI parameter | The URI carries `connectionString`, `sasToken`, or `accountKey` | Set `DefaultConnectionString`, `DefaultSasToken`, or `DefaultAccountKey` in the options |
| `FileNotFoundException` on write | Filesystem doesn't exist | Create the filesystem, or set `CreateContainerIfMissing` to `true` |
| `UnauthorizedAccessException` | Missing permissions | Assign `Storage Blob Data Contributor` (or equivalent ADLS Gen2 role) |

## Next Steps

- [Azure Blob Provider](azure-blob.md) - flat namespace alternative
- [Data Lake Connector](../connectors/datalake.md) - partitioned tables on ADLS
- [Storage Providers Overview](index.md) - choosing between providers

---
title: Azure Data Lake Storage Gen2 Provider
description: Read from and write to Azure Data Lake Storage Gen2 using the ADLS Gen2 storage provider.
order: 4
---

# Azure Data Lake Storage Gen2 Provider


The ADLS Gen2 provider enables NPipeline applications to read, write, list, move, and delete data in Azure Data Lake Storage Gen2 using the `adls://` URI scheme. Unlike the Azure Blob Storage provider, this provider targets the `Azure.Storage.Files.DataLake` SDK and exposes ADLS Gen2's true hierarchical namespace and O(1) atomic rename operations.

### When to Use This Provider vs Azure Blob Storage

| Concern | Azure Blob (`azure://`) | ADLS Gen2 (`adls://`) |
|---|---|---|
| SDK package | `Azure.Storage.Blobs` | `Azure.Storage.Files.DataLake` |
| Directory model | Flat (virtual `/` delimiter) | True POSIX-like directory tree |
| Atomic move/rename | Not natively supported | `RenameAsync` — O(1) atomic |
| Per-path ACLs | RBAC / container-level only | POSIX-style per-file and per-directory |
| `SupportsHierarchy` | `false` | `true` |

Use this provider when you require **true hierarchical namespace**, **atomic rename/move**, or **POSIX ACLs** (ACL support is planned for a future release).

## Dependencies

```xml
<PackageReference Include="NPipeline.StorageProviders.Adls" Version="*" />
```

Transitive dependencies pulled in automatically:

| Package | Purpose |
|---|---|
| `Azure.Storage.Files.DataLake` | ADLS Gen2 SDK |
| `Azure.Identity` | `DefaultAzureCredential` and token credential support |
| `NPipeline.StorageProviders` | Core storage abstractions |

## URI Format

```
adls://<filesystem>/<path/to/file.ext>[?param=value&...]
```

| URI component | Maps to |
|---|---|
| `Host` | Data Lake **filesystem** name (equivalent of a Blob container) |
| `Path` | File or directory path within the filesystem |

### Naming constraints

- **Filesystem name**: 3–63 characters; lowercase letters, digits, and hyphens; no leading or trailing hyphen.
- **Path**: 1–2,048 characters; no backslash (`\`); no `?`.

### Supported query parameters

| Parameter | Description |
|---|---|
| `accountName` | Storage account name (overrides default) |
| `accountKey` | Shared-key credential (base64-encoded) |
| `sasToken` | SAS token |
| `connectionString` | Full connection string |
| `contentType` | MIME type set on write |

## Authentication

Credential resolution follows this priority order (first match wins):

1. Per-URI `connectionString` query parameter
2. Per-URI `accountKey` query parameter → `StorageSharedKeyCredential`
3. Per-URI `sasToken` query parameter → `AzureSasCredential`
4. `AdlsGen2StorageProviderOptions.DefaultConnectionString`
5. `AdlsGen2StorageProviderOptions.DefaultCredential`
6. `AdlsGen2StorageProviderOptions.DefaultCredentialChain` (lazy `DefaultAzureCredential`) when `UseDefaultCredentialChain = true`

## Registration

### With a configuration delegate

```csharp
services.AddAdlsGen2StorageProvider(options =>
{
    // Managed identity / DefaultAzureCredential (recommended for production)
    options.ServiceUrl = new Uri("https://<account>.dfs.core.windows.net/");
    options.UseDefaultCredentialChain = true;

    // OR explicit connection string
    // options.DefaultConnectionString = "<connection-string>";
});
```

### With a pre-built options instance

```csharp
var options = new AdlsGen2StorageProviderOptions
{
    ServiceUrl = new Uri("https://<account>.dfs.core.windows.net/"),
    UseDefaultCredentialChain = true,
    UploadThresholdBytes = 128 * 1024 * 1024,
};

services.AddAdlsGen2StorageProvider(options);
```

### Azurite (local development)

```csharp
services.AddAdlsGen2StorageProvider(options =>
{
    options.DefaultConnectionString = "UseDevelopmentStorage=true";
    options.ServiceUrl = new Uri("http://127.0.0.1:10000/devstoreaccount1/");
});
```

```bash
# Start Azurite with ADLS Gen2 support
docker run -p 10000:10000 \
    mcr.microsoft.com/azure-storage/azurite \
    azurite --blobHost 0.0.0.0 --skipApiVersionCheck --inMemoryPersistence
```

## Configuration Reference

All properties are on `AdlsGen2StorageProviderOptions`:

| Property | Type | Default | Description |
|---|---|---|---|
| `DefaultCredential` | `TokenCredential?` | `null` | Explicit token credential |
| `DefaultConnectionString` | `string?` | `null` | Connection string (takes priority over token credentials) |
| `UseDefaultCredentialChain` | `bool` | `true` | Fall back to `DefaultAzureCredential` when no other credential is configured |
| `ServiceUrl` | `Uri?` | `null` | Custom DFS service URL (e.g., Azurite or sovereign clouds) |
| `ServiceVersion` | `DataLakeClientOptions.ServiceVersion?` | `null` | REST API version override |
| `UploadThresholdBytes` | `long` | `67,108,864` (64 MB) | Files at or above this size use chunked transfer options |
| `UploadMaximumConcurrency` | `int?` | `null` | Max parallel upload connections for large files |
| `UploadMaximumTransferSizeBytes` | `int?` | `null` | Max bytes per upload chunk |
| `ClientCacheSizeLimit` | `int` | `100` | Max cached `DataLakeServiceClient` instances |

## Usage Examples

### Reading a file

```csharp
var uri = StorageUri.Parse("adls://my-filesystem/data/records.csv");
await using var stream = await provider.OpenReadAsync(uri);
```

### Writing a file

```csharp
var uri = StorageUri.Parse("adls://my-filesystem/data/output.csv?contentType=text/csv");
await using var stream = await provider.OpenWriteAsync(uri);
await csvWriter.WriteToAsync(stream);
```

Data is buffered to a local temporary file and uploaded atomically when the stream is disposed.

### Checking existence

```csharp
var exists = await provider.ExistsAsync(uri);
```

### Listing (non-recursive)

```csharp
var dirUri = StorageUri.Parse("adls://my-filesystem/data/");
await foreach (var item in provider.ListAsync(dirUri, recursive: false))
{
    var type = item.IsDirectory ? "[dir]" : $"{item.Size,12} bytes";
    Console.WriteLine($"  {type}  {item.Uri}");
}
```

### Listing (recursive)

```csharp
await foreach (var item in provider.ListAsync(dirUri, recursive: true))
    Console.WriteLine(item.Uri);
```

### Retrieving metadata

```csharp
var metadata = await provider.GetMetadataAsync(uri);
if (metadata is not null)
{
    Console.WriteLine($"Size: {metadata.Size}");
    Console.WriteLine($"ContentType: {metadata.ContentType}");
    Console.WriteLine($"IsDirectory: {metadata.IsDirectory}");
}
```

### Deleting a file (idempotent)

```csharp
if (provider is IDeletableStorageProvider del)
    await del.DeleteAsync(uri);   // silently succeeds even if the path does not exist
```

### Moving / renaming a file (atomic)

ADLS Gen2's O(1) server-side rename is the primary differentiator over Azure Blob Storage:

```csharp
if (provider is IMoveableStorageProvider mov)
{
    var src  = StorageUri.Parse("adls://my-filesystem/staging/records.csv");
    var dest = StorageUri.Parse("adls://my-filesystem/processed/records.csv");
    await mov.MoveAsync(src, dest);
}
```

> **Note:** Cross-account moves are not supported in v1. Both source and destination must be within the same storage account. A `NotSupportedException` is thrown otherwise.

## Exception Handling

`RequestFailedException` errors from the Azure SDK are translated to standard .NET exceptions:

| HTTP status / error code | Thrown exception |
|---|---|
| `AuthenticationFailed`, `AuthorizationFailed`, 401, 403 | `UnauthorizedAccessException` |
| `FilesystemNotFound`, `PathNotFound`, 404 | `FileNotFoundException` |
| `InvalidResourceName`, 400 | `ArgumentException` |
| `PathAlreadyExists`, 409 | `IOException` |
| 429 / 5xx (transient) | `IOException` (preserves retryable context) |

`AdlsStorageException` (inherits `ConnectorException`) carries `Filesystem` and `Path` properties for structured diagnostics.

## Provider Capabilities

This provider reports the following capabilities via `IStorageProviderMetadataProvider.GetMetadata()`:

```csharp
new StorageProviderMetadata
{
    Name              = "Azure Data Lake Storage Gen2",
    SupportedSchemes  = ["adls"],
    SupportsRead      = true,
    SupportsWrite     = true,
    SupportsListing   = true,
    SupportsMetadata  = true,
    SupportsHierarchy = true,   // ← key differentiator from Azure Blob
    Capabilities = {
        ["supportsAtomicMove"]              = true,
        ["supportsNativeDelete"]            = true,
        ["supportsHierarchicalListing"]     = true,
        ["supportsServiceUrl"]              = true,
        ["supportsConnectionString"]        = true,
        ["supportsSasToken"]                = true,
        ["supportsAccountKey"]              = true,
        ["supportsDefaultCredentialChain"]  = true,
    }
}
```

## Troubleshooting

### `InvalidOperationException: Account name must be provided`

No credential resolved to an account. Provide one of: `connectionString`, `accountName + accountKey`, `accountName + sasToken`, or `DefaultConnectionString`.

### `FileNotFoundException` on write

Ensure the filesystem exists or that the provider has permissions to create it. `UploadAsync` calls `CreateIfNotExistsAsync` on the target filesystem automatically.

### `UnauthorizedAccessException`

Verify the identity has at minimum `Storage Blob Data Contributor` (or equivalent ADLS Gen2 role) on the storage account. For Azurite, use the default dev credentials directly.

### Azurite partial ADLS Gen2 fidelity

Azurite supports the ADLS Gen2 Data Lake API at partial fidelity. ACL operations and some advanced HNS behaviors may behave differently from the real service. Run nightly tests against a real ADLS Gen2 account for full validation.

## ACL Support (future roadmap)

POSIX ACLs (`SetAccessControlListAsync`, `GetAccessControlAsync`) are **not in scope for v1**. They will be exposed via a dedicated `IAdlsAclProvider` interface extension in a future release.

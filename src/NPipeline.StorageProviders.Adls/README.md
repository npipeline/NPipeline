# NPipeline.StorageProviders.Adls

Azure Data Lake Storage Gen2 (ADLS Gen2) storage provider for NPipeline.

## Overview

This library provides a storage provider implementation for Azure Data Lake Storage Gen2, enabling NPipeline to read, write, list, move, and delete files in ADLS Gen2 accounts using the `adls://` URI scheme.

## Key Features

- **Full `IStorageProvider` compliance** - Read, write, exists, list, and metadata operations
- **`IDeletableStorageProvider`** - Native path delete with idempotent behavior
- **`IMoveableStorageProvider`** - Native atomic rename/move via `DataLakePathClient.RenameAsync`
- **`IStorageProviderMetadataProvider`** - Provider capability advertisement
- **True hierarchical namespace** - `SupportsHierarchy = true` (unlike Azure Blob Storage)
- **Production-hardened** - Client caching, retries, cancellation, structured exception translation
- **Testable** - Unit tests with fakes, integration tests against Azurite

## Installation

```bash
dotnet add package NPipeline.StorageProviders.Adls
```

## URI Scheme

```
adls://<filesystem>/<path/to/file.ext>[?param=value&...]
```

| URI component | Maps to |
|---|---|
| `Host` | Data Lake **filesystem** name (equivalent of container) |
| `Path` | File or directory path within the filesystem |

### Supported Query Parameters

| Parameter | Description |
|---|---|
| `accountName` | Storage account name (overrides options default) |
| `accountKey` | Shared-key credential (base64) |
| `sasToken` | SAS token |
| `connectionString` | Full connection string |
| `contentType` | MIME type hint applied on write |

## Authentication

The provider supports multiple authentication methods with the following priority:

1. Per-URI `connectionString` query parameter
2. Per-URI `accountKey` query parameter → `StorageSharedKeyCredential`
3. Per-URI `sasToken` query parameter → `AzureSasCredential`
4. `Options.DefaultConnectionString`
5. `Options.DefaultCredential`
6. `Options.DefaultCredentialChain` (lazy `DefaultAzureCredential`) when `UseDefaultCredentialChain = true`

## Usage

### Basic Registration

```csharp
using NPipeline.StorageProviders.Adls;

// In your DI setup
services.AddAdlsGen2StorageProvider(options =>
{
    options.DefaultConnectionString = "<your-connection-string>";
    // OR use managed identity / DefaultAzureCredential
    options.UseDefaultCredentialChain = true;
});
```

### With Service URL (for Azurite or custom endpoints)

```csharp
services.AddAdlsGen2StorageProvider(options =>
{
    options.ServiceUrl = new Uri("http://127.0.0.1:10000/devstoreaccount1/");
    options.DefaultConnectionString = "UseDevelopmentStorage=true";
});
```

### Reading a File

```csharp
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

public class MyService
{
    private readonly IStorageProvider _storageProvider;

    public MyService(IStorageProvider storageProvider)
    {
        _storageProvider = storageProvider;
    }

    public async Task<Stream> ReadFileAsync(string filesystem, string path)
    {
        var uri = StorageUri.Parse($"adls://{filesystem}/{path}");
        return await _storageProvider.OpenReadAsync(uri);
    }
}
```

### Writing a File

```csharp
public async Task WriteFileAsync(string filesystem, string path, Stream content)
{
    var uri = StorageUri.Parse($"adls://{filesystem}/{path}");
    await using var writeStream = await _storageProvider.OpenWriteAsync(uri);
    await content.CopyToAsync(writeStream);
}
```

### Checking if a File Exists

```csharp
public async Task<bool> FileExistsAsync(string filesystem, string path)
{
    var uri = StorageUri.Parse($"adls://{filesystem}/{path}");
    return await _storageProvider.ExistsAsync(uri);
}
```

### Listing Files

```csharp
public async IAsyncEnumerable<StorageItem> ListFilesAsync(string filesystem, string directory)
{
    var uri = StorageUri.Parse($"adls://{filesystem}/{directory}");
    await foreach (var item in _storageProvider.ListAsync(uri, recursive: false))
    {
        yield return item;
    }
}
```

### Getting File Metadata

```csharp
public async Task<StorageMetadata?> GetFileMetadataAsync(string filesystem, string path)
{
    var uri = StorageUri.Parse($"adls://{filesystem}/{path}");
    return await _storageProvider.GetMetadataAsync(uri);
}
```

### Deleting a File

```csharp
public async Task DeleteFileAsync(string filesystem, string path)
{
    if (_storageProvider is IDeletableStorageProvider deletableProvider)
    {
        var uri = StorageUri.Parse($"adls://{filesystem}/{path}");
        await deletableProvider.DeleteAsync(uri);
    }
}
```

### Moving a File (Atomic Rename)

```csharp
public async Task MoveFileAsync(string filesystem, string sourcePath, string destPath)
{
    if (_storageProvider is IMoveableStorageProvider moveableProvider)
    {
        var sourceUri = StorageUri.Parse($"adls://{filesystem}/{sourcePath}");
        var destUri = StorageUri.Parse($"adls://{filesystem}/{destPath}");
        await moveableProvider.MoveAsync(sourceUri, destUri);
    }
}
```

## Configuration Options

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `DefaultCredential` | `TokenCredential?` | `null` | Custom token credential |
| `DefaultConnectionString` | `string?` | `null` | Storage account connection string |
| `UseDefaultCredentialChain` | `bool` | `true` | Use `DefaultAzureCredential` when no other credential is provided |
| `ServiceUrl` | `Uri?` | `null` | Custom service URL (e.g., for Azurite) |
| `ServiceVersion` | `DataLakeClientOptions.ServiceVersion?` | `null` | REST API version |
| `UploadThresholdBytes` | `long` | `67108864` (64 MB) | Threshold for chunked uploads |
| `UploadMaximumConcurrency` | `int?` | `null` | Max concurrent upload operations |
| `UploadMaximumTransferSizeBytes` | `int?` | `null` | Max bytes per transfer chunk |
| `ClientCacheSizeLimit` | `int` | `100` | Max cached service clients |

## ADLS Gen2 vs Azure Blob Storage

| Concern | Azure Blob | ADLS Gen2 |
|---|---|---|
| SDK package | `Azure.Storage.Blobs` | `Azure.Storage.Files.DataLake` |
| Hierarchy | Flat (virtual `/` delimiter) | True POSIX-like directory tree |
| Atomic rename / move | Not supported natively | `RenameAsync` — O(1) atomic |
| Write semantics | Block upload | Append + flush (or block upload) |
| ACLs | RBAC/container-level only | Per-file and per-directory POSIX ACLs |
| URI scheme | `azure://` | `adls://` |
| `SupportsHierarchy` | `false` | `true` |

## Exception Handling

The provider translates Azure `RequestFailedException` errors to standard .NET exceptions:

| HTTP status / error code | Thrown exception |
|---|---|
| `AuthenticationFailed`, `AuthorizationFailed`, 401, 403 | `UnauthorizedAccessException` |
| `FilesystemNotFound`, `PathNotFound`, 404 | `FileNotFoundException` |
| `InvalidResourceName`, 400 | `ArgumentException` |
| `PathAlreadyExists`, 409 | `IOException` |
| 429 / 5xx | `IOException` (retryable) |

## Development & Testing

For local development, you can use [Azurite](https://docs.microsoft.com/azure/storage/common/storage-use-azurite) with ADLS Gen2 support:

```bash
# Run Azurite with ADLS Gen2 support
docker run -p 10000:10000 -p 10001:10001 -p 10002:10002 \
    mcr.microsoft.com/azure-storage/azurite \
    azurite --blobHost 0.0.0.0 --queueHost 0.0.0.0 --tableHost 0.0.0.0 --location data --debug data/debug.log
```

## License

This project is licensed under the terms specified in the repository's LICENSE file.

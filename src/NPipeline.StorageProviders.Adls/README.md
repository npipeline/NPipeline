# NPipeline.StorageProviders.Adls

Azure Data Lake Storage Gen2 (ADLS Gen2) storage provider for NPipeline.

## Overview

This library provides a storage provider implementation for Azure Data Lake Storage Gen2, enabling NPipeline to read, write, list, move, and delete files in
ADLS Gen2 accounts using the `adls://` URI scheme.

## Key Features

- **One `IStorageProvider`** - Derives from `StorageProvider`; read, write, exists, list, and metadata operations
- **Streaming writes** - Uploads block blobs while you write, with no local temporary file. An object that fits in one block uploads in one request
- **Delete** - Deletes a file, idempotently (`StorageCapabilities.Delete`)
- **Move** - With a hierarchical namespace, the Data Lake rename (`DataLakePathClient.RenameAsync`) only: a rejected rename throws and never degrades to copy-and-delete. Without one (a plain blob account, or Azurite), a blob copy and a delete (`StorageCapabilities.Move`). `AtomicMove` isn't declared, because it depends on the account
- **Hierarchical listing** - With a hierarchical namespace, `ListAsync` uses the Data Lake path API and returns real directories. Without one, it lists blobs by prefix
- **Built on the Azure package** - Shares `AzureAccountOptions`, `AzureRetryOptions`, the client builder, and `AzureBlobWriteStream` with `NPipeline.StorageProviders.Azure`, which this package references
- **True hierarchical namespace** - Declares `StorageCapabilities.Hierarchy` (unlike Azure Blob Storage)
- **Production-hardened** - Endpoint-keyed client caching, retries, cancellation, structured exception translation
- **Testable** - Unit tests with fakes, integration tests against Azurite

## Installation

```bash
dotnet add package NPipeline.StorageProviders.Adls
```

## URI Scheme

```
adls://<filesystem>/<path/to/file.ext>[?param=value&...]
```

| URI component | Maps to                                                 |
|---------------|---------------------------------------------------------|
| `Host`        | Data Lake **filesystem** name (equivalent of container) |
| `Path`        | File or directory path within the filesystem            |

### Supported Query Parameters

| Parameter     | Description                                           |
|---------------|-------------------------------------------------------|
| `accountName` | Storage account name (overrides `AccountName`)        |
| `serviceUrl`  | Custom service URL (overrides `ServiceUrl`)           |
| `contentType` | MIME type hint applied on write                       |

A URI never carries credentials. The `connectionString`, `sasToken`, and `accountKey` parameters throw `ArgumentException` that names the option to set instead.

## Authentication

You set credentials in the options. The provider uses the first of these that you set:

1. `Options.DefaultConnectionString`. A connection string names its own endpoint, so combining it with `ServiceUrl` throws `ArgumentException`
2. `Options.DefaultSasToken` → `AzureSasCredential`
3. `Options.DefaultAccountKey`, with `AccountName` or an `accountName` URI parameter → `StorageSharedKeyCredential`
4. `Options.DefaultCredential`
5. The default credential chain (lazy `DefaultAzureCredential`) when `UseDefaultCredentialChain = true`

If none is available and `AllowAnonymousAccess` is `false`, creating a client throws `InvalidOperationException`.

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

### Azurite or custom endpoints

A connection string names its own endpoint, so don't combine it with `ServiceUrl`. To use `ServiceUrl` instead, set `AccountName` and `DefaultAccountKey` (or another credential).

```csharp
services.AddAdlsGen2StorageProvider(options =>
{
    options.DefaultConnectionString =
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=<azurite-key>;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";
    options.CreateContainerIfMissing = true;
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

The provider uploads blocks while you write. By default it doesn't create the filesystem; set `CreateContainerIfMissing` to create it on first use.

```csharp
public async Task WriteFileAsync(string filesystem, string path, Stream content)
{
    var uri = StorageUri.Parse($"adls://{filesystem}/{path}");
    await using var writeStream = await _storageProvider.OpenWriteAsync(uri);
    await content.CopyToAsync(writeStream);
    await writeStream.CommitAsync(); // commits the uploaded blocks; disposing without a commit discards them
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
    var uri = StorageUri.Parse($"adls://{filesystem}/{path}");
    await _storageProvider.DeleteAsync(uri);
}
```

### Moving a File (Rename)

```csharp
public async Task MoveFileAsync(string filesystem, string sourcePath, string destPath)
{
    var sourceUri = StorageUri.Parse($"adls://{filesystem}/{sourcePath}");
    var destUri = StorageUri.Parse($"adls://{filesystem}/{destPath}");
    await _storageProvider.MoveAsync(sourceUri, destUri);
}
```

## Configuration Options

| Property                    | Type                                    | Default     | Description                                                                           |
|-----------------------------|-----------------------------------------|-------------|---------------------------------------------------------------------------------------|
| `AccountName`               | `string?`                               | `null`      | Default account name; the `accountName` URI parameter overrides it                    |
| `DefaultConnectionString`   | `string?`                               | `null`      | Storage account connection string                                                     |
| `DefaultSasToken`           | `string?`                               | `null`      | Shared access signature token                                                         |
| `DefaultAccountKey`         | `string?`                               | `null`      | Storage account key; needs an account name                                            |
| `DefaultCredential`         | `TokenCredential?`                      | `null`      | Custom token credential                                                               |
| `UseDefaultCredentialChain` | `bool`                                  | `true`      | Use `DefaultAzureCredential` when no other credential is provided                     |
| `AllowAnonymousAccess`      | `bool`                                  | `false`     | Connect without credentials when none are configured                                  |
| `ServiceUrl`                | `Uri?`                                  | `null`      | Custom service URL (e.g., for Azurite)                                                |
| `ServiceVersion`            | `DataLakeClientOptions.ServiceVersion?` | `null`      | REST API version                                                                      |
| `PartSizeBytes`             | `int`                                   | `8 MiB`     | Size of each upload block; a file that fits in one block uploads in one request       |
| `MaxConcurrency`            | `int`                                   | `4`         | Most blocks of one file that upload at the same time                                  |
| `CreateContainerIfMissing`  | `bool`                                  | `false`     | Create the filesystem on the first write to it                                        |
| `ClientCacheSizeLimit`      | `int`                                   | `100`       | Most clients kept in each cache, one per endpoint                                     |
| `Retry`                     | `AzureRetryOptions`                     | see below   | Azure SDK retry settings                                                              |

### Resilience

The Azure SDK retries each request natively (429, 5xx, request timeouts, and network failures, honoring
`Retry-After`), and NPipeline adds no retry layer on top. `Retry` configures the SDK's `RetryOptions` on both the
Data Lake and Blob clients the provider creates. The type is `AzureRetryOptions` (namespace `NPipeline.StorageProviders.Azure`), which was `AdlsGen2RetryOptions` in earlier releases:

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

## ADLS Gen2 vs Azure Blob Storage

| Concern              | Azure Blob                   | ADLS Gen2                             |
|----------------------|------------------------------|---------------------------------------|
| SDK package          | `Azure.Storage.Blobs`        | `Azure.Storage.Files.DataLake`        |
| Hierarchy            | Flat (virtual `/` delimiter) | True POSIX-like directory tree        |
| Atomic rename / move | Not supported natively       | `RenameAsync` - O(1) atomic (with HNS) |
| Write semantics      | Streaming block upload       | Streaming block upload (Blob API)     |
| ACLs                 | RBAC/container-level only    | Per-file and per-directory POSIX ACLs |
| URI scheme           | `azure://`                   | `adls://`                             |
| `Hierarchy` capability | not declared               | declared                              |

## Exception Handling

The provider translates Azure `RequestFailedException` errors to standard .NET exceptions. The HTTP status decides first and the error code only refines it when the status is missing or generic. The SDK exception is the inner exception.

| HTTP status / error code                                | Thrown exception              |
|---------------------------------------------------------|-------------------------------|
| `AuthenticationFailed`, `AuthorizationFailed`, 401, 403 | `UnauthorizedAccessException` |
| `FilesystemNotFound`, `PathNotFound`, 404               | `FileNotFoundException`       |
| `InvalidResourceName`, 400                              | `ArgumentException`           |
| `PathAlreadyExists`, `BlobAlreadyExists` (409), 412     | `StoragePreconditionFailedException` |
| 429 / 5xx                                               | `IOException` (retryable)     |

Listing: the directory URI ends with `/`; a non-recursive listing yields files and directory entries (`IsDirectory = true`), and a recursive listing yields files only. With a hierarchical namespace, directory entries are real directories with a `LastModified` time, and empty directories appear. Without one, they are name prefixes. Names are case-sensitive. Listing makes no existence check, and a missing filesystem or directory yields no items.

## Development & Testing

Azurite has no hierarchical namespace, so against it the provider lists blobs by prefix and moves by copy and delete. The rename and `GetPaths` code paths are covered by unit tests with fakes. Validate against a real ADLS Gen2 account before production.

```bash
docker run -p 10000:10000 \
    mcr.microsoft.com/azure-storage/azurite \
    azurite --blobHost 0.0.0.0 --skipApiVersionCheck --inMemoryPersistence
```

## License

This package is licensed under the [Business Source License 1.1](LICENSE.txt).

**Free for non-production use.** Production use is free for organizations with 4 or fewer developers and annual revenue of $5M AUD or less. Larger organizations require a [commercial license](https://npipeline.com). This license automatically converts to MIT two years after each release.

---
title: "Storage Providers"
description: "Abstract file storage so connectors work with local files, S3, Azure Blob, GCS, and SFTP."
order: 5
---

# Storage Providers

> **Prerequisites:** [Connectors Overview](../connectors/index.md)

A [storage provider](../reference/glossary.md#storage-provider) implements `IStorageProvider` - a unified interface for reading and writing files regardless of where they're stored. File-based [connectors](../connectors/index.md) (CSV, JSON, Parquet, Excel) use storage providers so the same pipeline code works with local files, cloud storage, and SFTP.

## The IStorageProvider Interface

```csharp
public interface IStorageProvider
{
    string Name { get; }
    IReadOnlyList<StorageScheme> Schemes { get; }
    StorageCapabilities Capabilities { get; }

    Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken ct = default);
    Task<StorageWriteStream> OpenWriteAsync(StorageUri uri, StorageWriteOptions? options = null, CancellationToken ct = default);
    Task<StorageMetadata?> GetMetadataAsync(StorageUri uri, CancellationToken ct = default);   // null: not found
    Task<bool> ExistsAsync(StorageUri uri, CancellationToken ct = default);
    IAsyncEnumerable<StorageItem> ListAsync(StorageUri directory, bool recursive = false, CancellationToken ct = default);
    Task DeleteAsync(StorageUri uri, CancellationToken ct = default);
    Task MoveAsync(StorageUri source, StorageUri destination, CancellationToken ct = default);
}
```

`Capabilities` is a `[Flags]` enum that says what a provider supports: `Read`, `Write`, `List`, `Delete`, `Move`, `AtomicMove` (the move is a single rename), `Hierarchy` (real directories rather than key prefixes) and `ConditionalWrite`. Calling an operation the provider doesn't declare throws `UnsupportedStorageCapabilityException`.

| Provider | Capabilities |
|----------|--------------|
| File system | Read, Write, List, Delete, Move, AtomicMove, Hierarchy |
| S3 (AWS) | Read, Write, List, Delete, Move (copy, then delete), ConditionalWrite |
| S3-compatible, GCS | Read, Write, List, Delete, Move (copy, then delete) |
| Azure Blob | Read, Write, List, Delete, Move (copy, then delete), ConditionalWrite |
| ADLS Gen2 | Read, Write, List, Delete, Move, Hierarchy, ConditionalWrite |
| SFTP | Read, Write, List, Delete, Move, AtomicMove, Hierarchy |

ADLS Gen2 doesn't declare `AtomicMove`, even though an account with a hierarchical namespace renames atomically: whether that's true depends on the account, and `Capabilities` can't vary by endpoint. An account without a hierarchical namespace, such as the Azurite emulator, moves by copy and delete instead. SFTP declares `AtomicMove` because the server does a POSIX rename, but a rename can't overwrite, so a move onto an existing destination deletes it first and is not atomic in that case.

### Writes commit explicitly

`OpenWriteAsync` returns a `StorageWriteStream`, a `Stream` with a `CommitAsync` method and an `ETag` property. What you write becomes visible at the target only when you call `await stream.CommitAsync()`, once, after the last write. If you dispose the stream without committing, the provider *abandons* the write: it discards everything you wrote, and the target is untouched (an existing object stays as it was). Disposing never uploads. Upload errors surface from `CommitAsync`, with the token you pass to it, not from `Dispose`.

```csharp
await using var stream = await provider.OpenWriteAsync(uri, cancellationToken: ct);
await using (var writer = new StreamWriter(stream, leaveOpen: true))
{
    await writer.WriteAsync(text);
}
await stream.CommitAsync(ct);
```

A wrapper that closes the stream when it's disposed, such as `StreamWriter` or `GZipStream`, must not close the provider stream before you commit. Pass `leaveOpen: true`, or flush the wrapper and commit first.

| Provider | Commit | Abandon |
|----------|--------|---------|
| File system, SFTP | Renames a hidden sibling temporary file (`.<name>.<guid>.tmp`) into place | Deletes the temporary file |
| S3 | Completes the multipart upload, or sends one `PutObject` for an object that fits in a part. Parts upload while you write | Aborts the multipart upload |
| Azure Blob, ADLS Gen2 | Commits the block list, or uploads one block blob. Blocks upload while you write | Abandons the staged blocks, which the service discards |
| GCS | Finishes the resumable upload, which runs while you write | Cancels the upload session |

Providers that declare `ConditionalWrite` (AWS S3, Azure Blob and ADLS Gen2) can also make a commit depend on the state of the target:

- `StorageWriteOptions { Overwrite = false }` fails the commit if the target exists.
- `StorageWriteOptions { IfMatch = etag }` commits only if the current ETag matches. Get the ETag from `GetMetadataAsync`.

When a condition isn't met, `CommitAsync` throws `NPipeline.StorageProviders.Exceptions.StoragePreconditionFailedException`, which derives from `IOException`. S3-compatible stores, GCS, SFTP and the file system don't declare `ConditionalWrite`. `StorageWriteOptions.ContentType` sets the content type where the store has one; the `contentType` URI parameter is the fallback.

### Contract

Every provider behaves the same way:

| Situation | Result |
|-----------|--------|
| Object not found (read, move source) | `FileNotFoundException`. `GetMetadataAsync` returns `null` and `ExistsAsync` returns `false`. |
| Authentication or permission failure | `UnauthorizedAccessException` |
| Invalid bucket, container, key or path | `ArgumentException` |
| Any other service or I/O failure | `IOException`, with the SDK exception as `InnerException` |
| `ListAsync` | `directory` is always treated as a directory, so `logs` never matches `logs-archive/`. `recursive: false` yields direct children, including directory entries. `recursive: true` yields every file below and no directory entries. Listed URIs keep the host, port and parameters of the URI you passed in. A missing directory yields nothing. |
| `DeleteAsync` on a missing object | Succeeds |
| `MoveAsync` | Overwrites the destination |
| `ExistsAsync` on a directory | `true` on providers that declare `Hierarchy`; `false` for a bare prefix on an object store |

`StorageItem.Size` and the `LastModified` values are `null` when the store doesn't report them, for example for a prefix.

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

var uri = StorageUri.Parse("s3://my-bucket/data/orders.csv");
var source = CsvConnector.Source<Order>(uri, o => o with { Provider = storage });
```

Switch storage without changing pipeline logic:

```csharp
// Local development
var storage = new FileSystemStorageProvider();
var uri = StorageUri.Parse("file:///data/orders.csv");

// Production (same connector, different storage)
var storage = new AwsS3StorageProvider(s3Options);
var uri = StorageUri.Parse("s3://prod-bucket/data/orders.csv");
```

## DI Registration

Each provider package includes `IServiceCollection` extensions that register the provider as an `IStorageProvider` singleton. Calling one twice has no effect:

```csharp
services.AddAwsS3StorageProvider(options => { /* configure */ });
services.AddAzureBlobStorageProvider(options => { /* configure */ });
services.AddGcsStorageProvider(options => { /* configure */ });

services.AddStorageResolver();   // also registers the file system provider
```

`AddStorageResolver()` registers an `IStorageResolver` built from every `IStorageProvider` in the container, including providers registered after it. `AddFileSystemStorageProvider()` registers the file system provider on its own.

## Storage Resolver

`StorageResolver` selects the provider that serves a URI's scheme. It's immutable: build it from the providers you want. Two providers that serve the same scheme throw `ArgumentException` at construction, so a wrong configuration fails at startup.

```csharp
var resolver = new StorageResolver(new IStorageProvider[]
{
    new FileSystemStorageProvider(),
    new AwsS3StorageProvider(s3Options),
    new AzureBlobStorageProvider(blobOptions)
});

var provider = resolver.Resolve(StorageUri.Parse("s3://bucket/file.csv"));   // throws StorageProviderNotFoundException if none
await using var stream = await provider.OpenReadAsync(uri);
```

`TryResolve` returns `false` instead of throwing, and `StorageResolver.Default` serves the file system only. To read from AWS and write to MinIO through one resolver, give the S3-compatible provider its own scheme (see [S3-Compatible](s3-compatible.md)), or pass the provider to the node with `Provider = ...`.

## Custom Provider

See [Implementing a Custom Provider](custom-provider.md) if you need to support a storage system that isn't covered.

## StorageUri

`StorageUri` is the address type used by all storage providers. It supports local files, cloud storage, and SFTP - with optional query parameters for per-request configuration:

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

Properties: `Scheme`, `Host`, `Port`, `UserName`, `Password`, `Path`, `Parameters`, `Name`, `Parent` and `IsDirectory`.

`StorageUri` is immutable and has value semantics: two URIs parsed from the same text are equal, and a URI works as a dictionary key. Build variants with `WithPath`, `WithParameter`, `WithoutParameter` and `Combine`:

```csharp
var uri = StorageUri.Parse("s3://my-bucket/data/orders.csv?region=us-west-2");

var other = uri.WithPath("/data/customers.csv");   // keeps the host and the region
```

The text is decoded once, when you parse it, and `Path` is the literal object key or file path:

- `s3://my-bucket/my%20file.csv` and `s3://my-bucket/my file.csv` are both the key `my file.csv`. To address a key that contains a literal `%`, write `%25`.
- `#`, `..` and `//` stay in the path as you wrote them. Write `?` in a key as `%3F`, because `?` starts the query string.
- Text that doesn't start with `scheme://` is a local file path, for example `C:\data\orders.csv` or `./orders.csv`.

`ToString()` percent-encodes the URI and replaces the password and the values of secret parameters, such as `secretKey` and `sasToken`, with `***`, so it's safe to log. `ToUnredactedString()` keeps them.

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
await using var writeStream = await provider.OpenWriteAsync(uri);
await writeStream.WriteAsync(bytes);
await writeStream.CommitAsync();   // without this, disposing discards the data

// Delete and move
await provider.DeleteAsync(uri);
await provider.MoveAsync(source, destination);

// Exists
bool exists = await provider.ExistsAsync(uri);

// List (recursive or non-recursive)
await foreach (var item in provider.ListAsync(prefix, recursive: true))
{
    Console.WriteLine($"{item.Uri} - {item.Size?.ToString() ?? "?"} bytes - {item.LastModified}");
}

// Metadata (null when the object doesn't exist)
var metadata = await provider.GetMetadataAsync(uri);
```

## Next Steps

- Pick a provider from the table above to see configuration details
- [Custom Provider](custom-provider.md) - implement your own storage provider
- [Connectors](../connectors/index.md) - use storage providers with file-based connectors

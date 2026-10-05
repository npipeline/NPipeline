---
title: "Custom Storage Provider"
description: "Derive from StorageProvider to integrate with unsupported storage systems."
order: 8
---

# Custom Storage Provider

> **Prerequisites:** [Storage Providers Overview](index.md)

If NPipeline doesn't have a built-in provider for your storage system, derive from `StorageProvider`. The base class validates arguments, observes cancellation and rejects operations your provider doesn't declare, so your code only handles the real work.

## Implementing a provider

Override `Name`, `Schemes` and `Capabilities`, then override the `...CoreAsync` method for each capability you declare:

```csharp
public sealed class FtpStorageProvider : StorageProvider
{
    private static readonly IReadOnlyList<StorageScheme> Supported = [new StorageScheme("ftp")];

    public override string Name => "FTP";

    public override IReadOnlyList<StorageScheme> Schemes => Supported;

    public override StorageCapabilities Capabilities =>
        StorageCapabilities.Read | StorageCapabilities.Write | StorageCapabilities.List | StorageCapabilities.Hierarchy;

    protected override async Task<Stream> OpenReadCoreAsync(StorageUri uri, CancellationToken ct)
    {
        var client = new FtpClient(uri.Host);
        await client.ConnectAsync(ct);
        return await client.OpenReadAsync(uri.Path, ct);
    }

    protected override async Task<StorageWriteStream> OpenWriteCoreAsync(
        StorageUri uri, StorageWriteOptions? options, CancellationToken ct)
    {
        // FtpWriteStream is a StorageWriteStream subclass; see "Writing" below.
        var client = new FtpClient(uri.Host);
        await client.ConnectAsync(ct);
        return new FtpWriteStream(client, uri.Path);
    }

    protected override async Task<StorageMetadata?> GetMetadataCoreAsync(StorageUri uri, CancellationToken ct)
    {
        var client = new FtpClient(uri.Host);
        await client.ConnectAsync(ct);
        var info = await client.GetInfoAsync(uri.Path, ct);
        return info is null ? null : new StorageMetadata { Size = info.Size, LastModified = info.Modified };
    }

    protected override async IAsyncEnumerable<StorageItem> ListCoreAsync(
        StorageUri directory, bool recursive, [EnumeratorCancellation] CancellationToken ct)
    {
        var client = new FtpClient(directory.Host);
        await client.ConnectAsync(ct);

        foreach (var item in await client.ListDirectoryAsync(directory.Path, recursive, ct))
        {
            // WithPath keeps the caller's host, port and parameters.
            yield return new StorageItem
            {
                Uri = directory.WithPath(item.IsDirectory ? item.Path + "/" : item.Path),
                Size = item.IsDirectory ? null : item.Size,
                LastModified = item.Modified,
                IsDirectory = item.IsDirectory,
            };
        }
    }
}
```

`ExistsAsync` defaults to "`GetMetadataAsync` returned something"; override `ExistsCoreAsync` if your store has a cheaper check. `ListCoreAsync` receives a directory that already ends with `/`.

## Capabilities

| Flag | Override |
|------|----------|
| `Read` | `OpenReadCoreAsync`, `GetMetadataCoreAsync` (and optionally `ExistsCoreAsync`) |
| `Write` | `OpenWriteCoreAsync` |
| `List` | `ListCoreAsync` |
| `Delete` | `DeleteCoreAsync` (must succeed when the object is missing) |
| `Move` | `MoveCoreAsync` (must overwrite the destination; a missing source throws `FileNotFoundException`) |
| `AtomicMove` | Declare it only when `MoveCoreAsync` is a single atomic rename |
| `Hierarchy` | Declare it for real directories; `ExistsAsync` is then `true` for a directory |
| `ConditionalWrite` | Honour `StorageWriteOptions.Overwrite` and `IfMatch` atomically in `CommitAsync` |

Declaring a capability you don't implement, or the reverse, is a bug: connectors read `Capabilities` to decide how to behave. For example, the DataLake manifest writer replaces the main manifest conditionally only when the provider declares `ConditionalWrite`.

## Writing

`OpenWriteCoreAsync` returns a `StorageWriteStream`, an abstract class in `NPipeline.StorageProviders.Abstractions` that derives from `Stream`. Callers write to it and then call `CommitAsync`. Your subclass must follow this contract:

- `CommitAsync` makes the bytes visible at the target atomically, and callers call it once, after the last write. Set the `ETag` property to the new ETag if the store returns one.
- Disposing the stream without a commit discards everything written. The target stays untouched, and an existing object stays as it was. Dispose never uploads.
- Upload failures surface from `CommitAsync`, using the caller's token, not from `Dispose`.

For an object store, derive from `SpooledWriteStream` (also in `NPipeline.StorageProviders.Abstractions`). It buffers the writes to a local temporary file, deletes the file when the stream is disposed, and calls your `UploadAsync` from `CommitAsync`. You only upload the content and return the ETag:

```csharp
internal sealed class FtpWriteStream(FtpClient client, string path) : SpooledWriteStream("ftp-upload")
{
    protected override async Task<string?> UploadAsync(Stream content, CancellationToken ct)
    {
        await client.UploadAsync(path, content, ct);
        return null;   // return the object's ETag, or null if the store has none
    }
}
```

For a store that can rename, such as a file system or an SFTP server, write to a hidden sibling temporary file and rename it into place in `CommitAsync`. Delete the temporary file when the stream is disposed without a commit.

Declare `ConditionalWrite` only if the store enforces `StorageWriteOptions.Overwrite = false` and `IfMatch` atomically, for example with `If-None-Match` and `If-Match` headers. When a condition isn't met, throw `StoragePreconditionFailedException` (in `NPipeline.StorageProviders.Exceptions`) from `CommitAsync`. Don't declare the capability if you check the condition and then write in two steps: concurrent writers can slip between them.

## Error contract

Translate your store's errors into standard .NET exceptions, in one place:

| Situation | Throw |
|-----------|-------|
| Not found (read, move source) | `FileNotFoundException` (`GetMetadataCoreAsync` returns `null` instead) |
| Authentication or permission failure | `UnauthorizedAccessException` |
| Invalid bucket, container, key or path | `ArgumentException` |
| Any other failure | `IOException`, with the original exception as `InnerException` |

## Conformance tests

`NPipeline.Tests.Common` has an abstract `StorageProviderConformanceTests` class that checks the shared contract: object names with spaces, `#`, `%` and unicode; sizes around a part boundary; overwrite; missing objects; listing; delete and move; capabilities; and cancellation. Subclass it once for your provider, supply `CreateProviderAsync()` and `RootUri`, and override the switches for what your backend can't represent.

## Registration

### Manual

```csharp
var resolver = new StorageResolver([new FileSystemStorageProvider(), new FtpStorageProvider()]);
```

### Via DI

```csharp
services.AddStorageProvider<FtpStorageProvider>();   // singleton, registered once
services.AddStorageResolver();
```

Two providers that serve the same scheme make the resolver throw `ArgumentException`. Give the second one a different scheme, or pass it to a node through `Provider`.

## Next Steps

- [Storage Providers Overview](index.md) - built-in providers
- [Connectors](../connectors/index.md) - use your provider with file-based connectors

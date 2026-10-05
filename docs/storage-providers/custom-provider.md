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
        var client = new FtpClient(uri.Host);
        await client.ConnectAsync(ct);
        return new PassThroughWriteStream(await client.OpenWriteAsync(uri.Path, ct));
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
| `ConditionalWrite` | Honour `StorageWriteOptions.Overwrite` and `IfMatch` atomically |

Declaring a capability you don't implement, or the reverse, is a bug: connectors read `Capabilities` to decide how to behave. For example, `FileSinkNode` renames through a temporary object only when the provider declares `AtomicMove`.

## Writing

`OpenWriteAsync` returns a `StorageWriteStream`. Wrap a stream that publishes when it is disposed in `PassThroughWriteStream`. `CommitAsync` is the explicit commit point for providers that implement it.

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
